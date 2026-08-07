# restaurant-forecast-analytics

[![CI](https://github.com/awgdawg/restaurant-forecast-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/awgdawg/restaurant-forecast-analytics/actions/workflows/ci.yml)

Daily restaurant sales forecasting from live **Toast** POS data → **Databricks** (dbt) → **Tableau Public**.

Targets the forecasting and Tableau skill gaps. See the design spec in
[`docs/superpowers/specs/`](docs/superpowers/specs/) and the implementation plans in
[`docs/superpowers/plans/`](docs/superpowers/plans/).

## Forecasting result

On 2.5 years of real history (899 days, 14-day rolling-origin backtest, 8 folds), a **Prophet**
model beats a seasonal-naive baseline:

| Metric | Baseline | **Prophet** |
|---|---|---|
| WAPE | 16.9% | **13.4%** |
| MAE | $443 / day | **$351 / day** |
| RMSE | $612 | **$452** |

Full methodology — including why **WAPE, not MAPE**, is the right headline metric on
low-volume-day data — in **[`docs/forecast-writeup.md`](docs/forecast-writeup.md)**.

```powershell
.\.venv\Scripts\python.exe -m forecast.run_forecast   # backtest, forecast 14 days, export CSVs
```

## Cloud pipeline

The forecast now lands in the lakehouse itself — `forecast_daily_sales` + `model_metrics`
Delta tables and a `forecast_vs_actuals` dbt view — and the nightly pipeline is defined
as code in a Databricks Asset Bundle ([`databricks.yml`](databricks.yml)). The
`restaurant-forecast-nightly` job runs four serverless tasks in order —
`load` → `dbt_build` → `forecast` → `publish` — on a 21:00 America/Chicago schedule
(unpaused in the `prod` target). See the
[cloud design spec](docs/superpowers/specs/2026-06-29-databricks-cloud-pipeline-design.md).

### In-cloud I/O and two-speed extraction

The trial workspace's serverless egress allowlist blocks outbound internet — both the
Toast API and the workspace's *own* SQL endpoint (evidence and an open support ticket in
[`docs/databricks-egress-ticket.md`](docs/databricks-egress-ticket.md)) — and classic
compute is unavailable on it, so extraction has nowhere to run inside Databricks. Two
consequences shape the as-built system:

- **Spark-native table I/O in-cloud.** `load`, `forecast`, and `publish` detect the active
  Spark session (`load/spark_io.py`) and do all table reads/writes through Spark / Unity
  Catalog (Volume reads are FUSE-mounted); locally the same entry points use
  `databricks-sql-connector` unchanged.
- **Extraction runs on GCP Cloud Run** (project `restaurant-forecast-ops`), not on a PC:
  a containerized `rfa-sync` job pulls from Toast and uploads Parquet partitions to the UC
  Volume over the Databricks Files API (`ingest/to_volume.py`), with Toast and upload
  credentials in GCP Secret Manager. Two schedules give the pipeline two speeds:

| Lane | Cadence (America/Chicago) | Command | Feeds |
|---|---|---|---|
| **Nightly** — authoritative | 20:40 daily (post-close) | `rfa-sync --refresh-days 4 --through-today` | the 21:00 job: bronze → dbt → forecast → Sheet |
| **Intraday** — read-through | every 15 min, service hours (Tue–Sun) | `rfa-sync --today` | `intraday_today`: today-so-far vs. today's forecast |

Both daily lanes run **after close** rather than the next morning, so each day's
finalized tables and the next day's forecast are ready overnight — before the restaurant
opens rather than an hour into service. `--through-today` shifts the refresh window to end
today instead of yesterday; the window is **4 days, not 3**, because moving the run a day
forward would otherwise cut each date's final re-pull to 49h after close (vs. 60.5h on the
old morning cadence) — 4 days gives 73h, a strict superset of what the morning schedule
captured.

The sync→job gap is **20 minutes** (2026-08-07, narrowed from 45 when both lanes moved
earlier: close is 20:00, so 21:00/21:45 left the forecast landing later at night than it
needed to). That gap no longer covers `toast-sync`'s worst-case retry budget
(`--max-retries 1`, 900s task timeout ≈ 30 min) — observed runtime is ~2.5 min end to end,
so it covers the normal case but not a full hang-and-retry. If the job ever builds on a
partial Volume, widen this gap before investigating anything else. The nightly sync sits at
**:40** rather than a quarter-hour because the intraday lane fires on :00/:15/:30/:45 and
both lanes write the same `business_date=<today>/orders.parquet`; sharing a minute would be
a torn-write race. Keep the nightly off those four minutes if it ever moves again.

Only the nightly lane writes tables; its trailing 3-day re-pull finalizes each day's
post-close edits, and intraday partials never reach bronze or the marts. The fast lane
reads through instead —
[`models/marts/intraday_today.sql`](models/marts/intraday_today.sql) queries today's raw
partition directly with `read_files` and joins today's forecast — so a dashboard tile
stays current to within the polling interval with **zero extra Databricks job runs**.

The PC path is retired. Cloud Run and the PC job ran in parallel for two mornings and
produced identical partitions (334 then 418 rows across the same 3 partitions), after
which the `restaurant-forecast-morning-extract` Windows Task Scheduler entry was
**disabled, not deleted** — re-enable it with `schtasks /change /tn
"restaurant-forecast-morning-extract" /enable` if the cloud lane ever needs a stand-in.

Two extractors therefore remain as break-glass, neither of them the scheduled path:
`scripts/morning_extract.ps1` (local, manual — the original PC job, still the proven
gap-backfill tool) and [`.github/workflows/extract.yml`](.github/workflows/extract.yml)
(Actions manual dispatch, needs `TOAST_*` secrets). The bundle's own `extract` task stays
commented out.

`publish` (`publish/to_sheets.py`) mirrors `forecast_vs_actuals` and `model_metrics` into a
Google Sheet for Tableau Public to sync from. An internal AI/BI dashboard in the workspace
serves the owner day-to-day, including a live "Today" tile over `intraday_today` (sales so
far vs. forecast, current to the 15-minute poll); a public Tableau dashboard remains a
future step. Throughout, entry points read config from the environment first and fall back
to the `restaurant-forecast` Databricks secret scope in-cloud, so no secrets live in the repo.

### Deploying the bundle (prod)

The bundle deliberately contains no workspace hostname (public repo). The Databricks CLI
reads `DATABRICKS_HOST` / `DATABRICKS_TOKEN` — bridge them from `.env` before any
`bundle` command, and pass the deploy vars explicitly:

```bash
set -a && source .env && set +a
export DATABRICKS_HOST="https://$DBX_HOST" DATABRICKS_TOKEN="$DBX_TOKEN"
databricks bundle deploy -t prod \
  --var dbt_warehouse_id="<your warehouse id>" \
  --var raw_volume="/Volumes/<catalog>/default/raw_orders" \
  --var gsheet_id="<publish target Sheet id>"
```

`.env` therefore decides where prod deploys — keep it pointed at the intended workspace.
`dbt_warehouse_id` (dbt task) and `gsheet_id` (`publish` task) default to empty in the
bundle — a deploy validates but fails at run time — so both are required; `raw_volume`
defaults to `/Volumes/workspace/default/raw_orders`, override it if your catalog differs.

Deploy also builds the wheel via `python scripts/build_wheel.py` — make sure the repo
venv's interpreter is first on `PATH` (`PATH="$PWD/.venv/Scripts:$PATH"`), or the build
step fails with `ModuleNotFoundError: build`.

## Setup

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # then fill in Toast + Databricks credentials
```

## Layout

- `ingest/` — Toast API auth + extraction (retry, auto-detect, resumable backfill); `sync.py` (the `rfa-sync` entry point) + `to_volume.py` (Files-API upload) are what Cloud Run runs
- `load/` — raw Parquet → Databricks Delta bronze (incremental by default; `--full-refresh` to reload) + forecast/metrics Delta writers; `spark_io.py` is the in-cloud Spark path
- `models/` — dbt (staging → marts) on Databricks, plus the `intraday_today` fast-lane view over today's raw partition
- `forecast/` — baseline + Prophet, rolling-origin backtest, Tableau exports
- `publish/` — mirror `forecast_vs_actuals` + `model_metrics` into a Google Sheet (gspread) for Tableau Public
- `exports/` — forecast-vs-actuals + metrics CSVs (Tableau Public data source)
- `scripts/` — wheel build + `morning_extract.ps1`, the manual local-extract fallback
- `Dockerfile` — the Cloud Run image: `rfa-sync` (extract + Volume upload) on both schedules
- `databricks.yml` — Asset Bundle: the nightly `load` → `dbt_build` → `forecast` → `publish` Workflow as code (serverless; `prod` runs unpaused at 09:15; the `extract` task stays disabled — extraction runs on Cloud Run)
- `tests/` — pytest
- `docs/` — spec, plans, forecast writeup, captured API shapes
