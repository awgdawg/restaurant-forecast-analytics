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
`load` → `dbt_build` → `forecast` → `publish` — on a 09:15 America/Chicago schedule
(unpaused in the `prod` target). See the
[cloud design spec](docs/superpowers/specs/2026-06-29-databricks-cloud-pipeline-design.md).

### In-cloud I/O and the interim extract

The trial workspace's serverless egress allowlist blocks outbound internet — both the
Toast API and the workspace's *own* SQL endpoint (evidence and an open support ticket in
[`docs/databricks-egress-ticket.md`](docs/databricks-egress-ticket.md)). Two consequences
shape the as-built system:

- **Spark-native table I/O in-cloud.** `load`, `forecast`, and `publish` detect the active
  Spark session (`load/spark_io.py`) and do all table reads/writes through Spark / Unity
  Catalog (Volume reads are FUSE-mounted); locally the same entry points use
  `databricks-sql-connector` unchanged.
- **Interim local extract.** The in-cloud `extract` task is commented out in the bundle.
  Instead `scripts/morning_extract.ps1` runs at 08:45 daily (Windows Task Scheduler),
  re-pulling the trailing 3 days from Toast and uploading them to the UC Volume ahead of
  the 09:15 cloud run. Restoring full-cloud extraction is a one-shot change once Databricks
  opens egress — uncomment the task, restore `load`'s `depends_on`, move the schedule to
  04:30 (steps are in the `databricks.yml` comments).

`publish` (`publish/to_sheets.py`) mirrors `forecast_vs_actuals` and `model_metrics` into a
Google Sheet for Tableau Public to sync from; dashboard work is next (no dashboard exists
yet). Throughout, entry points read config from the environment first and fall back to the
`restaurant-forecast` Databricks secret scope in-cloud, so no secrets live in the repo.

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

- `ingest/` — local Python: Toast API auth + extraction (retry, auto-detect, resumable backfill)
- `load/` — raw Parquet → Databricks Delta bronze (incremental by default; `--full-refresh` to reload) + forecast/metrics Delta writers; `spark_io.py` is the in-cloud Spark path
- `models/` — dbt (staging → marts) on Databricks
- `forecast/` — baseline + Prophet, rolling-origin backtest, Tableau exports
- `publish/` — mirror `forecast_vs_actuals` + `model_metrics` into a Google Sheet (gspread) for Tableau Public
- `exports/` — forecast-vs-actuals + metrics CSVs (Tableau Public data source)
- `scripts/` — wheel build + the interim `morning_extract.ps1` local extract
- `databricks.yml` — Asset Bundle: the nightly `load` → `dbt_build` → `forecast` → `publish` Workflow as code (serverless; `prod` runs unpaused at 09:15, the `extract` task is interim-local)
- `tests/` — pytest
- `docs/` — spec, plans, forecast writeup, captured API shapes
