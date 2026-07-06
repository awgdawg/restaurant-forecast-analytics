# restaurant-forecast-analytics

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
Delta tables and a `forecast_vs_actuals` dbt view — and the whole nightly pipeline
(extract → load → dbt → forecast) is defined as code in a Databricks Asset Bundle
([`databricks.yml`](databricks.yml)). See the
[cloud design spec](docs/superpowers/specs/2026-06-29-databricks-cloud-pipeline-design.md):
Phase 2 deploys it to a paid workspace with in-cloud extraction, a live AI/BI dashboard,
and a daily-refreshing Tableau Public feed.

## Setup

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # then fill in Toast + Databricks credentials
```

## Layout

- `ingest/` — local Python: Toast API auth + extraction (retry, auto-detect, resumable backfill)
- `load/` — raw Parquet → Databricks Delta bronze (incremental by default; `--full-refresh` to reload) + forecast/metrics Delta writers
- `models/` — dbt (staging → marts) on Databricks
- `forecast/` — baseline + Prophet, rolling-origin backtest, Tableau exports
- `exports/` — forecast-vs-actuals + metrics CSVs (Tableau Public data source)
- `databricks.yml` — Asset Bundle: the nightly extract → load → dbt → forecast Workflow as code (deploys to a paid workspace; schedule ships paused)
- `tests/` — pytest
- `docs/` — spec, plans, forecast writeup, captured API shapes
