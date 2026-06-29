# Forecasting Daily Restaurant Sales — Toast → Databricks → Prophet → Tableau

An end-to-end analytics-engineering project: pull a single restaurant's **live Toast POS
data**, model it on a **Databricks lakehouse** with dbt, **forecast the next 14 days of
sales** with a model that beats a strong seasonal baseline, and publish a
**forecast-vs-actuals dashboard to Tableau Public**.

## The problem

Restaurants live and die by anticipating demand — staffing, prep, and ordering all hinge on
*"how much will we sell?"* This forecasts **daily net sales** 14 days out and proves the
forecast is materially better than the "same as last week" rule of thumb most operators run
in their heads.

## Architecture

```
Toast Orders API ──(local Python, PII-stripped)──▶ Parquet
   └▶ Databricks Delta (bronze) ──▶ dbt (staging → fct_daily_sales, reconciled to Toast)
        └▶ Prophet forecast + rolling-origin backtest (local) ──▶ CSV exports
             └▶ Tableau Public dashboard
```

Forecasting runs **locally** against the curated mart (Databricks Free Edition is
non-commercial and can't reliably `%pip install prophet`), which keeps the modeling portable
and the lakehouse the single source of truth. The architecture ports to a paid Databricks
workspace with minimal rework.

## The data

- **899 daily observations**, 2024-01-10 → 2026-06-26 (~2.5 years).
- **$1.93M** total net sales, **~$2,530/day** average (min $16, max $6,533).
- Reconciled to Toast's own Sales Summary **to the penny** — net sales excludes *deferred*
  gift-card revenue, a subtlety that caused an $80 discrepancy until it was traced, fixed, and
  locked with a dbt test.
- ~134 weekly-closure days (closed Mondays) carried as $0 — a strong weekly signal the model
  learns rather than being told.
- **Zero customer PII** anywhere in the pipeline — stripped at extraction by an allowlist.

## Method

Two models, compared honestly on the same **14-day rolling-origin (expanding-window)
backtest**, 8 folds:

1. **Baseline** — seasonal-naive: repeat the last week. A genuinely strong baseline for
   weekly-seasonal restaurant data, so beating it means something.
2. **Prophet** — trend + **multiplicative** weekly & yearly seasonality + US-holiday
   regressors. Multiplicative because sales scale *by* day-of-week, not by a fixed amount.

## Results

| Metric | Baseline (seasonal-naive) | **Prophet** | Improvement |
|---|---|---|---|
| **WAPE** | 16.9% | **13.4%** | **−21% relative** |
| **MAE** | $443 / day | **$351 / day** | **−$92 / day** |
| **RMSE** | $612 | **$452** | **−$160** |
| MAPE | 18.4% | 34.4% | *(misleading — see below)* |

Prophet wins every absolute-error metric decisively. Over a ~$2,530/day operation, cutting the
typical daily miss from **$443 to $351** is real money for staffing and prep decisions.

**Why MAPE disagrees — and why it's the wrong metric here.** Only **4 non-zero days in 2.5
years fall under $200**. MAPE divides each day's error by that day's actual, so a $400 miss on
a ~$16 near-holiday day registers as a **2,500%** error; a handful of such days dominate the
average and make Prophet look worse than it is. **WAPE** (total error ÷ total sales) is robust
to these low-volume days and measures what actually matters operationally — dollars. Picking
the metric *for the shape of the data*, before reading the result, is the point.

**The 14-day forecast** (2026-06-27 → 07-10) totals **~$37K** (~$2,640/day). Notably, the model
forecasts the **Monday closures at ~$110–160** (near-zero) — it learned the weekly pattern on
its own.

## Known limitations & next steps

- **Negative lower band on near-zero days.** Prophet's multiplicative mode places the *lower*
  uncertainty bound slightly below $0 on closed Mondays (the point forecast is fine). The right
  presentation fix is a clip at $0.
- **SARIMA** was scoped but deliberately skipped — Prophet already beats the baseline clearly,
  so adding it would be effort with no decision riding on it.
- **Next:** daypart-level forecasting, a labor/staffing-demand model (Phase 2, via the Toast
  Labor or Sling API), and inventory/ingredient demand forecasting through a recipe bill-of-materials.

## How it's built

Test-first throughout — the forecasting package ships with **39 passing tests** (metrics,
data-cleaning, the baseline, the backtest harness, the Prophet wrapper, and the export shapes).

```powershell
.\.venv\Scripts\python.exe -m forecast.run_forecast   # backtest both models, forecast 14 days, export CSVs
.\.venv\Scripts\python.exe -m pytest -q               # run the suite
```

Outputs `exports/forecast_vs_actuals.csv` (full actual history + the 14-day forecast) and
`exports/backtest_metrics.csv` — the data behind the dashboard.

## Dashboard

> **Live Tableau Public dashboard:** *publishing in progress — link pending.*
>
> *(Screenshot to be added once published.)*
