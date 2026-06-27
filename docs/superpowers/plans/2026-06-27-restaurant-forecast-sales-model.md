# Sales Forecast (Baseline + Prophet) → Backtest → Tableau Public Implementation Plan (Plan 5 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forecast daily `net_sales` 14 days ahead, prove the model **beats a seasonal-naive baseline** on a rolling-origin backtest, and publish a **forecast-vs-actuals dashboard to Tableau Public** embedded in the portfolio.

**Why:** This is the payoff task — it hits both named skill gaps (forecasting + Tableau) and is the visible portfolio centerpiece. The data is ready: `fct_daily_sales` holds **763 daily observations (2024-01-10 → 2026-06-26), ~$2,530/day avg**, reconciled to Toast — enough history for weekly *and* yearly seasonality.

**Architecture:** A tested `forecast/` Python package (refines the spec's single `notebooks/forecast_sales.py` into focused, CI-testable modules). It reads the curated `fct_daily_sales` mart from Databricks into a clean continuous daily series (closed days = $0), runs a model-agnostic rolling-origin backtest comparing **seasonal-naive** vs **Prophet** (weekly + yearly + US-holiday, multiplicative), fits the winner on full history, forecasts 14 days, and writes tidy CSVs that Tableau Public reads. Forecasting runs **locally** (Free Edition can't `%pip install prophet` reliably), exactly as §11 anticipated.

**Tech Stack:** Python 3.10, `pandas`/`numpy` (installed), **`prophet`** + **`holidays`** (to add), `databricks-sql-connector` (installed, via `load.databricks.connect`), `pytest`, Tableau Public.

---

## Scope (Plan 5 of 5)

- **In scope:** `forecast/` package — `metrics`, `data` (load + clean), `models` (seasonal-naive), `backtest` (rolling-origin + summarize), `prophet_model`, `export_tableau`, `run_forecast` CLI; a published Tableau Public dashboard; README writeup + portfolio embed.
- **Out of scope (YAGNI / later phases):**
  - **SARIMA / `auto_arima`** — optional stretch only (see Task 9). Success criterion #3 needs *one* model to beat baseline; Prophet should. Don't add `pmdarima` unless Prophet underperforms after tuning.
  - **Daypart-level** forecast (spec extension), **labor** (Phase 2 / M5), **inventory/BOM**, **near-real-time** (§19), **writing `forecast_daily_sales` back to Databricks** (optional Task 8 — the CSV is the deliverable).

**Repo root:** `E:\PyProj\restaurant-forecast-analytics` (PowerShell; `.\.venv\Scripts\python.exe`). `.env` has working Databricks creds; `load.databricks.connect()` already reads them. Mart is `workspace.default_marts.fct_daily_sales`.

**Canonical data shapes (used by every task — keep these exact):**
- **series / train:** `pd.DataFrame` cols `ds` (datetime64[ns]), `y` (float), sorted ascending, **continuous daily** (no gaps).
- **forecast:** `pd.DataFrame` cols `ds`, `yhat` (+ `yhat_lower`, `yhat_upper` for Prophet), exactly the `horizon` days **after** the train's last `ds`.
- **model_fn:** any callable `model_fn(train, horizon) -> forecast` with the shapes above.

---

## Task 1: Add forecasting dependencies + verify Prophet install (de-risk first)

Prophet pulls a Stan backend (`cmdstanpy`), which on Windows is the single biggest risk in this plan. Install and smoke-fit it **before** writing any model code, so a toolchain problem surfaces in isolation. Tasks 2–5 (harness + baseline) have **no** Prophet dependency, so they proceed regardless.

**Files:** Modify `requirements.txt`

- [ ] **Step 1: Add deps to `requirements.txt`**

Append:
```
prophet==1.1.6
holidays>=0.50
```

- [ ] **Step 2: Install**

Run:
```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
Expected: resolves and installs `prophet`, `cmdstanpy`, `holidays`. (First install may download a cmdstan toolchain — allow a few minutes.)

- [ ] **Step 3: Smoke-fit Prophet on synthetic data (one-time toolchain check)**

Run:
```powershell
.\.venv\Scripts\python.exe -c "import pandas as pd; from prophet import Prophet; ds=pd.date_range('2025-01-01',periods=90,freq='D'); m=Prophet(weekly_seasonality=True).fit(pd.DataFrame({'ds':ds,'y':[100+ (d.weekday()) for d in ds]})); print('PROPHET OK', m.predict(m.make_future_dataframe(periods=14)).shape)"
```
Expected: prints `PROPHET OK (104, ...)`. **If this fails** (cmdstan compile error): try `.\.venv\Scripts\python.exe -c "import cmdstanpy; cmdstanpy.install_cmdstan()"`; if still failing on Windows, document the blocker and fall back to `statsmodels` SARIMAX as the challenger model (Task 6 alt) — the harness in Tasks 2–5 is model-agnostic, so nothing else changes.

- [ ] **Step 4: Commit**

```powershell
git add requirements.txt
git commit -m "build: add prophet + holidays for sales forecasting"
```

---

## Task 2: `forecast/metrics.py` — error metrics (TDD)

Zero-actual days (the ~134 closed days = $0) make vanilla MAPE divide by zero. Metrics must handle that: **MAPE skips zero-actual days**; **WAPE** (sum|err| / sum|actual|) is the robust headline metric.

**Files:** Create `forecast/__init__.py` (empty), `forecast/metrics.py`, `tests/test_metrics.py`

- [ ] **Step 1: Write the failing test** — `tests/test_metrics.py`

```python
import math

from forecast.metrics import mae, mape, rmse, wape


def test_mae_and_rmse_on_known_values():
    y = [10.0, 20.0, 30.0]
    yhat = [12.0, 18.0, 33.0]  # errors 2, 2, 3
    assert mae(y, yhat) == (2 + 2 + 3) / 3
    assert rmse(y, yhat) == math.sqrt((4 + 4 + 9) / 3)


def test_mape_skips_zero_actual_days():
    y = [0.0, 100.0, 200.0]  # closed day first
    yhat = [50.0, 110.0, 180.0]  # 10% then 10% on the non-zero days
    assert mape(y, yhat) == 10.0  # the 0-actual day is excluded, not inf


def test_wape_is_total_abs_error_over_total_actual():
    y = [0.0, 100.0, 200.0]
    yhat = [50.0, 110.0, 180.0]  # abs errors 50,10,20 = 80; total actual 300
    assert wape(y, yhat) == 80 / 300 * 100
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_metrics.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast'`.

- [ ] **Step 3: Implement** — create empty `forecast/__init__.py`, then `forecast/metrics.py`

```python
from __future__ import annotations

import numpy as np


def mae(y_true, y_pred) -> float:
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    return float(np.mean(np.abs(yt - yp)))


def rmse(y_true, y_pred) -> float:
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    return float(np.sqrt(np.mean((yt - yp) ** 2)))


def mape(y_true, y_pred) -> float:
    """Mean abs % error over days with non-zero actuals (closed days excluded)."""
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    mask = yt != 0
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs((yt[mask] - yp[mask]) / yt[mask])) * 100)


def wape(y_true, y_pred) -> float:
    """Weighted abs % error: sum|err| / sum|actual|. Robust to zero days."""
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    denom = np.sum(np.abs(yt))
    return float(np.sum(np.abs(yt - yp)) / denom * 100) if denom else float("nan")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_metrics.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```powershell
git add forecast/__init__.py forecast/metrics.py tests/test_metrics.py
git commit -m "feat: forecast error metrics (mae, rmse, zero-safe mape, wape)"
```

---

## Task 3: `forecast/data.py` — load + clean to a continuous daily series (TDD)

Closed days are simply absent from `fct_daily_sales`. A daily forecast needs a **gap-free** series, so reindex to a continuous calendar and fill missing days with `$0` (the truthful value — the restaurant earned nothing those days; weekly seasonality will learn the regular closure).

**Files:** Create `forecast/data.py`, `tests/test_forecast_data.py`

- [ ] **Step 1: Write the failing test** — `tests/test_forecast_data.py`

```python
import pandas as pd

from forecast.data import clean_daily_series


def test_clean_fills_missing_days_with_zero_and_is_continuous():
    # 6/16 and 6/17 present, 6/18 missing (closed), 6/19 present
    raw = pd.DataFrame(
        {"business_date": [20260616, 20260617, 20260619], "net_sales": [100.0, 200.0, 300.0]}
    )

    out = clean_daily_series(raw)

    assert list(out.columns) == ["ds", "y"]
    assert str(out["ds"].dtype) == "datetime64[ns]"
    # continuous 6/16..6/19 = 4 rows, the gap day filled with 0.0
    assert list(out["ds"]) == list(pd.date_range("2026-06-16", "2026-06-19", freq="D"))
    assert list(out["y"]) == [100.0, 200.0, 0.0, 300.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_forecast_data.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast.data'`.

- [ ] **Step 3: Implement** — `forecast/data.py`

```python
from __future__ import annotations

import pandas as pd

MART = "workspace.default_marts.fct_daily_sales"


def clean_daily_series(raw: pd.DataFrame) -> pd.DataFrame:
    """raw: cols business_date (int YYYYMMDD), net_sales (float).
    Returns continuous daily df[ds, y] with missing (closed) days filled 0.0."""
    df = raw.copy()
    df["ds"] = pd.to_datetime(df["business_date"].astype(int).astype(str), format="%Y%m%d")
    df = df[["ds", "net_sales"]].rename(columns={"net_sales": "y"}).sort_values("ds")
    full = pd.date_range(df["ds"].min(), df["ds"].max(), freq="D")
    df = df.set_index("ds").reindex(full).rename_axis("ds").reset_index()
    df["y"] = df["y"].astype(float).fillna(0.0)
    return df


def load_daily_series(conn, table: str = MART) -> pd.DataFrame:
    """Query the daily-sales mart and clean it into a forecasting series."""
    cur = conn.cursor()
    cur.execute(f"SELECT business_date, net_sales FROM {table} ORDER BY business_date")
    rows = cur.fetchall()
    cur.close()
    raw = pd.DataFrame(rows, columns=["business_date", "net_sales"])
    return clean_daily_series(raw)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_forecast_data.py -q`
Expected: PASS (1 passed). (`load_daily_series` is exercised live in Task 8.)

- [ ] **Step 5: Commit**

```powershell
git add forecast/data.py tests/test_forecast_data.py
git commit -m "feat: load + clean fct_daily_sales into a continuous daily series"
```

---

## Task 4: `forecast/models.py` — seasonal-naive baseline (TDD)

Seasonal-naive (repeat the last week) is a **strong** baseline for restaurant data — the model has to genuinely beat it.

**Files:** Create `forecast/models.py`, `tests/test_models.py`

- [ ] **Step 1: Write the failing test** — `tests/test_models.py`

```python
import pandas as pd

from forecast.models import seasonal_naive


def test_seasonal_naive_repeats_last_week_by_weekday():
    ds = pd.date_range("2026-01-01", periods=21, freq="D")
    y = [float(d.weekday()) for d in ds]  # value == weekday, a pure weekly pattern
    train = pd.DataFrame({"ds": ds, "y": y})

    fc = seasonal_naive(train, horizon=14)

    assert list(fc.columns) == ["ds", "yhat"]
    assert len(fc) == 14
    # forecast dates immediately follow the last train day
    assert list(fc["ds"]) == list(pd.date_range("2026-01-22", periods=14, freq="D"))
    # each future day's yhat == that day's weekday (i.e. same weekday last week)
    assert list(fc["yhat"]) == [float(d.weekday()) for d in fc["ds"]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_models.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast.models'`.

- [ ] **Step 3: Implement** — `forecast/models.py`

```python
from __future__ import annotations

import numpy as np
import pandas as pd


def seasonal_naive(train: pd.DataFrame, horizon: int, season_length: int = 7) -> pd.DataFrame:
    """Forecast = the last `season_length` observed values, tiled across the horizon.
    Future day T+1 reuses the value from T+1-season_length (same weekday last week)."""
    last_season = train["y"].to_numpy()[-season_length:]
    if len(last_season) < season_length:  # very short history: pad by repetition
        last_season = np.resize(last_season, season_length)
    last_ds = train["ds"].iloc[-1]
    future_ds = pd.date_range(last_ds + pd.Timedelta(days=1), periods=horizon, freq="D")
    yhat = np.resize(last_season, horizon)  # [s0..s6, s0..s6, ...]
    return pd.DataFrame({"ds": future_ds, "yhat": yhat.astype(float)})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_models.py -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```powershell
git add forecast/models.py tests/test_models.py
git commit -m "feat: seasonal-naive (repeat-last-week) baseline forecaster"
```

---

## Task 5: `forecast/backtest.py` — rolling-origin backtest + summarize (TDD)

Expanding-window backtest: for each cutoff, train on everything before it, forecast `horizon` days, compare to the held-out actuals. Continuous daily series (Task 3) guarantees the forecast `ds` align exactly with held-out `ds`, so the merge is clean.

**Files:** Create `forecast/backtest.py`, `tests/test_backtest.py`

- [ ] **Step 1: Write the failing test** — `tests/test_backtest.py`

```python
import pandas as pd

from forecast.backtest import rolling_origin_backtest, summarize
from forecast.models import seasonal_naive


def _weekly_series(n):
    ds = pd.date_range("2026-01-01", periods=n, freq="D")
    return pd.DataFrame({"ds": ds, "y": [float(d.weekday()) for d in ds]})


def test_backtest_aligns_actuals_to_predictions_across_folds():
    series = _weekly_series(100)

    bt = rolling_origin_backtest(series, seasonal_naive, horizon=14, n_folds=3, step=14, min_train=30)

    assert set(bt.columns) == {"ds", "y", "yhat", "fold"}
    assert bt["fold"].nunique() == 3
    assert len(bt) == 3 * 14
    # pure weekly pattern => seasonal-naive is exact => zero error everywhere
    assert (bt["y"] == bt["yhat"]).all()


def test_summarize_returns_all_metrics():
    series = _weekly_series(100)
    bt = rolling_origin_backtest(series, seasonal_naive, horizon=14, n_folds=3, step=14, min_train=30)

    m = summarize(bt)

    assert set(m) == {"mae", "rmse", "mape", "wape"}
    assert m["mae"] == 0.0  # exact on the synthetic series
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_backtest.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast.backtest'`.

- [ ] **Step 3: Implement** — `forecast/backtest.py`

```python
from __future__ import annotations

import pandas as pd

from forecast.metrics import mae, mape, rmse, wape


def rolling_origin_backtest(
    series: pd.DataFrame,
    model_fn,
    horizon: int = 14,
    n_folds: int = 8,
    step: int = 14,
    min_train: int = 60,
) -> pd.DataFrame:
    """Expanding-window backtest. Returns long df[ds, y, yhat, fold]. The last fold
    ends at the series end; earlier folds step back `step` days each."""
    s = series.sort_values("ds").reset_index(drop=True)
    last_cutoff = len(s) - horizon
    frames = []
    for k in range(n_folds):
        cutoff = last_cutoff - (n_folds - 1 - k) * step
        if cutoff < min_train:
            continue
        train = s.iloc[:cutoff].copy()
        actual = s.iloc[cutoff : cutoff + horizon][["ds", "y"]].reset_index(drop=True)
        fc = model_fn(train, horizon)[["ds", "yhat"]].reset_index(drop=True)
        merged = actual.merge(fc, on="ds", how="left")
        merged["fold"] = k
        frames.append(merged)
    if not frames:
        raise ValueError("series too short for the given horizon/min_train/folds")
    return pd.concat(frames, ignore_index=True)


def summarize(bt: pd.DataFrame) -> dict:
    """Aggregate metrics across every backtest fold."""
    return {
        "mae": mae(bt["y"], bt["yhat"]),
        "rmse": rmse(bt["y"], bt["yhat"]),
        "mape": mape(bt["y"], bt["yhat"]),
        "wape": wape(bt["y"], bt["yhat"]),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_backtest.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```powershell
git add forecast/backtest.py tests/test_backtest.py
git commit -m "feat: rolling-origin backtest harness + metric summary"
```

---

## Task 6: `forecast/prophet_model.py` — Prophet challenger (smoke TDD)

Multiplicative seasonality (sales scale up/down by weekday, not by a fixed amount) + weekly + yearly (we have 2.5 yrs) + US holidays. Keep the test light — one real fit on a short synthetic series asserting the output contract; Prophet's accuracy is judged by the live backtest in Task 8, not unit tests.

**Files:** Create `forecast/prophet_model.py`, `tests/test_prophet_model.py`

- [ ] **Step 1: Write the failing test** — `tests/test_prophet_model.py`

```python
import pandas as pd
import pytest

prophet = pytest.importorskip("prophet")  # skip cleanly if install failed (Task 1)

from forecast.prophet_model import prophet_forecast  # noqa: E402


def test_prophet_forecast_returns_horizon_rows_with_band():
    ds = pd.date_range("2024-01-01", periods=140, freq="D")
    y = [1000 + 200 * (d.weekday() >= 5) for d in ds]  # weekend bump
    train = pd.DataFrame({"ds": ds, "y": y})

    fc = prophet_forecast(train, horizon=14, yearly=False)

    assert list(fc.columns) == ["ds", "yhat", "yhat_lower", "yhat_upper"]
    assert len(fc) == 14
    assert list(fc["ds"]) == list(pd.date_range("2024-05-20", periods=14, freq="D"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_prophet_model.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast.prophet_model'` (or SKIP if Prophet didn't install — then revisit Task 1).

- [ ] **Step 3: Implement** — `forecast/prophet_model.py`

```python
from __future__ import annotations

import logging

import pandas as pd
from prophet import Prophet

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)  # quiet the Stan logs


def prophet_forecast(train: pd.DataFrame, horizon: int, *, yearly: bool = True) -> pd.DataFrame:
    """Fit Prophet (multiplicative weekly + optional yearly + US holidays) and
    return the `horizon` days after the last train date with a prediction band."""
    m = Prophet(
        weekly_seasonality=True,
        yearly_seasonality=yearly,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
    )
    m.add_country_holidays(country_name="US")
    m.fit(train[["ds", "y"]])
    future = m.make_future_dataframe(periods=horizon, freq="D")
    fc = m.predict(future)
    return fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon).reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_prophet_model.py -q`
Expected: PASS (1 passed; takes a few seconds for the fit).

- [ ] **Step 5: Commit**

```powershell
git add forecast/prophet_model.py tests/test_prophet_model.py
git commit -m "feat: Prophet challenger (multiplicative, weekly+yearly+US holidays)"
```

---

## Task 7: `forecast/export_tableau.py` — tidy CSVs for Tableau (TDD)

Two flat files Tableau Public reads: `forecast_vs_actuals.csv` (the chart — full actual history + the 14-day forecast band) and `backtest_metrics.csv` (KPI cards proving the model beats baseline). Aggregates only, no PII — publishable per §14.

**Files:** Create `forecast/export_tableau.py`, `tests/test_export_tableau.py`

- [ ] **Step 1: Write the failing test** — `tests/test_export_tableau.py`

```python
import pandas as pd

from forecast.export_tableau import build_forecast_vs_actuals, build_metrics_frame


def test_build_forecast_vs_actuals_stacks_history_then_forecast():
    series = pd.DataFrame({"ds": pd.date_range("2026-06-01", periods=3, freq="D"), "y": [10.0, 20.0, 30.0]})
    forecast = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-04", periods=2, freq="D"),
            "yhat": [40.0, 50.0],
            "yhat_lower": [35.0, 45.0],
            "yhat_upper": [45.0, 55.0],
        }
    )

    out = build_forecast_vs_actuals(series, forecast, model_name="prophet")

    assert list(out.columns) == [
        "date", "net_sales_actual", "yhat", "yhat_lower", "yhat_upper", "model", "is_forecast",
    ]
    assert len(out) == 5  # 3 history + 2 forecast
    assert out["is_forecast"].tolist() == [False, False, False, True, True]
    assert out.loc[0, "net_sales_actual"] == 10.0 and pd.isna(out.loc[0, "yhat"])
    assert out.loc[4, "yhat"] == 50.0 and pd.isna(out.loc[4, "net_sales_actual"])


def test_build_metrics_frame_one_row_per_model():
    metrics = {"baseline": {"mae": 500.0, "wape": 20.0}, "prophet": {"mae": 400.0, "wape": 15.0}}

    out = build_metrics_frame(metrics)

    assert set(out["model"]) == {"baseline", "prophet"}
    assert out.loc[out["model"] == "prophet", "wape"].iloc[0] == 15.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_export_tableau.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'forecast.export_tableau'`.

- [ ] **Step 3: Implement** — `forecast/export_tableau.py`

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd

COLUMNS = ["date", "net_sales_actual", "yhat", "yhat_lower", "yhat_upper", "model", "is_forecast"]


def build_forecast_vs_actuals(
    series: pd.DataFrame, forecast: pd.DataFrame, model_name: str
) -> pd.DataFrame:
    """Tidy history+forecast frame for the dashboard line chart."""
    hist = series.rename(columns={"ds": "date", "y": "net_sales_actual"}).copy()
    hist["yhat"] = pd.NA
    hist["yhat_lower"] = pd.NA
    hist["yhat_upper"] = pd.NA
    hist["is_forecast"] = False

    fc = forecast.rename(columns={"ds": "date"}).copy()
    fc["net_sales_actual"] = pd.NA
    fc["is_forecast"] = True

    out = pd.concat([hist, fc], ignore_index=True)
    out["model"] = model_name
    return out[COLUMNS]


def build_metrics_frame(metrics_by_model: dict) -> pd.DataFrame:
    """One row per model: model, mae, rmse, mape, wape (whatever keys are present)."""
    return pd.DataFrame([{"model": name, **vals} for name, vals in metrics_by_model.items()])


def write_exports(fva: pd.DataFrame, metrics: pd.DataFrame, out_dir: str = "exports") -> None:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    fva.to_csv(Path(out_dir) / "forecast_vs_actuals.csv", index=False)
    metrics.to_csv(Path(out_dir) / "backtest_metrics.csv", index=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_export_tableau.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```powershell
git add forecast/export_tableau.py tests/test_export_tableau.py
git commit -m "feat: tidy Tableau CSV builders (forecast-vs-actuals + metrics)"
```

---

## Task 8: `forecast/run_forecast.py` — CLI: backtest, compare, forecast, export (integration)

Orchestrates the tested pieces against **live** Databricks data, prints the baseline-vs-Prophet comparison (success criterion #3), fits the winner on full history, forecasts 14 days, and writes the CSVs.

**Files:** Create `forecast/run_forecast.py`; modify `.gitignore` (allow `exports/*.csv`)

- [ ] **Step 1: Implement** — `forecast/run_forecast.py`

```python
"""Backtest baseline vs Prophet on fct_daily_sales, forecast 14 days, export CSVs.

Usage:  python -m forecast.run_forecast [--horizon 14] [--folds 8]
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from forecast.backtest import rolling_origin_backtest, summarize
from forecast.data import load_daily_series
from forecast.export_tableau import build_forecast_vs_actuals, build_metrics_frame, write_exports
from forecast.models import seasonal_naive
from forecast.prophet_model import prophet_forecast
from load.databricks import connect


def main() -> None:
    parser = argparse.ArgumentParser(description="Forecast daily net sales.")
    parser.add_argument("--horizon", type=int, default=14)
    parser.add_argument("--folds", type=int, default=8)
    args = parser.parse_args()

    load_dotenv()
    conn = connect()
    try:
        series = load_daily_series(conn)
    finally:
        conn.close()
    print(f"Loaded {len(series)} days: {series['ds'].min().date()} -> {series['ds'].max().date()}")

    models = {"baseline": seasonal_naive, "prophet": lambda tr, h: prophet_forecast(tr, h)}
    metrics = {}
    for name, fn in models.items():
        bt = rolling_origin_backtest(series, fn, horizon=args.horizon, n_folds=args.folds)
        metrics[name] = summarize(bt)
        m = metrics[name]
        print(f"{name:9s}  WAPE={m['wape']:.2f}%  MAPE={m['mape']:.2f}%  MAE=${m['mae']:.0f}  RMSE=${m['rmse']:.0f}")

    winner = min(metrics, key=lambda k: metrics[k]["wape"])
    print(f"WINNER: {winner} (lowest WAPE)")
    if metrics["prophet"]["wape"] < metrics["baseline"]["wape"]:
        print("Prophet BEATS the seasonal-naive baseline.")
    else:
        print("Prophet does NOT beat baseline yet — see Task 8 tuning levers.")

    forecast = (
        prophet_forecast(series, args.horizon)
        if winner == "prophet"
        else seasonal_naive(series, args.horizon)
    )
    fva = build_forecast_vs_actuals(series, forecast, model_name=winner)
    write_exports(fva, build_metrics_frame(metrics))
    print(f"Wrote exports/forecast_vs_actuals.csv ({len(fva)} rows) + exports/backtest_metrics.csv")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Ensure `exports/*.csv` are committable** — `.gitignore`

The aggregate CSVs back the published dashboard + writeup numbers, so they should be tracked. Confirm `exports/` is not ignored:
```powershell
New-Item -ItemType Directory -Force exports | Out-Null
"placeholder" | Out-File exports/.gitkeep -Encoding utf8
git check-ignore exports/forecast_vs_actuals.csv
```
Expected: **no output** (not ignored). If it *is* ignored, add `!exports/*.csv` to `.gitignore` below the data-ignore block.

- [ ] **Step 3: Run the forecast live**

Run: `.\.venv\Scripts\python.exe -m forecast.run_forecast`
Expected: prints the day count (~895 continuous days), a metrics line for `baseline` and `prophet`, the winner, and the export confirmation. **Success criterion #3:** Prophet's WAPE/MAPE < baseline's.

- [ ] **Step 4 (only if Prophet does NOT beat baseline): tune, then re-run Step 3**

Apply levers in `forecast/prophet_model.py`, cheapest first, re-running the backtest after each:
  1. **Log-transform** the target: fit on `log1p(y)`, invert with `expm1` on `yhat/lower/upper` (stabilizes the weekend/holiday variance).
  2. **Winsorize outliers** before fit: cap `y` at its 1st/99th percentile (tames the $16 holiday day and any spikes).
  3. **Tune** `changepoint_prior_scale` (try 0.01 and 0.1) and `seasonality_prior_scale`.
  4. Add explicit **closure regressors** for the restaurant's known multi-day closures (holidays Prophet's US set misses).
Document the winning configuration in the commit message.

- [ ] **Step 5: Commit**

```powershell
git add forecast/run_forecast.py .gitignore exports/forecast_vs_actuals.csv exports/backtest_metrics.csv
git commit -m "feat: forecast CLI — backtest baseline vs Prophet, export Tableau CSVs"
```

---

## Task 9 (OPTIONAL stretch): SARIMA challenger + Databricks forecast table

Only do this if you want a third model or the lakehouse `forecast_daily_sales` mart (spec §10). Not required for success criteria.

- [ ] **9a — SARIMA:** add `statsmodels` to `requirements.txt`; create `forecast/sarima_model.py` with `sarima_forecast(train, horizon)` using `statsmodels.tsa.statespace.SARIMAX` (order `(1,1,1)`, seasonal `(1,1,1,7)`), returning `ds`+`yhat`; add it to the `models` dict in `run_forecast.py`. TDD a shape smoke test like Task 6.
- [ ] **9b — persist forecast:** create `forecast/load_forecast.py` that writes the winning forecast to `workspace.default_marts.forecast_daily_sales` (cols `forecast_date`, `yhat`, `yhat_lower`, `yhat_upper`, `model`, `run_ts`) via `load.databricks.connect()`, reusing the DELETE-by-run + multi-row INSERT pattern from `load/load_to_delta.py`.

---

## Task 10: Tableau Public dashboard (manual — GUI, documented)

**Files:** Create `tableau/` (the workbook saves to Tableau Public's cloud; `.twb`/`.twbx` stay gitignored per §14). Modify `docs/` with screenshots.

- [ ] **Step 1: Connect data** — Tableau Public Desktop → Connect → **Text file** → `exports/forecast_vs_actuals.csv`; add a second connection to `exports/backtest_metrics.csv`. Set `date` to Date type.
- [ ] **Step 2: Build the main chart** — a dual/overlay line chart on `date`:
  - `net_sales_actual` as the actual line (history).
  - `yhat` as the forecast line (last 14 days), with a band from `yhat_lower`→`yhat_upper` (use a Measure Values area or reference band).
  - Color/annotate the forecast region distinctly; filter `date` to a useful default window (e.g. last 120 days + the 14-day forecast).
- [ ] **Step 3: KPI cards** — from `backtest_metrics.csv`: show **baseline WAPE vs Prophet WAPE** (and MAPE) side by side so the "model beats baseline" story is explicit. Add a text callout with the % improvement.
- [ ] **Step 4: Polish** — title ("Daily Net Sales — 14-Day Forecast vs Actuals"), day-of-week filter, tooltips formatted as currency, captions noting the model + history depth.
- [ ] **Step 5: Publish** — File → Save to Tableau Public; capture the **public URL**. Verify it renders logged-out (incognito).
- [ ] **Step 6: Screenshot** — save a PNG into `docs/` for the README.

---

## Task 11: README writeup + portfolio embed + final merge

**Files:** Modify `README.md`; create `docs/forecast-writeup.md`; embed in the portfolio site.

- [ ] **Step 1: Writeup** — `docs/forecast-writeup.md`: the problem, the architecture (Toast → Databricks/dbt → Prophet → Tableau), the **headline result** (Prophet WAPE vs baseline, with the numbers from `backtest_metrics.csv`), the history depth (763 days / 2.5 yrs), and the embedded Tableau screenshot + public link.
- [ ] **Step 2: README** — add a "Forecasting" section: how to run (`python -m forecast.run_forecast`), the metrics table, the live Tableau Public link, and a note that the model runs locally against the exported mart (§11).
- [ ] **Step 3: Portfolio embed** — add the Tableau Public embed/link to the portfolio site (`[[project_job_search]]`), with a one-paragraph summary and the result metric.
- [ ] **Step 4: Run the full suite + commit**

```powershell
.\.venv\Scripts\python.exe -m pytest -q
git add README.md docs/
git commit -m "docs: forecast writeup, results, and Tableau Public embed"
```

- [ ] **Step 5: Finish the branch** — use **superpowers:finishing-a-development-branch** to merge to `main` (verify tests, fast-forward, delete branch).

---

## Self-review (spec coverage check)

- **§11 target = daily net_sales** → Tasks 3–8. ✅
- **§11 weekly + yearly + US holidays** → Task 6 (Prophet, `yearly=True` by default; 2.5 yrs supports it). ✅
- **§11 baseline seasonal-naive** → Task 4. ✅
- **§11 rolling-origin backtest, horizon 14, MAPE/RMSE/MAE** → Tasks 2, 5 (+ WAPE for zero-day robustness). ✅
- **§11 SARIMA comparison** → Task 9a (optional — YAGNI unless Prophet underperforms). ✅ (scoped out with rationale)
- **§11 runs locally against exported marts** → Tasks 3 + 8 (reads mart via `connect()`, fits locally). ✅
- **§10 forecast_daily_sales mart** → Task 9b (optional; CSV is the MVP deliverable). ✅ (scoped)
- **§16 M3 (forecast beats baseline) / M4 (Tableau + portfolio)** → Tasks 8 / 10–11. ✅
- **§18 success #3 (beat baseline, documented) / #4 (published + embedded dashboard)** → Tasks 8 / 10–11. ✅
- **§14 no PII, real aggregate figures publishable** → exports are aggregates only; series carries only `ds`+`y`. ✅
- **Closed-day zeros** (forecasting-specific, not in spec) → handled in Tasks 2 (zero-safe metrics) + 3 (0-fill). ✅

**Open decisions deferred to the user at execution:** forecast horizon (default 14 per spec), whether to do the optional SARIMA/Databricks-table tasks, and the exact Tableau default date window.
