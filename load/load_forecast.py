"""Write forecast outputs to Delta: forecast_daily_sales + model_metrics +
forecast_history.

forecast_daily_sales is overwritten each run (it holds only the latest
horizon); model_metrics and forecast_history are append-only. forecast_history
archives every run's winning-model horizon (same columns as
forecast_daily_sales, stamped with that run's run_ts) as an as-of record for
future lead-time accuracy analysis. Tables land in the connection's default
schema, alongside bronze_orders.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

FORECAST_TABLE = "forecast_daily_sales"
METRICS_TABLE = "model_metrics"
HISTORY_TABLE = "forecast_history"
BACKTEST_TABLE = "backtest_predictions"

_FORECAST_TYPES = {
    "forecast_date": "BIGINT",
    "yhat": "DOUBLE",
    "yhat_lower": "DOUBLE",
    "yhat_upper": "DOUBLE",
    "model": "STRING",
    "run_ts": "TIMESTAMP",
}
FORECAST_COLUMNS = list(_FORECAST_TYPES)

_METRICS_TYPES = {
    "model": "STRING",
    "mae": "DOUBLE",
    "rmse": "DOUBLE",
    "mape": "DOUBLE",
    "wape": "DOUBLE",
    "horizon": "INT",
    "n_folds": "INT",
    "run_ts": "TIMESTAMP",
}
METRICS_COLUMNS = list(_METRICS_TYPES)

_BACKTEST_TYPES = {
    "forecast_date": "BIGINT",
    "yhat": "DOUBLE",
    "model": "STRING",
    "fold": "INT",
    "run_ts": "TIMESTAMP",
}
BACKTEST_COLUMNS = list(_BACKTEST_TYPES)


def _ddl(table: str, types: dict[str, str]) -> str:
    cols = ",\n  ".join(f"{c} {t}" for c, t in types.items())
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA"


def forecast_ddl(table: str = FORECAST_TABLE) -> str:
    return _ddl(table, _FORECAST_TYPES)


def metrics_ddl(table: str = METRICS_TABLE) -> str:
    return _ddl(table, _METRICS_TYPES)


def history_ddl(table: str = HISTORY_TABLE) -> str:
    # Same columns as forecast_daily_sales (reuses _FORECAST_TYPES).
    return _ddl(table, _FORECAST_TYPES)


def backtest_ddl(table: str = BACKTEST_TABLE) -> str:
    return _ddl(table, _BACKTEST_TYPES)


def _insert_sql(table: str, columns: list[str], n_rows: int) -> str:
    row = "(" + ", ".join(["?"] * len(columns)) + ")"
    values = ", ".join([row] * n_rows)
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values}"


def forecast_rows(fc: pd.DataFrame, model: str, run_ts: datetime) -> list[tuple]:
    """fc: df[ds, yhat(, yhat_lower, yhat_upper)] -> FORECAST_COLUMNS-ordered rows.
    Baseline forecasts carry no band; missing columns become SQL NULLs."""
    out = fc.reindex(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])
    rows = []
    for r in out.itertuples(index=False):
        vals = [
            None if pd.isna(v) else float(v)
            for v in (r.yhat, r.yhat_lower, r.yhat_upper)
        ]
        rows.append((int(r.ds.strftime("%Y%m%d")), *vals, model, run_ts))
    return rows


def backtest_rows(bt: pd.DataFrame, model: str, run_ts: datetime) -> list[tuple]:
    """bt: rolling_origin_backtest long df[ds, y, yhat, fold] -> BACKTEST_COLUMNS-
    ordered rows for one model. ds -> yyyymmdd int (same as forecast_rows); the
    actual column y is dropped (the view sources actuals from fct_daily_sales).
    fold is coerced to a plain int (the table's fold column is INT)."""
    rows = []
    for r in bt.itertuples(index=False):
        rows.append(
            (
                int(r.ds.strftime("%Y%m%d")),
                float(r.yhat),
                model,
                int(r.fold),
                run_ts,
            )
        )
    return rows


def metrics_rows(
    metrics_by_model: dict, horizon: int, n_folds: int, run_ts: datetime
) -> list[tuple]:
    return [
        (name, m["mae"], m["rmse"], m["mape"], m["wape"], horizon, n_folds, run_ts)
        for name, m in metrics_by_model.items()
    ]


def write_forecast(
    cursor, fc: pd.DataFrame, model: str, run_ts: datetime, table: str = FORECAST_TABLE
) -> int:
    """Replace the table's contents with this run's horizon (latest-only)."""
    cursor.execute(forecast_ddl(table))
    cursor.execute(f"DELETE FROM {table}")
    rows = forecast_rows(fc, model, run_ts)
    if not rows:
        return 0
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, FORECAST_COLUMNS, len(rows)), params)
    return len(rows)


def write_metrics(
    cursor,
    metrics_by_model: dict,
    horizon: int,
    n_folds: int,
    run_ts: datetime,
    table: str = METRICS_TABLE,
) -> int:
    """Append this run's backtest metrics (keeps history across runs)."""
    cursor.execute(metrics_ddl(table))
    rows = metrics_rows(metrics_by_model, horizon, n_folds, run_ts)
    if not rows:
        return 0
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, METRICS_COLUMNS, len(rows)), params)
    return len(rows)


def write_backtest_predictions(
    cursor,
    bt_by_model: dict[str, pd.DataFrame],
    run_ts: datetime,
    table: str = BACKTEST_TABLE,
) -> int:
    """Replace the table with this run's out-of-sample backtest for BOTH models
    (latest-only, ~112 days x 2 models). Returns total rows written.

    Design note (the sharp edge): unlike write_forecast (one model per call), a
    single run persists two models' frames. So this writer takes a dict of
    model -> backtest frame and DELETEs exactly ONCE up front, then appends each
    model's rows. A per-model writer that each DELETEd would have the second
    model's write wipe the first. Mirrors write_forecast's DELETE+INSERT
    overwrite, generalized to multiple frames in one run."""
    cursor.execute(backtest_ddl(table))
    cursor.execute(f"DELETE FROM {table}")
    total = 0
    for model, bt in bt_by_model.items():
        rows = backtest_rows(bt, model, run_ts)
        if not rows:
            continue
        params = [v for row in rows for v in row]
        cursor.execute(_insert_sql(table, BACKTEST_COLUMNS, len(rows)), params)
        total += len(rows)
    return total


def write_forecast_history(
    cursor, fc: pd.DataFrame, model: str, run_ts: datetime, table: str = HISTORY_TABLE
) -> int:
    """Append this run's horizon to the as-of archive (keeps every run's
    forecast for later lead-time accuracy analysis). Same rows as
    write_forecast, but append-only (no DELETE) -- mirrors write_metrics."""
    cursor.execute(history_ddl(table))
    rows = forecast_rows(fc, model, run_ts)
    if not rows:
        return 0
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, FORECAST_COLUMNS, len(rows)), params)
    return len(rows)
