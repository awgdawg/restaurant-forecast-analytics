"""Write forecast outputs to Delta: forecast_daily_sales + model_metrics.

forecast_daily_sales is overwritten each run (it holds only the latest
horizon); model_metrics is append-only (a history of backtest results).
Tables land in the connection's default schema, alongside bronze_orders.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

FORECAST_TABLE = "forecast_daily_sales"
METRICS_TABLE = "model_metrics"

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


def _ddl(table: str, types: dict[str, str]) -> str:
    cols = ",\n  ".join(f"{c} {t}" for c, t in types.items())
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA"


def forecast_ddl(table: str = FORECAST_TABLE) -> str:
    return _ddl(table, _FORECAST_TYPES)


def metrics_ddl(table: str = METRICS_TABLE) -> str:
    return _ddl(table, _METRICS_TYPES)


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
        vals = [None if pd.isna(v) else float(v) for v in (r.yhat, r.yhat_lower, r.yhat_upper)]
        rows.append((int(r.ds.strftime("%Y%m%d")), *vals, model, run_ts))
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
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, FORECAST_COLUMNS, len(rows)), params)
    return len(rows)


def write_metrics(
    cursor, metrics_by_model: dict, horizon: int, n_folds: int, run_ts: datetime,
    table: str = METRICS_TABLE,
) -> int:
    """Append this run's backtest metrics (keeps history across runs)."""
    cursor.execute(metrics_ddl(table))
    rows = metrics_rows(metrics_by_model, horizon, n_folds, run_ts)
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, METRICS_COLUMNS, len(rows)), params)
    return len(rows)
