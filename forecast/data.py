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
