"""Load order Parquet partitions into a Databricks Delta bronze table.

Idempotent per business date: each day's rows are deleted then re-inserted, so
re-running a date range never duplicates.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

COLUMNS = [
    "business_date",
    "order_guid",
    "opened_date",
    "closed_date",
    "source",
    "dining_option_guid",
    "num_guests",
    "num_checks",
    "net_amount",
    "total_amount",
    "tax_amount",
    "tip_amount",
    "voided",
    "deleted",
]

_DDL_TYPES = {
    "business_date": "BIGINT",
    "order_guid": "STRING",
    "opened_date": "STRING",
    "closed_date": "STRING",
    "source": "STRING",
    "dining_option_guid": "STRING",
    "num_guests": "INT",
    "num_checks": "INT",
    "net_amount": "DOUBLE",
    "total_amount": "DOUBLE",
    "tax_amount": "DOUBLE",
    "tip_amount": "DOUBLE",
    "voided": "BOOLEAN",
    "deleted": "BOOLEAN",
}


def bronze_ddl(table: str) -> str:
    cols = ",\n  ".join(f"{c} {_DDL_TYPES[c]}" for c in COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA"


INSERT_CHUNK = 500


def insert_sql(table: str, n_rows: int = 1) -> str:
    row = "(" + ", ".join(["?"] * len(COLUMNS)) + ")"
    values = ", ".join([row] * n_rows)
    cols = ", ".join(COLUMNS)
    return f"INSERT INTO {table} ({cols}) VALUES {values}"


def rows_from_df(df: pd.DataFrame) -> list[tuple]:
    return [tuple(r) for r in df[COLUMNS].itertuples(index=False, name=None)]


def load_day(cursor, table: str, business_date: int, df: pd.DataFrame) -> int:
    """Replace a day's rows. Inserts in chunks of one multi-row statement each
    (far fewer round-trips than row-by-row executemany — essential for backfills)."""
    cursor.execute(f"DELETE FROM {table} WHERE business_date = ?", [int(business_date)])
    rows = rows_from_df(df)
    for start in range(0, len(rows), INSERT_CHUNK):
        chunk = rows[start : start + INSERT_CHUNK]
        params = [value for row in chunk for value in row]
        cursor.execute(insert_sql(table, len(chunk)), params)
    return len(df)


def load_parquet_root(conn, root: str | Path, table: str = "bronze_orders") -> int:
    cursor = conn.cursor()
    cursor.execute(bronze_ddl(table))
    total = 0
    for part in sorted(Path(root).glob("business_date=*")):
        business_date = int(part.name.split("=")[1])
        df = pd.read_parquet(part / "orders.parquet")
        total += load_day(cursor, table, business_date, df)
    cursor.close()
    return total
