"""Load all order Parquet partitions into the Delta bronze table.

Usage:
    python -m load.run_load
"""

from __future__ import annotations

from dotenv import load_dotenv

from load.databricks import connect
from load.load_to_delta import load_parquet_root


def main() -> None:
    load_dotenv()
    conn = connect()
    try:
        total = load_parquet_root(conn, "data/raw/orders")
    finally:
        conn.close()
    print(f"Loaded {total} rows into bronze_orders")


if __name__ == "__main__":
    main()
