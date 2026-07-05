"""Load order Parquet partitions into the Delta bronze table.

Incremental by default: only days missing from bronze (or with mismatched row
counts) are loaded. Use --full-refresh to reload every day on disk.

Usage:
    python -m load.run_load [--root data/raw/orders] [--full-refresh] [--window N]
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from load.databricks import connect
from load.load_to_delta import load_parquet_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Load order Parquet into Delta bronze.")
    parser.add_argument(
        "--root",
        default="data/raw/orders",
        help="Parquet root (local path or /Volumes/... in-cloud)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="reload every day on disk"
    )
    parser.add_argument(
        "--window",
        type=int,
        default=0,
        help="always reload the most recent N days on disk (captures same-count edits)",
    )
    args = parser.parse_args()

    load_dotenv()
    conn = connect()
    try:
        total = load_parquet_root(
            conn,
            args.root,
            full_refresh=args.full_refresh,
            window=args.window,
            log=print,
        )
    finally:
        conn.close()
    print(f"Loaded {total} rows into bronze_orders")


if __name__ == "__main__":
    main()
