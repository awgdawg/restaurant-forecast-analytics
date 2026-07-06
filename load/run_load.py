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
from load.load_to_delta import (
    bronze_ddl,
    days_needing_load,
    load_parquet_root,
    parquet_day_counts,
)
from load.spark_io import active_spark, spark_day_counts, spark_load_day


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load order Parquet into Delta bronze."
    )
    parser.add_argument(
        "--root",
        default="data/raw/orders",
        help="Parquet root (local path or /Volumes/... in-cloud)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="reload every day on disk (ignores --window)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=0,
        help="always reload the most recent N days on disk (captures same-count edits)",
    )
    args = parser.parse_args()

    load_dotenv()

    spark = active_spark()
    if spark is not None:
        # In-cloud: serverless egress blocks the SQL connector, so read and
        # write through the job's own Spark session. connect() is never called.
        spark.sql(bronze_ddl("bronze_orders"))
        on_disk = parquet_day_counts(args.root)
        todo = (
            sorted(on_disk)
            if args.full_refresh
            else days_needing_load(
                on_disk, spark_day_counts(spark, "bronze_orders"), args.window
            )
        )
        print(f"{len(on_disk)} days on disk; loading {len(todo)}")
        total = 0
        for bd in todo:
            n = spark_load_day(
                spark,
                "bronze_orders",
                bd,
                f"{args.root}/business_date={bd}/orders.parquet",
            )
            total += n
            print(f"{bd}: {n} rows")
        print(f"Loaded {total} rows into bronze_orders")
        return

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
