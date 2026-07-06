"""Spark-native bronze I/O, for in-cloud runs on serverless job compute.

The workspace's serverless egress allowlist blocks the SQL connector (even to
its own warehouse), so in-cloud tasks do their reads and writes through the
job's own active Spark session instead of a `databricks.sql` connection.

Locally, pyspark isn't installed at all -- that absence is the detection seam:
`active_spark()` returns None and `run_load.main()` falls back to the connector
path unchanged. Each function mirrors its connector-path twin in
load/load_to_delta.py so the two paths stay behaviourally identical.
"""

from __future__ import annotations

from load.load_to_delta import COLUMNS


def active_spark():
    """Return the active SparkSession, or None when there isn't one.

    The import is lazy and inside the function so that importing this module
    never requires pyspark: local/CI runs have no pyspark, so the ImportError
    is caught and None is returned, steering run_load to the connector path.
    On serverless job compute pyspark is present and a session is already
    active, so getActiveSession() returns it.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


def spark_day_counts(spark, table: str) -> dict[int, int]:
    """business_date -> row count in the bronze table (mirrors bronze_day_counts)."""
    rows = spark.sql(
        f"SELECT business_date, COUNT(*) FROM {table} GROUP BY business_date"
    ).collect()
    return {int(bd): int(n) for bd, n in rows}


def spark_load_day(spark, table: str, business_date, parquet_path: str) -> int:
    """Idempotently replace one day's rows in bronze from a Parquet file.

    Reads the Parquet, projects to the bronze column order by name (guarding
    against parquet column-order drift), then replaces just that day via Delta
    replaceWhere -- the Spark equivalent of load_day's DELETE+INSERT.

    Returns the row count taken BEFORE the write, because the write consumes the
    query plan and a post-write count() would re-scan the (now replaced) table.
    """
    df = spark.read.parquet(parquet_path).select(*COLUMNS)
    n = df.count()
    df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"business_date = {int(business_date)}"
    ).saveAsTable(table)
    return n
