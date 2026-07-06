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

from load.load_forecast import (
    FORECAST_TABLE,
    METRICS_TABLE,
    forecast_rows,
    metrics_rows,
)
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


# --- schema-pinned forecast/metrics writers ----------------------------------
#
# Explicit schemas are LOAD-BEARING here. spark.createDataFrame on forecast_rows
# output CANNOT infer types when the baseline model wins: its band columns
# (yhat_lower/yhat_upper) are all None, and Spark has no type to infer from
# an all-None column. So every write pins an explicit schema.
#
# The schema builders below do the pyspark import lazily *inside* the function
# (same pattern as active_spark) so importing this module never needs pyspark.
# The writers obtain their schema by calling these helpers -- that indirection
# is the seam local, pyspark-free tests monkeypatch (they swap in a sentinel
# schema and assert the writer forwarded it to createDataFrame). See
# tests/test_spark_io.py.


def _forecast_schema():
    """Explicit StructType for forecast_daily_sales, mirroring
    load/load_forecast.py::_FORECAST_TYPES. Band fields are nullable (baseline
    forecasts carry no band -> all-None columns)."""
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("forecast_date", LongType(), nullable=False),
            StructField("yhat", DoubleType(), nullable=False),
            StructField("yhat_lower", DoubleType(), nullable=True),
            StructField("yhat_upper", DoubleType(), nullable=True),
            StructField("model", StringType(), nullable=False),
            StructField("run_ts", TimestampType(), nullable=False),
        ]
    )


def _metrics_schema():
    """Explicit StructType for model_metrics, mirroring
    load/load_forecast.py::_METRICS_TYPES. horizon/n_folds are Python ints ->
    LongType (Spark's default integer width, avoids overflow surprises)."""
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("model", StringType(), nullable=False),
            StructField("mae", DoubleType(), nullable=True),
            StructField("rmse", DoubleType(), nullable=True),
            StructField("mape", DoubleType(), nullable=True),
            StructField("wape", DoubleType(), nullable=True),
            StructField("horizon", LongType(), nullable=False),
            StructField("n_folds", LongType(), nullable=False),
            StructField("run_ts", TimestampType(), nullable=False),
        ]
    )


def spark_write_forecast(spark, fc, model, run_ts, table=FORECAST_TABLE) -> int:
    """Latest-only write of this run's horizon to forecast_daily_sales.

    Mirrors load_forecast.write_forecast: rows come from the same pure
    forecast_rows() builder; the overwrite is the Delta equivalent of that
    path's DELETE-all + INSERT. Empty rows -> no write (matches the connector
    writer's guard). Returns the row count written.
    """
    rows = forecast_rows(fc, model, run_ts)
    if not rows:
        return 0
    df = spark.createDataFrame(rows, _forecast_schema())
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    return len(rows)


def spark_write_metrics(
    spark, metrics_by_model, horizon, n_folds, run_ts, table=METRICS_TABLE
) -> int:
    """Append this run's backtest metrics to model_metrics (keeps history).

    Mirrors load_forecast.write_metrics: rows from the same pure metrics_rows()
    builder, append mode. Empty -> no write. Returns the row count written.
    """
    rows = metrics_rows(metrics_by_model, horizon, n_folds, run_ts)
    if not rows:
        return 0
    df = spark.createDataFrame(rows, _metrics_schema())
    df.write.format("delta").mode("append").saveAsTable(table)
    return len(rows)
