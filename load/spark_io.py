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
    BACKTEST_TABLE,
    FORECAST_TABLE,
    HISTORY_TABLE,
    METRICS_TABLE,
    backtest_rows,
    forecast_rows,
    metrics_rows,
)
from load.load_to_delta import _DDL_TYPES


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

    Reads the Parquet, then cast-projects every column to its bronze DDL type
    in bronze column order (by name, guarding against parquet column-order
    drift). The casts are load-bearing: pandas writes nullable ints as
    int64/double, which Spark reads as Long/Double, and saveAsTable refuses to
    merge those into the table's INT columns (DELTA_FAILED_TO_MERGE_FIELDS).
    The SQL connector coerced server-side, so only this path needs the casts.
    Then replaces just that day via Delta replaceWhere -- the Spark equivalent
    of load_day's DELETE+INSERT.

    Returns the row count taken BEFORE the write, because the write consumes the
    query plan and a post-write count() would re-scan the (now replaced) table.
    """
    df = spark.read.parquet(parquet_path).selectExpr(
        *[f"CAST({c} AS {t}) AS {c}" for c, t in _DDL_TYPES.items()]
    )
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
    load/load_forecast.py::_METRICS_TYPES. horizon/n_folds are IntegerType, NOT
    LongType: the live table was created by the connector path with INT columns,
    and spark_write_metrics APPENDS -- appending BIGINT into INT fails with
    DELTA_FAILED_TO_MERGE_FIELDS. The values are tiny (horizon=14, n_folds=8),
    so IntegerType is safe and matches the live table exactly."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
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
            StructField("horizon", IntegerType(), nullable=False),
            StructField("n_folds", IntegerType(), nullable=False),
            StructField("run_ts", TimestampType(), nullable=False),
        ]
    )


def _backtest_schema():
    """Explicit StructType for backtest_predictions, mirroring
    load/load_forecast.py::_BACKTEST_TYPES. fold is IntegerType, NOT LongType:
    same INT trap as model_metrics -- the connector path (and backtest_ddl)
    create fold as INT, and a Delta write of BIGINT into an INT column fails
    with DELTA_FAILED_TO_MERGE_FIELDS. fold values are tiny (0..7), so
    IntegerType is safe and matches the table exactly."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
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
            StructField("model", StringType(), nullable=False),
            StructField("fold", IntegerType(), nullable=False),
            StructField("run_ts", TimestampType(), nullable=False),
        ]
    )


def spark_write_backtest_predictions(
    spark, bt_by_model, run_ts, table=BACKTEST_TABLE
) -> int:
    """Latest-only write of this run's out-of-sample backtest for BOTH models to
    backtest_predictions. Returns total rows written.

    Mirrors load_forecast.write_backtest_predictions: rows come from the same
    pure backtest_rows() builder. Overwrite-once semantics: both models' rows are
    concatenated into a SINGLE createDataFrame + one mode('overwrite') write, so
    there is no partial-overwrite trap (two separate overwrites would have the
    second wipe the first). Empty -> no write (matches the connector guard)."""
    rows = []
    for model, bt in bt_by_model.items():
        rows.extend(backtest_rows(bt, model, run_ts))
    if not rows:
        return 0
    df = spark.createDataFrame(rows, _backtest_schema())
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    return len(rows)


def spark_write_forecast(spark, fc, model, run_ts, table=FORECAST_TABLE) -> int:
    """Latest-only write of this run's horizon to forecast_daily_sales.

    Mirrors load_forecast.write_forecast: rows come from the same pure
    forecast_rows() builder; the overwrite is the Delta equivalent of that
    path's DELETE-all + INSERT. Empty rows -> no write (matches the connector
    writer's guard). Returns the row count written.

    Note on overwrite semantics: unlike the connector path (DDL + DELETE +
    INSERT into a pre-shaped table), mode("overwrite").saveAsTable REDEFINES
    forecast_daily_sales with the DataFrame's schema on every run. That is
    intentional and safe here: the table is latest-only (no history to
    preserve), and the pinned _forecast_schema() mirrors _FORECAST_TYPES
    exactly (LongType=BIGINT, DoubleType=DOUBLE, ...), so the redefined table
    is identical to the connector-created one.
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


def spark_write_forecast_history(spark, fc, model, run_ts, table=HISTORY_TABLE) -> int:
    """Append this run's horizon to forecast_history (as-of archive; keeps every
    run's forecast across runs for later lead-time accuracy analysis).

    Mirrors load_forecast.write_forecast_history: rows from the same pure
    forecast_rows() builder, the pinned _forecast_schema() (baseline band
    columns are all-None and un-inferrable), append mode. Empty -> no write.
    Returns the row count written. Note: append + saveAsTable creates the table
    if absent, so -- unlike the connector path -- no explicit DDL is needed here.
    """
    rows = forecast_rows(fc, model, run_ts)
    if not rows:
        return 0
    df = spark.createDataFrame(rows, _forecast_schema())
    df.write.format("delta").mode("append").saveAsTable(table)
    return len(rows)
