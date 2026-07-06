"""Tests for the Spark-native bronze I/O path (load/spark_io.py).

These fakes mirror only the slice of the Spark DataFrame API the module touches:
- spark.sql(q) -> FakeDF whose .collect() yields preset Rows
- spark.read.parquet(path) -> FakeDF with .select(*cols), .count(), and a
  .write chain (.format/.mode/.option/.saveAsTable) that records its args.

Row for day-counts behaves like a 2-tuple, matching the `for bd, n in ...`
unpacking that spark_day_counts does against Spark Row objects.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from load.load_forecast import FORECAST_TABLE, METRICS_TABLE
from load.load_to_delta import COLUMNS
from load.spark_io import (
    active_spark,
    spark_day_counts,
    spark_load_day,
    spark_write_forecast,
    spark_write_metrics,
)


class FakeWrite:
    """Records the .format/.mode/.option/.saveAsTable chain."""

    def __init__(self):
        self.calls = {}

    def format(self, fmt):
        self.calls["format"] = fmt
        return self

    def mode(self, mode):
        self.calls["mode"] = mode
        return self

    def option(self, key, value):
        self.calls.setdefault("options", {})[key] = value
        return self

    def saveAsTable(self, table):
        self.calls["saveAsTable"] = table
        return None


class FakeDF:
    def __init__(self, rows=None, count=0):
        self._rows = rows or []
        self._count = count
        self.selected = None
        self.write = FakeWrite()

    def collect(self):
        return self._rows

    def select(self, *cols):
        self.selected = list(cols)
        return self

    def count(self):
        return self._count


class FakeReader:
    def __init__(self, df):
        self._df = df
        self.parquet_path = None

    def parquet(self, path):
        self.parquet_path = path
        return self._df


class FakeSpark:
    def __init__(self, sql_df=None, read_df=None):
        self.sql_queries = []
        self._sql_df = sql_df or FakeDF()
        self.read = FakeReader(read_df or FakeDF())
        # createDataFrame recorder: captures (rows, schema) and hands back a
        # FakeDF whose .write chain records mode/saveAsTable.
        self.created = []  # list of (rows, schema)
        self.created_df = None

    def sql(self, query):
        self.sql_queries.append(query)
        return self._sql_df

    def createDataFrame(self, rows, schema=None):
        self.created.append((rows, schema))
        self.created_df = FakeDF()
        return self.created_df


def test_active_spark_returns_none_locally():
    """pyspark is genuinely absent from this venv, so the lazy import inside
    active_spark() raises ImportError and the function returns None. This
    exercises the real ImportError path, not a mock."""
    assert active_spark() is None


def test_spark_day_counts_parses_rows_and_queries_table():
    rows = [(20260627, 179), (20260628, 90)]
    spark = FakeSpark(sql_df=FakeDF(rows=rows))

    counts = spark_day_counts(spark, "bronze_orders")

    assert counts == {20260627: 179, 20260628: 90}
    assert len(spark.sql_queries) == 1
    query = spark.sql_queries[0]
    assert "SELECT business_date, COUNT(*)" in query
    assert "FROM bronze_orders" in query
    assert "GROUP BY business_date" in query


def test_spark_day_counts_coerces_to_int():
    rows = [("20260627", "179")]
    spark = FakeSpark(sql_df=FakeDF(rows=rows))

    assert spark_day_counts(spark, "bronze_orders") == {20260627: 179}


def test_spark_load_day_projects_columns_writes_and_returns_count():
    read_df = FakeDF(count=42)
    spark = FakeSpark(read_df=read_df)

    n = spark_load_day(
        spark,
        "bronze_orders",
        20260627,
        "/Volumes/x/business_date=20260627/orders.parquet",
    )

    # read the right parquet path
    assert spark.read.parquet_path == "/Volumes/x/business_date=20260627/orders.parquet"
    # projected to the exact bronze column order (by-name resolution)
    assert read_df.selected == COLUMNS
    # returns the count taken BEFORE the write consumed the plan
    assert n == 42
    # wrote delta / overwrite with the exact replaceWhere predicate + table
    assert read_df.write.calls["format"] == "delta"
    assert read_df.write.calls["mode"] == "overwrite"
    assert read_df.write.calls["options"] == {
        "replaceWhere": "business_date = 20260627"
    }
    assert read_df.write.calls["saveAsTable"] == "bronze_orders"


def test_spark_load_day_replacewhere_uses_int_business_date():
    """A string business_date is coerced to int in the predicate (no quotes,
    no injection surface)."""
    read_df = FakeDF(count=1)
    spark = FakeSpark(read_df=read_df)

    spark_load_day(spark, "bronze_orders", "20260628", "/Volumes/x/orders.parquet")

    assert read_df.write.calls["options"]["replaceWhere"] == "business_date = 20260628"


# --- Spark forecast/metrics writers ------------------------------------------
#
# The real schema builders (_forecast_schema/_metrics_schema) do a lazy pyspark
# import, so they cannot run in this pyspark-free venv. That is exactly why the
# writers obtain their schema from those seam helpers: tests monkeypatch the
# helpers with a sentinel and assert the writer forwarded *that* object into
# createDataFrame(rows, schema). Asserting a non-None schema was passed is the
# load-bearing check -- it proves the writer never lets Spark infer types (which
# fails on the all-None baseline band).

RUN_TS = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)

_FC_SENTINEL = object()
_METRICS_SENTINEL = object()


def _pin_schemas(monkeypatch):
    """Replace both lazy schema builders with sentinels so no pyspark import
    happens locally, and the writer's passed-through schema is identifiable."""
    monkeypatch.setattr("load.spark_io._forecast_schema", lambda: _FC_SENTINEL)
    monkeypatch.setattr("load.spark_io._metrics_schema", lambda: _METRICS_SENTINEL)


def test_spark_write_forecast_prophet_shape_overwrites_with_schema(monkeypatch):
    _pin_schemas(monkeypatch)
    fc = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-29", periods=2, freq="D"),
            "yhat": [100.0, 200.0],
            "yhat_lower": [90.0, 190.0],
            "yhat_upper": [110.0, 210.0],
        }
    )
    spark = FakeSpark()

    n = spark_write_forecast(spark, fc, model="prophet", run_ts=RUN_TS)

    assert n == 2
    rows, schema = spark.created[0]
    # explicit schema forwarded (not inferred)
    assert schema is _FC_SENTINEL
    # rows carry the band values from the prophet-shaped frame
    assert rows[0] == (20260629, 100.0, 90.0, 110.0, "prophet", RUN_TS)
    # latest-only overwrite into the forecast table
    assert spark.created_df.write.calls["format"] == "delta"
    assert spark.created_df.write.calls["mode"] == "overwrite"
    assert spark.created_df.write.calls["saveAsTable"] == FORECAST_TABLE


def test_spark_write_forecast_baseline_band_less_shape_uses_explicit_schema(
    monkeypatch,
):
    """LOAD-BEARING: a band-less baseline frame yields rows whose band values are
    all None. createDataFrame must receive the explicit schema, because Spark
    cannot infer types from all-None columns."""
    _pin_schemas(monkeypatch)
    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=2, freq="D"), "yhat": [100.0, 200.0]}
    )  # baseline shape: NO yhat_lower/yhat_upper
    spark = FakeSpark()

    n = spark_write_forecast(spark, fc, model="baseline", run_ts=RUN_TS)

    assert n == 2
    rows, schema = spark.created[0]
    # band values are None (un-inferrable) ...
    assert rows[0] == (20260629, 100.0, None, None, "baseline", RUN_TS)
    assert rows[1] == (20260630, 200.0, None, None, "baseline", RUN_TS)
    # ... so the explicit schema MUST have been passed, not left to inference
    assert schema is _FC_SENTINEL


def test_spark_write_forecast_custom_table(monkeypatch):
    _pin_schemas(monkeypatch)
    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=1, freq="D"), "yhat": [1.0]}
    )
    spark = FakeSpark()

    spark_write_forecast(spark, fc, model="baseline", run_ts=RUN_TS, table="cat.sch.fc")

    assert spark.created_df.write.calls["saveAsTable"] == "cat.sch.fc"


def test_spark_write_forecast_empty_returns_zero_no_create(monkeypatch):
    _pin_schemas(monkeypatch)
    spark = FakeSpark()

    n = spark_write_forecast(
        spark, pd.DataFrame(columns=["ds", "yhat"]), model="baseline", run_ts=RUN_TS
    )

    assert n == 0
    assert spark.created == []  # no createDataFrame, no write


def test_spark_write_metrics_appends_with_schema(monkeypatch):
    _pin_schemas(monkeypatch)
    metrics = {
        "baseline": {"mae": 434.0, "rmse": 605.0, "mape": 18.0, "wape": 16.6},
        "prophet": {"mae": 351.0, "rmse": 456.0, "mape": 31.6, "wape": 13.4},
    }
    spark = FakeSpark()

    n = spark_write_metrics(spark, metrics, horizon=14, n_folds=8, run_ts=RUN_TS)

    assert n == 2
    rows, schema = spark.created[0]
    assert schema is _METRICS_SENTINEL
    assert ("prophet", 351.0, 456.0, 31.6, 13.4, 14, 8, RUN_TS) in rows
    # append-only (history preserved across runs)
    assert spark.created_df.write.calls["mode"] == "append"
    assert spark.created_df.write.calls["saveAsTable"] == METRICS_TABLE


def test_spark_write_metrics_empty_returns_zero_no_create(monkeypatch):
    _pin_schemas(monkeypatch)
    spark = FakeSpark()

    n = spark_write_metrics(spark, {}, horizon=14, n_folds=8, run_ts=RUN_TS)

    assert n == 0
    assert spark.created == []
