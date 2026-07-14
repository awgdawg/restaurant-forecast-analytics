"""Tests for the Spark-native bronze I/O path (load/spark_io.py).

These fakes mirror only the slice of the Spark DataFrame API the module touches:
- spark.sql(q) -> FakeDF whose .collect() yields preset Rows
- spark.read.parquet(path) -> FakeDF with .selectExpr(*exprs), .count(), and a
  .write chain (.format/.mode/.option/.saveAsTable) that records its args.

Row for day-counts behaves like a 2-tuple, matching the `for bd, n in ...`
unpacking that spark_day_counts does against Spark Row objects.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from load.load_forecast import (
    BACKTEST_TABLE,
    FORECAST_TABLE,
    HISTORY_TABLE,
    METRICS_TABLE,
    backtest_rows,
    forecast_rows,
    metrics_rows,
)
from load.load_to_delta import _DDL_TYPES, COLUMNS
from load.spark_io import (
    active_spark,
    spark_day_counts,
    spark_load_day,
    spark_write_backtest_predictions,
    spark_write_forecast,
    spark_write_forecast_history,
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
        self.select_exprs = None
        self.write = FakeWrite()

    def collect(self):
        return self._rows

    def selectExpr(self, *exprs):
        self.select_exprs = list(exprs)
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


def test_spark_load_day_casts_columns_writes_and_returns_count():
    """The projection must CAST every column to its bronze DDL type. pandas
    writes nullable ints as int64/double in parquet, which Spark reads as
    Long/Double; without the cast, saveAsTable fails with
    DELTA_FAILED_TO_MERGE_FIELDS against the table's INT columns (the SQL
    connector coerced server-side, so this only bites the Spark path)."""
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
    # cast-projected: one CAST per column, to the exact DDL type, by name
    expected_exprs = [f"CAST({c} AS {t}) AS {c}" for c, t in _DDL_TYPES.items()]
    assert read_df.select_exprs == expected_exprs
    # the type that broke the live run is cast explicitly
    assert "CAST(num_guests AS INT) AS num_guests" in read_df.select_exprs
    assert "CAST(business_date AS BIGINT) AS business_date" in read_df.select_exprs
    # _DDL_TYPES iteration preserves the bronze column order, so the projected
    # output column order matches COLUMNS exactly
    assert [e.rsplit(" AS ", 1)[1] for e in read_df.select_exprs] == COLUMNS
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
_BACKTEST_SENTINEL = object()


def _pin_schemas(monkeypatch):
    """Replace the lazy schema builders with sentinels so no pyspark import
    happens locally, and the writer's passed-through schema is identifiable."""
    monkeypatch.setattr("load.spark_io._forecast_schema", lambda: _FC_SENTINEL)
    monkeypatch.setattr("load.spark_io._metrics_schema", lambda: _METRICS_SENTINEL)
    monkeypatch.setattr("load.spark_io._backtest_schema", lambda: _BACKTEST_SENTINEL)


def _bt(yhats, fold=0, start="2026-03-01"):
    """A rolling_origin_backtest-shaped frame: long df[ds, y, yhat, fold]."""
    n = len(yhats)
    return pd.DataFrame(
        {
            "ds": pd.date_range(start, periods=n, freq="D"),
            "y": [float(v) + 5.0 for v in yhats],
            "yhat": [float(v) for v in yhats],
            "fold": [fold] * n,
        }
    )


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


def test_spark_write_forecast_history_appends_with_schema(monkeypatch):
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

    n = spark_write_forecast_history(spark, fc, model="prophet", run_ts=RUN_TS)

    assert n == 2
    rows, schema = spark.created[0]
    # same explicit forecast schema as forecast_daily_sales (not inferred)
    assert schema is _FC_SENTINEL
    assert rows[0] == (20260629, 100.0, 90.0, 110.0, "prophet", RUN_TS)
    # append-only archive (every run's forecast preserved across runs)
    assert spark.created_df.write.calls["format"] == "delta"
    assert spark.created_df.write.calls["mode"] == "append"
    assert spark.created_df.write.calls["saveAsTable"] == HISTORY_TABLE


def test_spark_write_forecast_history_baseline_band_less_uses_schema(monkeypatch):
    """LOAD-BEARING: a band-less baseline frame yields all-None band values, which
    Spark cannot infer -- so the explicit schema MUST be forwarded here too."""
    _pin_schemas(monkeypatch)
    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=2, freq="D"), "yhat": [100.0, 200.0]}
    )  # baseline shape: NO yhat_lower/yhat_upper
    spark = FakeSpark()

    n = spark_write_forecast_history(spark, fc, model="baseline", run_ts=RUN_TS)

    assert n == 2
    rows, schema = spark.created[0]
    assert rows[0] == (20260629, 100.0, None, None, "baseline", RUN_TS)
    assert schema is _FC_SENTINEL
    assert spark.created_df.write.calls["mode"] == "append"


def test_spark_write_forecast_history_empty_returns_zero_no_create(monkeypatch):
    _pin_schemas(monkeypatch)
    spark = FakeSpark()

    n = spark_write_forecast_history(
        spark, pd.DataFrame(columns=["ds", "yhat"]), model="baseline", run_ts=RUN_TS
    )

    assert n == 0
    assert spark.created == []  # no createDataFrame, no write


def test_spark_write_backtest_predictions_overwrites_once_with_both_models(monkeypatch):
    """Overwrite-once semantics: ONE createDataFrame carrying BOTH models' rows,
    written with a SINGLE mode('overwrite') -- no per-model partial-overwrite
    trap (a second overwrite would wipe the first model's rows)."""
    _pin_schemas(monkeypatch)
    bt_by_model = {"baseline": _bt([10.0, 20.0]), "prophet": _bt([11.0, 21.0])}
    spark = FakeSpark()

    n = spark_write_backtest_predictions(spark, bt_by_model, run_ts=RUN_TS)

    assert n == 4  # 2 models x 2 rows, one combined write
    assert len(spark.created) == 1  # a single createDataFrame (one overwrite)
    rows, schema = spark.created[0]
    assert schema is _BACKTEST_SENTINEL  # explicit schema forwarded, not inferred
    # both models present in the single combined row set
    assert {r[2] for r in rows} == {"baseline", "prophet"}
    # backtest row shape: forecast_date int, yhat, model, fold int, run_ts
    assert (20260301, 10.0, "baseline", 0, RUN_TS) in rows
    assert spark.created_df.write.calls["format"] == "delta"
    assert spark.created_df.write.calls["mode"] == "overwrite"
    assert spark.created_df.write.calls["saveAsTable"] == BACKTEST_TABLE


def test_spark_write_backtest_predictions_custom_table(monkeypatch):
    _pin_schemas(monkeypatch)
    spark = FakeSpark()

    spark_write_backtest_predictions(
        spark, {"baseline": _bt([1.0])}, run_ts=RUN_TS, table="cat.sch.bt"
    )

    assert spark.created_df.write.calls["saveAsTable"] == "cat.sch.bt"


def test_spark_write_backtest_predictions_empty_returns_zero_no_create(monkeypatch):
    _pin_schemas(monkeypatch)
    spark = FakeSpark()

    n = spark_write_backtest_predictions(
        spark,
        {"baseline": pd.DataFrame(columns=["ds", "y", "yhat", "fold"])},
        run_ts=RUN_TS,
    )

    assert n == 0
    assert spark.created == []  # no createDataFrame, no write


def test_real_spark_schemas_bind_baseline_rows():
    """Pyspark-gated smoke test: SKIPS locally (pyspark is absent from the
    venv by design). In any pyspark-bearing environment (in-cloud, future CI)
    it exercises the REAL schema builders against a real createDataFrame --
    including the all-None-band baseline shape that Spark cannot infer types
    for, and the IntegerType horizon/n_folds that must match the live
    model_metrics table's INT columns."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from load.spark_io import _backtest_schema, _forecast_schema, _metrics_schema

    spark = SparkSession.getActiveSession() or (
        SparkSession.builder.master("local[1]").appName("schema-smoke").getOrCreate()
    )

    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=2, freq="D"), "yhat": [100.0, 200.0]}
    )  # baseline shape: NO yhat_lower/yhat_upper -> all-None band columns
    fc_df = spark.createDataFrame(
        forecast_rows(fc, "baseline", RUN_TS), _forecast_schema()
    )
    collected = fc_df.collect()
    assert len(collected) == 2
    assert collected[0]["forecast_date"] == 20260629
    assert collected[0]["yhat_lower"] is None  # bound only via the explicit schema

    metrics = {"baseline": {"mae": 1.0, "rmse": 2.0, "mape": 3.0, "wape": 4.0}}
    m_df = spark.createDataFrame(
        metrics_rows(metrics, horizon=14, n_folds=8, run_ts=RUN_TS), _metrics_schema()
    )
    assert m_df.collect()[0]["horizon"] == 14
    # IntegerType, matching the live table's INT columns (append-compatible)
    assert dict(m_df.dtypes)["horizon"] == "int"
    assert dict(m_df.dtypes)["n_folds"] == "int"

    bt = _bt([100.0, 200.0], fold=3)
    bt_df = spark.createDataFrame(
        backtest_rows(bt, "prophet", RUN_TS), _backtest_schema()
    )
    bt_collected = bt_df.collect()
    assert bt_collected[0]["forecast_date"] == 20260301
    assert bt_collected[0]["fold"] == 3
    # INT trap: fold MUST be IntegerType (mirrors metrics horizon/n_folds), else
    # a Delta overwrite/merge against the live INT column fails.
    assert dict(bt_df.dtypes)["fold"] == "int"
