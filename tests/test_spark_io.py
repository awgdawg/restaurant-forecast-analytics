"""Tests for the Spark-native bronze I/O path (load/spark_io.py).

These fakes mirror only the slice of the Spark DataFrame API the module touches:
- spark.sql(q) -> FakeDF whose .collect() yields preset Rows
- spark.read.parquet(path) -> FakeDF with .select(*cols), .count(), and a
  .write chain (.format/.mode/.option/.saveAsTable) that records its args.

Row for day-counts behaves like a 2-tuple, matching the `for bd, n in ...`
unpacking that spark_day_counts does against Spark Row objects.
"""

from __future__ import annotations

from load.load_to_delta import COLUMNS
from load.spark_io import active_spark, spark_day_counts, spark_load_day


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

    def sql(self, query):
        self.sql_queries.append(query)
        return self._sql_df


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
