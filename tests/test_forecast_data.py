import pandas as pd

from forecast.data import (
    clean_daily_series,
    load_daily_series_spark,
    mart_table,
    metrics_table,
)


def test_clean_fills_missing_days_with_zero_and_is_continuous():
    # 6/16 and 6/17 present, 6/18 missing (closed), 6/19 present
    raw = pd.DataFrame(
        {
            "business_date": [20260616, 20260617, 20260619],
            "net_sales": [100.0, 200.0, 300.0],
        }
    )

    out = clean_daily_series(raw)

    assert list(out.columns) == ["ds", "y"]
    assert str(out["ds"].dtype) == "datetime64[ns]"
    # continuous 6/16..6/19 = 4 rows, the gap day filled with 0.0
    assert list(out["ds"]) == list(pd.date_range("2026-06-16", "2026-06-19", freq="D"))
    assert list(out["y"]) == [100.0, 200.0, 0.0, 300.0]


def test_mart_table_builds_from_env(monkeypatch):
    monkeypatch.setenv("DBX_CATALOG", "maincat")
    monkeypatch.setenv("DBX_SCHEMA", "restaurant")
    assert mart_table() == "maincat.restaurant_marts.fct_daily_sales"


def test_mart_table_defaults_match_free_workspace(monkeypatch):
    monkeypatch.delenv("DBX_CATALOG", raising=False)
    monkeypatch.delenv("DBX_SCHEMA", raising=False)
    assert mart_table() == "workspace.default_marts.fct_daily_sales"


def test_metrics_table_builds_from_env(monkeypatch):
    monkeypatch.setenv("DBX_CATALOG", "maincat")
    monkeypatch.setenv("DBX_SCHEMA", "restaurant")
    # model_metrics is a forecast-written SOURCE table in the BASE schema,
    # NOT in <schema>_marts (unlike mart_table's fct_daily_sales).
    assert metrics_table() == "maincat.restaurant.model_metrics"


def test_metrics_table_defaults_match_free_workspace(monkeypatch):
    monkeypatch.delenv("DBX_CATALOG", raising=False)
    monkeypatch.delenv("DBX_SCHEMA", raising=False)
    assert metrics_table() == "workspace.default.model_metrics"


class _FakeSparkDF:
    """spark.sql(q).toPandas() -> a preset pandas frame."""

    def __init__(self, pdf):
        self._pdf = pdf

    def toPandas(self):
        return self._pdf


class _FakeSpark:
    def __init__(self, pdf):
        self._pdf = pdf
        self.sql_queries = []

    def sql(self, query):
        self.sql_queries.append(query)
        return _FakeSparkDF(self._pdf)


def test_load_daily_series_spark_queries_mart_and_cleans(monkeypatch):
    monkeypatch.delenv("DBX_CATALOG", raising=False)
    monkeypatch.delenv("DBX_SCHEMA", raising=False)
    # same gap-day scenario as clean_daily_series's own test, but arriving via Spark
    pdf = pd.DataFrame(
        {
            "business_date": [20260616, 20260617, 20260619],
            "net_sales": [100.0, 200.0, 300.0],
        }
    )
    spark = _FakeSpark(pdf)

    out = load_daily_series_spark(spark)

    # issued the right SQL against the default mart table
    assert len(spark.sql_queries) == 1
    q = spark.sql_queries[0]
    assert "select business_date, net_sales" in q
    assert "workspace.default_marts.fct_daily_sales" in q
    assert "order by business_date" in q
    # returned a cleaned, continuous [ds, y] series with the closed gap day filled 0.0
    assert list(out.columns) == ["ds", "y"]
    assert list(out["ds"]) == list(pd.date_range("2026-06-16", "2026-06-19", freq="D"))
    assert list(out["y"]) == [100.0, 200.0, 0.0, 300.0]


def test_load_daily_series_spark_honors_explicit_table():
    pdf = pd.DataFrame({"business_date": [20260616], "net_sales": [100.0]})
    spark = _FakeSpark(pdf)

    load_daily_series_spark(spark, table="cat.sch.custom_mart")

    assert "cat.sch.custom_mart" in spark.sql_queries[0]
