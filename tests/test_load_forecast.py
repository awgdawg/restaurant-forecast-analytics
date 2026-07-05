from datetime import datetime, timezone

import pandas as pd

from load.load_forecast import (
    FORECAST_COLUMNS,
    METRICS_COLUMNS,
    forecast_ddl,
    forecast_rows,
    metrics_ddl,
    metrics_rows,
    write_forecast,
    write_metrics,
)

RUN_TS = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


def test_ddls_list_every_column():
    fddl = forecast_ddl()
    for col in FORECAST_COLUMNS:
        assert col in fddl
    assert "USING DELTA" in fddl
    mddl = metrics_ddl()
    for col in METRICS_COLUMNS:
        assert col in mddl


def test_forecast_rows_convert_ds_and_null_missing_band():
    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=2, freq="D"), "yhat": [100.0, 200.0]}
    )  # baseline shape: no band columns

    rows = forecast_rows(fc, model="baseline", run_ts=RUN_TS)

    assert rows == [
        (20260629, 100.0, None, None, "baseline", RUN_TS),
        (20260630, 200.0, None, None, "baseline", RUN_TS),
    ]


def test_metrics_rows_one_per_model_in_column_order():
    metrics = {
        "baseline": {"mae": 434.0, "rmse": 605.0, "mape": 18.0, "wape": 16.6},
        "prophet": {"mae": 351.0, "rmse": 456.0, "mape": 31.6, "wape": 13.4},
    }

    rows = metrics_rows(metrics, horizon=14, n_folds=8, run_ts=RUN_TS)

    assert ("prophet", 351.0, 456.0, 31.6, 13.4, 14, 8, RUN_TS) in rows
    assert len(rows) == 2


def test_write_forecast_overwrites_then_inserts():
    fc = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-29", periods=2, freq="D"),
            "yhat": [100.0, 200.0],
            "yhat_lower": [90.0, 190.0],
            "yhat_upper": [110.0, 210.0],
        }
    )
    cur = FakeCursor()

    n = write_forecast(cur, fc, model="prophet", run_ts=RUN_TS)

    assert n == 2
    sqls = [s for s, _ in cur.calls]
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS forecast_daily_sales") for s in sqls)
    assert "DELETE FROM forecast_daily_sales" in sqls  # overwrite semantics
    insert_sql, params = cur.calls[-1]
    assert insert_sql.count("?") == 2 * len(FORECAST_COLUMNS)
    assert params[0] == 20260629 and params[1] == 100.0


def test_write_metrics_appends_without_delete():
    cur = FakeCursor()

    n = write_metrics(
        cur,
        {"prophet": {"mae": 1.0, "rmse": 2.0, "mape": 3.0, "wape": 4.0}},
        horizon=14,
        n_folds=8,
        run_ts=RUN_TS,
    )

    assert n == 1
    sqls = [s for s, _ in cur.calls]
    assert not any(s.startswith("DELETE") for s in sqls)  # append-only
    assert cur.calls[-1][0].count("?") == len(METRICS_COLUMNS)


def test_writers_no_op_on_empty_input():
    cur = FakeCursor()

    n_fc = write_forecast(cur, pd.DataFrame(columns=["ds", "yhat"]), "prophet", RUN_TS)
    n_m = write_metrics(cur, {}, horizon=14, n_folds=8, run_ts=RUN_TS)

    assert n_fc == 0 and n_m == 0
    # DDL may run, but no INSERT (and no malformed 'VALUES ' SQL) is issued
    assert not any(s.startswith("INSERT") for s, _ in cur.calls)
