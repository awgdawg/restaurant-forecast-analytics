from datetime import datetime, timezone

import pandas as pd

from load.load_forecast import (
    BACKTEST_COLUMNS,
    FORECAST_COLUMNS,
    METRICS_COLUMNS,
    backtest_ddl,
    backtest_rows,
    forecast_ddl,
    forecast_rows,
    history_ddl,
    metrics_ddl,
    metrics_rows,
    write_backtest_predictions,
    write_forecast,
    write_forecast_history,
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
    assert any(
        s.startswith("CREATE TABLE IF NOT EXISTS forecast_daily_sales") for s in sqls
    )
    assert "DELETE FROM forecast_daily_sales" in sqls  # overwrite semantics
    insert_sql, params = cur.calls[-1]
    assert insert_sql.count("?") == 2 * len(FORECAST_COLUMNS)
    assert params[0] == 20260629 and params[1] == 100.0


def test_history_ddl_lists_forecast_columns_and_table():
    hddl = history_ddl()
    for col in FORECAST_COLUMNS:  # same columns as forecast_daily_sales
        assert col in hddl
    assert "CREATE TABLE IF NOT EXISTS forecast_history" in hddl
    assert "USING DELTA" in hddl


def test_write_forecast_history_appends_without_delete():
    fc = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-29", periods=2, freq="D"),
            "yhat": [100.0, 200.0],
            "yhat_lower": [90.0, 190.0],
            "yhat_upper": [110.0, 210.0],
        }
    )
    cur = FakeCursor()

    n = write_forecast_history(cur, fc, model="prophet", run_ts=RUN_TS)

    assert n == 2
    sqls = [s for s, _ in cur.calls]
    assert any(
        s.startswith("CREATE TABLE IF NOT EXISTS forecast_history") for s in sqls
    )
    assert not any(s.startswith("DELETE") for s in sqls)  # append-only archive
    insert_sql, params = cur.calls[-1]
    assert insert_sql.startswith("INSERT INTO forecast_history")
    assert insert_sql.count("?") == 2 * len(FORECAST_COLUMNS)
    # rows come from the same pure forecast_rows() builder as forecast_daily_sales
    expected = forecast_rows(fc, "prophet", RUN_TS)
    assert params == [v for row in expected for v in row]


def test_write_forecast_history_no_op_on_empty_input():
    cur = FakeCursor()

    n = write_forecast_history(
        cur, pd.DataFrame(columns=["ds", "yhat"]), "prophet", RUN_TS
    )

    assert n == 0
    # DDL may run, but no INSERT (and no malformed 'VALUES ' SQL) is issued
    assert not any(s.startswith("INSERT") for s, _ in cur.calls)


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


# --- backtest_predictions -----------------------------------------------------


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


def test_backtest_ddl_lists_columns_and_table():
    bddl = backtest_ddl()
    for col in BACKTEST_COLUMNS:
        assert col in bddl
    assert "CREATE TABLE IF NOT EXISTS backtest_predictions" in bddl
    assert "USING DELTA" in bddl
    assert "fold INT" in bddl  # fold is INT, not BIGINT (mirrors metrics INT trap)


def test_backtest_rows_convert_ds_and_fold_int():
    bt = _bt([100.0, 200.0], fold=3)

    rows = backtest_rows(bt, model="prophet", run_ts=RUN_TS)

    # BACKTEST_COLUMNS order: forecast_date, yhat, model, fold, run_ts
    assert rows == [
        (20260301, 100.0, "prophet", 3, RUN_TS),
        (20260302, 200.0, "prophet", 3, RUN_TS),
    ]
    # ds converted to yyyymmdd int, fold coerced to a plain int
    assert isinstance(rows[0][0], int)
    assert isinstance(rows[0][3], int)


def test_write_backtest_predictions_deletes_once_across_both_models():
    """The sharp edge: one DELETE per RUN (not per model), and it must precede
    every INSERT -- otherwise the second model's write would wipe the first."""
    bt_by_model = {
        "baseline": _bt([10.0, 20.0]),
        "prophet": _bt([11.0, 21.0]),
    }
    cur = FakeCursor()

    n = write_backtest_predictions(cur, bt_by_model, run_ts=RUN_TS)

    assert n == 4  # 2 models x 2 rows
    sqls = [s for s, _ in cur.calls]
    delete_idx = [i for i, s in enumerate(sqls) if s.startswith("DELETE")]
    insert_idx = [i for i, s in enumerate(sqls) if s.startswith("INSERT")]
    assert len(delete_idx) == 1  # exactly ONE delete for the whole run
    assert "DELETE FROM backtest_predictions" in sqls
    assert len(insert_idx) == 2  # one insert per model
    assert delete_idx[0] < min(insert_idx)  # delete before any insert
    # both models' rows land, each tagged with its own model name
    all_params = [p for _, p in cur.calls if p]
    models_written = {p[2] for params in all_params for p in [params[:5], params[5:10]]}
    assert models_written == {"baseline", "prophet"}


def test_write_backtest_predictions_no_op_on_empty_input():
    cur = FakeCursor()

    n = write_backtest_predictions(
        cur, {"baseline": pd.DataFrame(columns=["ds", "y", "yhat", "fold"])}, RUN_TS
    )

    assert n == 0
    # DDL/DELETE may run, but no INSERT (no malformed 'VALUES ' SQL) is issued
    assert not any(s.startswith("INSERT") for s, _ in cur.calls)
