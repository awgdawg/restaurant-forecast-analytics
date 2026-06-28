import pandas as pd

from forecast.backtest import rolling_origin_backtest, summarize
from forecast.models import seasonal_naive


def _weekly_series(n):
    ds = pd.date_range("2026-01-01", periods=n, freq="D")
    return pd.DataFrame({"ds": ds, "y": [float(d.weekday()) for d in ds]})


def test_backtest_aligns_actuals_to_predictions_across_folds():
    series = _weekly_series(100)

    bt = rolling_origin_backtest(series, seasonal_naive, horizon=14, n_folds=3, step=14, min_train=30)

    assert set(bt.columns) == {"ds", "y", "yhat", "fold"}
    assert bt["fold"].nunique() == 3
    assert len(bt) == 3 * 14
    # pure weekly pattern => seasonal-naive is exact => zero error everywhere
    assert (bt["y"] == bt["yhat"]).all()


def test_summarize_returns_all_metrics():
    series = _weekly_series(100)
    bt = rolling_origin_backtest(series, seasonal_naive, horizon=14, n_folds=3, step=14, min_train=30)

    m = summarize(bt)

    assert set(m) == {"mae", "rmse", "mape", "wape"}
    assert m["mae"] == 0.0  # exact on the synthetic series
