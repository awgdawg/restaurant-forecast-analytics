import pandas as pd

from forecast.models import seasonal_naive


def test_seasonal_naive_repeats_last_week_by_weekday():
    ds = pd.date_range("2026-01-01", periods=21, freq="D")
    y = [float(d.weekday()) for d in ds]  # value == weekday, a pure weekly pattern
    train = pd.DataFrame({"ds": ds, "y": y})

    fc = seasonal_naive(train, horizon=14)

    assert list(fc.columns) == ["ds", "yhat"]
    assert len(fc) == 14
    # forecast dates immediately follow the last train day
    assert list(fc["ds"]) == list(pd.date_range("2026-01-22", periods=14, freq="D"))
    # each future day's yhat == that day's weekday (i.e. same weekday last week)
    assert list(fc["yhat"]) == [float(d.weekday()) for d in fc["ds"]]
