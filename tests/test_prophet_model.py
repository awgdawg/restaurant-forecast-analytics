import pandas as pd
import pytest

prophet = pytest.importorskip("prophet")  # skip cleanly if install failed (Task 1)

from forecast.prophet_model import prophet_forecast  # noqa: E402


def test_prophet_forecast_returns_horizon_rows_with_band():
    ds = pd.date_range("2024-01-01", periods=140, freq="D")
    y = [1000 + 200 * (d.weekday() >= 5) for d in ds]  # weekend bump
    train = pd.DataFrame({"ds": ds, "y": y})

    fc = prophet_forecast(train, horizon=14, yearly=False)

    assert list(fc.columns) == ["ds", "yhat", "yhat_lower", "yhat_upper"]
    assert len(fc) == 14
    assert list(fc["ds"]) == list(pd.date_range("2024-05-20", periods=14, freq="D"))
