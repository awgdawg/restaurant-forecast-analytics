import pandas as pd
import pytest

prophet = pytest.importorskip("prophet")  # skip cleanly if install failed (Task 1)

from forecast.prophet_model import clip_nonnegative, prophet_forecast  # noqa: E402


def test_prophet_forecast_returns_horizon_rows_with_band():
    ds = pd.date_range("2024-01-01", periods=140, freq="D")
    y = [1000 + 200 * (d.weekday() >= 5) for d in ds]  # weekend bump
    train = pd.DataFrame({"ds": ds, "y": y})

    fc = prophet_forecast(train, horizon=14, yearly=False)

    assert list(fc.columns) == ["ds", "yhat", "yhat_lower", "yhat_upper"]
    assert len(fc) == 14
    assert list(fc["ds"]) == list(pd.date_range("2024-05-20", periods=14, freq="D"))


def test_clip_nonnegative_floors_forecast_at_zero():
    fc = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-27", periods=2, freq="D"),
            "yhat": [100.0, -5.0],
            "yhat_lower": [-50.0, -200.0],
            "yhat_upper": [300.0, 150.0],
        }
    )

    out = clip_nonnegative(fc)

    # sales can't be negative -> point + band floored at 0, positives untouched
    assert list(out["yhat"]) == [100.0, 0.0]
    assert list(out["yhat_lower"]) == [0.0, 0.0]
    assert list(out["yhat_upper"]) == [300.0, 150.0]
