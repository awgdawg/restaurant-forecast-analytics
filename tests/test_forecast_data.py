import pandas as pd

from forecast.data import clean_daily_series


def test_clean_fills_missing_days_with_zero_and_is_continuous():
    # 6/16 and 6/17 present, 6/18 missing (closed), 6/19 present
    raw = pd.DataFrame(
        {"business_date": [20260616, 20260617, 20260619], "net_sales": [100.0, 200.0, 300.0]}
    )

    out = clean_daily_series(raw)

    assert list(out.columns) == ["ds", "y"]
    assert str(out["ds"].dtype) == "datetime64[ns]"
    # continuous 6/16..6/19 = 4 rows, the gap day filled with 0.0
    assert list(out["ds"]) == list(pd.date_range("2026-06-16", "2026-06-19", freq="D"))
    assert list(out["y"]) == [100.0, 200.0, 0.0, 300.0]
