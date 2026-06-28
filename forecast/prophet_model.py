from __future__ import annotations

import logging

import pandas as pd
from prophet import Prophet

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)  # quiet the Stan logs


def prophet_forecast(train: pd.DataFrame, horizon: int, *, yearly: bool = True) -> pd.DataFrame:
    """Fit Prophet (multiplicative weekly + optional yearly + US holidays) and
    return the `horizon` days after the last train date with a prediction band."""
    m = Prophet(
        weekly_seasonality=True,
        yearly_seasonality=yearly,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
    )
    m.add_country_holidays(country_name="US")
    m.fit(train[["ds", "y"]])
    future = m.make_future_dataframe(periods=horizon, freq="D")
    fc = m.predict(future)
    return fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon).reset_index(drop=True)
