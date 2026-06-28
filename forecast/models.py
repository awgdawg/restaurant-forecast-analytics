from __future__ import annotations

import numpy as np
import pandas as pd


def seasonal_naive(train: pd.DataFrame, horizon: int, season_length: int = 7) -> pd.DataFrame:
    """Forecast = the last `season_length` observed values, tiled across the horizon.
    Future day T+1 reuses the value from T+1-season_length (same weekday last week)."""
    last_season = train["y"].to_numpy()[-season_length:]
    if len(last_season) < season_length:  # very short history: pad by repetition
        last_season = np.resize(last_season, season_length)
    last_ds = train["ds"].iloc[-1]
    future_ds = pd.date_range(last_ds + pd.Timedelta(days=1), periods=horizon, freq="D")
    yhat = np.resize(last_season, horizon)  # [s0..s6, s0..s6, ...]
    return pd.DataFrame({"ds": future_ds, "yhat": yhat.astype(float)})
