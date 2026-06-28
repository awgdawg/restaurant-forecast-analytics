from __future__ import annotations

import pandas as pd

from forecast.metrics import mae, mape, rmse, wape


def rolling_origin_backtest(
    series: pd.DataFrame,
    model_fn,
    horizon: int = 14,
    n_folds: int = 8,
    step: int = 14,
    min_train: int = 60,
) -> pd.DataFrame:
    """Expanding-window backtest. Returns long df[ds, y, yhat, fold]. The last fold
    ends at the series end; earlier folds step back `step` days each."""
    s = series.sort_values("ds").reset_index(drop=True)
    last_cutoff = len(s) - horizon
    frames = []
    for k in range(n_folds):
        cutoff = last_cutoff - (n_folds - 1 - k) * step
        if cutoff < min_train:
            continue
        train = s.iloc[:cutoff].copy()
        actual = s.iloc[cutoff : cutoff + horizon][["ds", "y"]].reset_index(drop=True)
        fc = model_fn(train, horizon)[["ds", "yhat"]].reset_index(drop=True)
        merged = actual.merge(fc, on="ds", how="left")
        merged["fold"] = k
        frames.append(merged)
    if not frames:
        raise ValueError("series too short for the given horizon/min_train/folds")
    return pd.concat(frames, ignore_index=True)


def summarize(bt: pd.DataFrame) -> dict:
    """Aggregate metrics across every backtest fold."""
    return {
        "mae": mae(bt["y"], bt["yhat"]),
        "rmse": rmse(bt["y"], bt["yhat"]),
        "mape": mape(bt["y"], bt["yhat"]),
        "wape": wape(bt["y"], bt["yhat"]),
    }
