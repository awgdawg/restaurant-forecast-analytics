from __future__ import annotations

import numpy as np


def mae(y_true, y_pred) -> float:
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    return float(np.mean(np.abs(yt - yp)))


def rmse(y_true, y_pred) -> float:
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    return float(np.sqrt(np.mean((yt - yp) ** 2)))


def mape(y_true, y_pred) -> float:
    """Mean abs % error over days with non-zero actuals (closed days excluded)."""
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    mask = yt != 0
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs((yt[mask] - yp[mask]) / yt[mask])) * 100)


def wape(y_true, y_pred) -> float:
    """Weighted abs % error: sum|err| / sum|actual|. Robust to zero days."""
    yt, yp = np.asarray(y_true, float), np.asarray(y_pred, float)
    denom = np.sum(np.abs(yt))
    return float(np.sum(np.abs(yt - yp)) / denom * 100) if denom else float("nan")
