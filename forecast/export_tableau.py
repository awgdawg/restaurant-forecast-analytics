from __future__ import annotations

from pathlib import Path

import pandas as pd

COLUMNS = ["date", "net_sales_actual", "yhat", "yhat_lower", "yhat_upper", "model", "is_forecast"]


def build_forecast_vs_actuals(
    series: pd.DataFrame, forecast: pd.DataFrame, model_name: str
) -> pd.DataFrame:
    """Tidy history+forecast frame for the dashboard line chart."""
    hist = series.rename(columns={"ds": "date", "y": "net_sales_actual"}).copy()
    hist["yhat"] = pd.NA
    hist["yhat_lower"] = pd.NA
    hist["yhat_upper"] = pd.NA
    hist["is_forecast"] = False

    fc = forecast.rename(columns={"ds": "date"}).copy()
    fc["net_sales_actual"] = pd.NA
    fc["is_forecast"] = True

    out = pd.concat([hist, fc], ignore_index=True)
    out["model"] = model_name
    return out[COLUMNS]


def build_metrics_frame(metrics_by_model: dict) -> pd.DataFrame:
    """One row per model: model, mae, rmse, mape, wape (whatever keys are present)."""
    return pd.DataFrame([{"model": name, **vals} for name, vals in metrics_by_model.items()])


def write_exports(fva: pd.DataFrame, metrics: pd.DataFrame, out_dir: str = "exports") -> None:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    fva.to_csv(Path(out_dir) / "forecast_vs_actuals.csv", index=False)
    metrics.to_csv(Path(out_dir) / "backtest_metrics.csv", index=False)
