"""Backtest baseline vs Prophet on fct_daily_sales, forecast 14 days, export CSVs.

Usage:  python -m forecast.run_forecast [--horizon 14] [--folds 8]
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from forecast.backtest import rolling_origin_backtest, summarize
from forecast.data import load_daily_series
from forecast.export_tableau import build_forecast_vs_actuals, build_metrics_frame, write_exports
from forecast.models import seasonal_naive
from forecast.prophet_model import prophet_forecast
from load.databricks import connect


def main() -> None:
    parser = argparse.ArgumentParser(description="Forecast daily net sales.")
    parser.add_argument("--horizon", type=int, default=14)
    parser.add_argument("--folds", type=int, default=8)
    args = parser.parse_args()

    load_dotenv()
    conn = connect()
    try:
        series = load_daily_series(conn)
    finally:
        conn.close()
    print(f"Loaded {len(series)} days: {series['ds'].min().date()} -> {series['ds'].max().date()}")

    models = {"baseline": seasonal_naive, "prophet": lambda tr, h: prophet_forecast(tr, h)}
    metrics = {}
    for name, fn in models.items():
        bt = rolling_origin_backtest(series, fn, horizon=args.horizon, n_folds=args.folds)
        metrics[name] = summarize(bt)
        m = metrics[name]
        print(
            f"{name:9s}  WAPE={m['wape']:.2f}%  MAPE={m['mape']:.2f}%  "
            f"MAE=${m['mae']:.0f}  RMSE=${m['rmse']:.0f}"
        )

    winner = min(metrics, key=lambda k: metrics[k]["wape"])
    print(f"WINNER: {winner} (lowest WAPE)")
    if metrics["prophet"]["wape"] < metrics["baseline"]["wape"]:
        print("Prophet BEATS the seasonal-naive baseline.")
    else:
        print("Prophet does NOT beat baseline yet -- see Task 8 tuning levers.")

    forecast = (
        prophet_forecast(series, args.horizon)
        if winner == "prophet"
        else seasonal_naive(series, args.horizon)
    )
    fva = build_forecast_vs_actuals(series, forecast, model_name=winner)
    write_exports(fva, build_metrics_frame(metrics))
    print(f"Wrote exports/forecast_vs_actuals.csv ({len(fva)} rows) + exports/backtest_metrics.csv")


if __name__ == "__main__":
    main()
