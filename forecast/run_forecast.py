"""Backtest baseline vs Prophet on fct_daily_sales, forecast 14 days, export CSVs.

Usage:  python -m forecast.run_forecast [--horizon 14] [--folds 8]
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv

from forecast.backtest import rolling_origin_backtest, summarize
from forecast.data import load_daily_series, load_daily_series_spark
from forecast.export_tableau import (
    build_forecast_vs_actuals,
    build_metrics_frame,
    write_exports,
)
from forecast.models import seasonal_naive
from forecast.prophet_model import prophet_forecast
from load.databricks import connect
from load.load_forecast import (
    write_backtest_predictions,
    write_forecast,
    write_forecast_history,
    write_metrics,
)
from load.spark_io import (
    active_spark,
    spark_write_backtest_predictions,
    spark_write_forecast,
    spark_write_forecast_history,
    spark_write_metrics,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Forecast daily net sales.")
    parser.add_argument("--horizon", type=int, default=14)
    parser.add_argument("--folds", type=int, default=8)
    args = parser.parse_args()

    load_dotenv()

    # Detect the cloud seam once. On serverless job compute an active Spark
    # session exists, so reads and writes go through it (the egress allowlist
    # blocks the SQL connector) and no CSVs are written. Locally there is no
    # pyspark, active_spark() is None, and behaviour is byte-identical to before.
    spark = active_spark()

    if spark is not None:
        series = load_daily_series_spark(spark)
    else:
        conn = connect()
        try:
            series = load_daily_series(conn)
        finally:
            conn.close()
    print(
        f"Loaded {len(series)} days: {series['ds'].min().date()} -> {series['ds'].max().date()}"
    )

    models = {
        "baseline": seasonal_naive,
        "prophet": lambda tr, h: prophet_forecast(tr, h),
    }
    metrics = {}
    backtests = {}
    for name, fn in models.items():
        bt = rolling_origin_backtest(
            series, fn, horizon=args.horizon, n_folds=args.folds
        )
        backtests[name] = bt
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
    # CSV exports are a local-only convenience (feeding Tableau Public). In-cloud
    # there is no writable local filesystem to publish from, so skip them.
    if spark is None:
        fva = build_forecast_vs_actuals(series, forecast, model_name=winner)
        write_exports(fva, build_metrics_frame(metrics))
        print(
            f"Wrote exports/forecast_vs_actuals.csv ({len(fva)} rows) "
            "+ exports/backtest_metrics.csv"
        )

    run_ts = datetime.now(timezone.utc)
    if spark is not None:
        n_fc = spark_write_forecast(spark, forecast, winner, run_ts)
        n_m = spark_write_metrics(spark, metrics, args.horizon, args.folds, run_ts)
        n_h = spark_write_forecast_history(spark, forecast, winner, run_ts)
        n_bt = spark_write_backtest_predictions(spark, backtests, run_ts)
    else:
        conn = connect()
        try:
            cur = conn.cursor()
            n_fc = write_forecast(cur, forecast, winner, run_ts)
            n_m = write_metrics(cur, metrics, args.horizon, args.folds, run_ts)
            n_h = write_forecast_history(cur, forecast, winner, run_ts)
            n_bt = write_backtest_predictions(cur, backtests, run_ts)
            cur.close()
        finally:
            conn.close()
    print(
        f"Wrote {n_fc} rows to forecast_daily_sales, {n_m} rows to model_metrics, "
        f"{n_h} rows to forecast_history, {n_bt} rows to backtest_predictions"
    )


if __name__ == "__main__":
    main()
