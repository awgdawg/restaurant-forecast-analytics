import pandas as pd

from forecast.export_tableau import build_forecast_vs_actuals, build_metrics_frame


def test_build_forecast_vs_actuals_stacks_history_then_forecast():
    series = pd.DataFrame({"ds": pd.date_range("2026-06-01", periods=3, freq="D"), "y": [10.0, 20.0, 30.0]})
    forecast = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-04", periods=2, freq="D"),
            "yhat": [40.0, 50.0],
            "yhat_lower": [35.0, 45.0],
            "yhat_upper": [45.0, 55.0],
        }
    )

    out = build_forecast_vs_actuals(series, forecast, model_name="prophet")

    assert list(out.columns) == [
        "date", "net_sales_actual", "yhat", "yhat_lower", "yhat_upper", "model", "is_forecast",
    ]
    assert len(out) == 5  # 3 history + 2 forecast
    assert out["is_forecast"].tolist() == [False, False, False, True, True]
    assert out.loc[0, "net_sales_actual"] == 10.0 and pd.isna(out.loc[0, "yhat"])
    assert out.loc[4, "yhat"] == 50.0 and pd.isna(out.loc[4, "net_sales_actual"])


def test_build_metrics_frame_one_row_per_model():
    metrics = {"baseline": {"mae": 500.0, "wape": 20.0}, "prophet": {"mae": 400.0, "wape": 15.0}}

    out = build_metrics_frame(metrics)

    assert set(out["model"]) == {"baseline", "prophet"}
    assert out.loc[out["model"] == "prophet", "wape"].iloc[0] == 15.0
