import math

from forecast.metrics import mae, mape, rmse, wape


def test_mae_and_rmse_on_known_values():
    y = [10.0, 20.0, 30.0]
    yhat = [12.0, 18.0, 33.0]  # errors 2, 2, 3
    assert mae(y, yhat) == (2 + 2 + 3) / 3
    assert rmse(y, yhat) == math.sqrt((4 + 4 + 9) / 3)


def test_mape_skips_zero_actual_days():
    y = [0.0, 100.0, 200.0]  # closed day first
    yhat = [50.0, 110.0, 180.0]  # 10% then 10% on the non-zero days
    assert mape(y, yhat) == 10.0  # the 0-actual day is excluded, not inf


def test_wape_is_total_abs_error_over_total_actual():
    y = [0.0, 100.0, 200.0]
    yhat = [50.0, 110.0, 180.0]  # abs errors 50,10,20 = 80; total actual 300
    assert wape(y, yhat) == 80 / 300 * 100
