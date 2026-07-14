{{ config(materialized='view') }}

with actuals as (
    select business_date, net_sales
    from {{ ref('fct_daily_sales') }}
),

forecast as (
    select
        forecast_date as business_date,
        yhat,
        yhat_lower,
        yhat_upper,
        model,
        run_ts
    from {{ source('forecast', 'forecast_daily_sales') }}
),

-- forecast_daily_sales is latest-only and holds exactly ONE model's rows (the
-- current winner), so max(model) collapses that single distinct value to a
-- scalar. We use it to filter backtest_predictions (which stores BOTH models)
-- down to the winner. That leaves one backtest prediction per date, so the
-- join below can't fan out -- given two assumptions: the winner filter here
-- removes cross-model duplication, and one-prediction-per-date-per-model
-- holds upstream because run_forecast pins step=horizon (non-overlapping
-- backtest folds). The dbt unique test on business_date is the backstop if
-- either ever breaks. Deriving the winner (vs hardcoding 'prophet') keeps
-- the view honest if the baseline ever wins.
winner as (
    select max(model) as model
    from {{ source('forecast', 'forecast_daily_sales') }}
),

-- Out-of-sample backtest yhat for the trailing ~112 days, one row per past
-- date (winner only). No intervals: backtest carries no bands.
backtest as (
    select b.forecast_date as business_date, b.yhat
    from {{ source('forecast', 'backtest_predictions') }} b
    inner join winner w on b.model = w.model
)

select
    coalesce(a.business_date, f.business_date)                as business_date,
    to_date(
        cast(coalesce(a.business_date, f.business_date) as string), 'yyyyMMdd'
    )                                                         as date_day,
    a.net_sales                                               as net_sales_actual,
    -- future rows keep the live forecast yhat; past rows fall back to the
    -- out-of-sample backtest yhat (the honest historical forecast line).
    coalesce(f.yhat, b.yhat)                                  as yhat,
    f.yhat_lower,
    f.yhat_upper,
    f.model,
    f.run_ts,
    (a.business_date is null and f.business_date is not null) as is_forecast,
    case
        when f.yhat is not null then 'forecast'
        when b.yhat is not null then 'backtest'
    end                                                       as yhat_source
from actuals a
full outer join forecast f
    on a.business_date = f.business_date
-- Enrichment only: LEFT JOIN onto the already-combined actuals x forecast rows
-- so backtest can add a yhat to existing past dates but never create new rows
-- (backtest dates for closed days simply don't match and are dropped).
left join backtest b
    on coalesce(a.business_date, f.business_date) = b.business_date
