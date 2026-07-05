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
)

select
    coalesce(a.business_date, f.business_date)                as business_date,
    to_date(
        cast(coalesce(a.business_date, f.business_date) as string), 'yyyyMMdd'
    )                                                         as date_day,
    a.net_sales                                               as net_sales_actual,
    f.yhat,
    f.yhat_lower,
    f.yhat_upper,
    f.model,
    f.run_ts,
    (a.business_date is null and f.business_date is not null) as is_forecast
from actuals a
full outer join forecast f
    on a.business_date = f.business_date
