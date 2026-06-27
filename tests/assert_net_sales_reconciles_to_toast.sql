-- fails for any day where our net_sales differs from Toast's reported net sales by > 1 cent
with ours as (
    select business_date, net_sales from {{ ref('fct_daily_sales') }}
),

toast as (
    select business_date, net_sales as toast_net_sales
    from {{ ref('toast_sales_summary') }}
)

select
    ours.business_date,
    ours.net_sales,
    toast.toast_net_sales,
    abs(ours.net_sales - toast.toast_net_sales) as diff
from ours
join toast using (business_date)
where abs(ours.net_sales - toast.toast_net_sales) > 0.01
