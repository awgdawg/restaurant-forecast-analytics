with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    business_date,
    count(*)                         as order_count,
    sum(num_guests)                  as guest_count,
    round(sum(net_amount) - sum(deferred_amount), 2) as net_sales,
    round(sum(deferred_amount), 2)   as deferred_revenue,
    round(sum(total_amount), 2)      as total_sales,
    round(sum(tax_amount), 2)        as tax,
    round(sum(tip_amount), 2)        as tips
from orders
group by business_date
