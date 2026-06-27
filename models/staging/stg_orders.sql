with source as (
    select * from {{ source('raw', 'bronze_orders') }}
)

select
    cast(business_date as int)        as business_date,
    order_guid,
    cast(opened_date as timestamp)    as opened_at,
    source                            as order_source,
    dining_option_guid,
    num_guests,
    num_checks,
    net_amount,
    total_amount,
    tax_amount,
    tip_amount
from source
where not voided and not deleted
