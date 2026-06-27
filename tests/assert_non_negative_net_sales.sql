-- fails if any day has negative net sales
select business_date, net_sales
from {{ ref('fct_daily_sales') }}
where net_sales < 0
