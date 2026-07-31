{{ config(materialized='view') }}

-- Fast-lane intraday view (Phase 2 of the 2026-07-30 cloud-extraction spec).
-- Reads today's raw partition STRAIGHT off the Volume that the Cloud Run
-- poller refreshes every 15 minutes -- freshness comes from the upload
-- cadence, with no Databricks job run in between. Bronze and the marts stay
-- nightly-authoritative; nothing downstream reads this view.
-- Anchored on a constant "today" row and LEFT JOINed both ways, so it
-- returns EXACTLY ONE ROW even pre-open, on closed Mondays, or if last
-- night's forecast is missing (zeros/nulls, never an empty result when the
-- Volume is readable -- an unreadable root errors loudly, as it should).

with today as (
    select cast(
        date_format(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 'yyyyMMdd')
        as bigint
    ) as business_date
),

-- read_files over the partitioned root infers ONE business_date (from the
-- directory name, as INT); the copy inside each parquet file loses the
-- collision and is diverted to the rescued-data column (verified live
-- 2026-07-31: still true after the schema fix described below). Values
-- agree, so the derived one is authoritative -- we only widen it to BIGINT
-- to match forecast_date.
--
-- HISTORY (2026-07-31): deferred_amount used to need a _rescued_data
-- coalesce here -- inference-typed writes (pre-fix) made it INT64 on
-- zero-deferred days and DOUBLE elsewhere, so the typed read rescue-NULLed
-- 718 of 796 days' files (schemaHints and the direct parquet.`path` reader
-- were both dead ends -- see git history of this file for the full
-- mechanism). Fixed at the writer (explicit ORDERS_SCHEMA in
-- ingest/orders.py, applied at the write site in ingest/extract.py) and the
-- whole archive was rewritten + re-uploaded the same day; verified zero
-- rescued values across all data columns. If net_sales_so_far ever
-- collapses to 0 while order_count stays non-zero, deferred_amount is
-- rescue-NULLing again (sum() returns NULL over the all-NULL day and the
-- outer coalesce hides it as 0) -- look for a writer bypassing
-- ORDERS_SCHEMA before touching this view.
raw_orders as (
    select
        cast(business_date as bigint) as business_date,
        order_guid,
        opened_date,
        num_guests,
        net_amount,
        deferred_amount,
        voided,
        deleted
    from read_files(
        '/Volumes/workspace/default/raw_orders/',
        format => 'parquet'
    )
),

-- Live-order filter and net-sales math mirror stg_orders / fct_daily_sales.
-- opened_date is stored UTC; it is converted to Chicago local here so the
-- timestamp agrees with the Chicago-local business_date the view is keyed on.
today_orders as (
    select
        o.business_date,
        count(*)                                             as order_count,
        sum(o.num_guests)                                    as guest_count,
        round(sum(o.net_amount) - sum(o.deferred_amount), 2) as net_sales_so_far,
        max(from_utc_timestamp(
            cast(o.opened_date as timestamp), 'America/Chicago'
        ))                                                   as last_order_opened_at
    from raw_orders o
    join today t on o.business_date = t.business_date
    where not o.voided and not o.deleted
    group by o.business_date
),

forecast as (
    select forecast_date as business_date, yhat, yhat_lower, yhat_upper
    from {{ source('forecast', 'forecast_daily_sales') }}
)

select
    t.business_date,
    coalesce(o.order_count, 0)                        as order_count,
    coalesce(o.guest_count, 0)                        as guest_count,
    coalesce(o.net_sales_so_far, 0)                   as net_sales_so_far,
    o.last_order_opened_at,
    f.yhat                                            as forecast_total,
    f.yhat_lower,
    f.yhat_upper,
    case
        when f.yhat > 0
        then round(coalesce(o.net_sales_so_far, 0) / f.yhat * 100, 1)
    end                                               as pct_of_forecast
from today t
left join today_orders o on t.business_date = o.business_date
left join forecast f on t.business_date = f.business_date
