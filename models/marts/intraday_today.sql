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
-- collision and is diverted to _rescued_data. Values agree, so the derived
-- one is authoritative -- we only widen it to BIGINT to match forecast_date.
--
-- deferred_amount needs the same rescue treatment for a subtler reason:
-- ingest.orders._deferred_amount returns int 0 when a day has no deferred
-- selections, so those days' parquet files carry INT64 where days with a real
-- gift-card sale carry DOUBLE. Inference picks DOUBLE, and every INT64 file
-- then fails the typed read and lands in _rescued_data as NULL -- silently
-- turning net sales into NULL. schemaHints cannot fix this (hinting either
-- type just moves the mismatch to the other half of the files; the direct
-- parquet.`path` reader errors outright with
-- PARQUET_COLUMN_DATA_TYPE_MISMATCH). Reading the typed column first and
-- falling back to the rescued JSON covers both file shapes: verified against
-- bronze_orders over all 792 shared dates, max abs diff 0.00. The fallback is
-- direction-agnostic on purpose -- it recovers the value whichever type
-- inference happens to pick, which stops mattering only in theory and starts
-- mattering once the partition count passes read_files' inference sample cap
-- (~1000 files, so around mid-2027) and the sampled majority can flip.
-- EXIT CONDITION: drop this coalesce only once ingest/orders.py writes
-- explicit-schema parquet AND the ~790 existing partitions have been
-- rewritten -- fixing the writer alone leaves every historical file INT64.
-- rescuedDataColumn is pinned below rather than left to its default, since
-- this expression load-bears on that column existing.
raw_orders as (
    select
        cast(business_date as bigint) as business_date,
        order_guid,
        opened_date,
        num_guests,
        net_amount,
        coalesce(
            deferred_amount,
            try_cast(get_json_object(_rescued_data, '$.deferred_amount') as double),
            0
        ) as deferred_amount,
        voided,
        deleted
    from read_files(
        '/Volumes/workspace/default/raw_orders/',
        format => 'parquet',
        rescuedDataColumn => '_rescued_data'
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
