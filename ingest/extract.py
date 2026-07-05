"""Pull a date range of Toast orders, flatten + strip PII, write Parquet.

Usage:
    python -m ingest.extract                       # backfill: auto-detect start -> yesterday
    python -m ingest.extract --start 2026-06-20 --end 2026-06-26
    python -m ingest.extract --start 2026-06-20 --end 2026-06-26 --overwrite
    python -m ingest.extract --refresh-days 3        # nightly: re-pull the last 3 days
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.orders import flatten_order
from ingest.toast_client import ToastClient


def _daterange(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _confirmed_empty(day_has_orders, day: date) -> bool:
    """True when `day` AND the day before it both have no orders.

    Requiring two consecutive empty days avoids mistaking a single weekly closed
    day for the restaurant's true start. (A multi-day holiday/closure gap can
    still fool it, which is why the detected start is logged for the user to
    sanity-check and override with --start if needed.)"""
    return not day_has_orders(day) and not day_has_orders(day - timedelta(days=1))


def find_earliest_business_date(
    day_has_orders, today: date, max_lookback_days: int = 900
) -> date:
    """Find a safe backfill start: the earliest business date with orders.

    Exponential search backward from `today` to bracket the start between a
    confirmed-empty day and a day that still has data, then binary-search the
    bracket down to the exact first day with orders. If data goes back further
    than max_lookback_days, returns that floor (over-shooting early is harmless —
    empty days are skipped cheaply at extract time)."""
    step = 1
    after = today  # most recent probe still known to have data
    before = None
    while step <= max_lookback_days:
        probe = today - timedelta(days=step)
        if _confirmed_empty(day_has_orders, probe):
            before = probe
            break
        after = probe
        step *= 2
    if before is None:
        return today - timedelta(days=max_lookback_days)

    # Invariant: `before` is pre-start (empty), `after` still has data.
    lo, hi = before, after
    while (hi - lo).days > 1:
        mid = lo + timedelta(days=(hi - lo).days // 2)
        if _confirmed_empty(day_has_orders, mid):
            lo = mid
        else:
            hi = mid
    return hi


def _day_has_orders(client, day: date) -> bool:
    """Cheap single-row probe: does this business date have any orders at all?
    Used by auto-detect so we never pull full pages just to test for existence."""
    batch = client.get(
        "/orders/v2/ordersBulk",
        {"businessDate": day.strftime("%Y%m%d"), "page": 1, "pageSize": 1},
    )
    return isinstance(batch, list) and len(batch) > 0


def extract_range(
    client,
    start: date,
    end: date,
    out_dir: Path,
    *,
    overwrite: bool = False,
    log=None,
) -> int:
    """Pull each business date in [start, end], write one Parquet file per
    non-empty day under out_dir/business_date=YYYYMMDD/. Returns rows written.

    Resumable: days whose Parquet already exists are skipped (no API call) unless
    overwrite=True, so an interrupted backfill can simply be re-run. Pass log to
    receive a per-day progress message."""
    emit = log or (lambda _msg: None)
    total = 0
    for day in _daterange(start, end):
        bd = day.strftime("%Y%m%d")
        part = out_dir / f"business_date={bd}"
        target = part / "orders.parquet"
        if target.exists() and not overwrite:
            emit(f"{bd}: skip (already on disk)")
            continue
        orders = client.get_paginated("/orders/v2/ordersBulk", {"businessDate": bd})
        rows = [flatten_order(o) for o in orders] if isinstance(orders, list) else []
        if not rows:
            emit(f"{bd}: 0 orders")
            continue
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(target, index=False)
        total += len(rows)
        emit(f"{bd}: {len(rows)} orders")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Toast orders to Parquet.")
    parser.add_argument(
        "--start", help="YYYY-MM-DD; if omitted, auto-detect earliest day with orders"
    )
    parser.add_argument("--end", help="YYYY-MM-DD; default = yesterday")
    parser.add_argument(
        "--overwrite", action="store_true", help="re-fetch days already on disk"
    )
    parser.add_argument(
        "--out-dir", default="data/raw/orders",
        help="output root (local path or /Volumes/... in-cloud)",
    )
    parser.add_argument(
        "--refresh-days", type=int, default=0,
        help="re-pull the last N days with overwrite (captures post-close edits); "
        "overrides --start and auto-detect",
    )
    args = parser.parse_args()
    if args.refresh_days < 0:
        parser.error("--refresh-days must be >= 0")

    load_dotenv()
    client = ToastClient(load_toast_config())
    out_dir = Path(args.out_dir)

    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    overwrite = args.overwrite or args.refresh_days > 0
    if args.refresh_days:
        start = end - timedelta(days=args.refresh_days - 1)
    elif args.start:
        start = date.fromisoformat(args.start)
    else:
        print("Auto-detecting earliest business date with orders...")
        start = find_earliest_business_date(
            lambda d: _day_has_orders(client, d), date.today()
        )
        print(f"Detected start: {start.isoformat()}")

    print(f"Extracting {start.isoformat()} -> {end.isoformat()} into {out_dir}/")
    total = extract_range(
        client, start, end, out_dir, overwrite=overwrite, log=print
    )
    print(f"Wrote {total} order rows under {out_dir}")


if __name__ == "__main__":
    main()
