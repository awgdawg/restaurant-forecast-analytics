"""Pull a date range of Toast orders, flatten + strip PII, write Parquet.

Usage:
    python -m ingest.extract --start 2026-06-20 --end 2026-06-26
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


def extract_range(client, start: date, end: date, out_dir: Path) -> int:
    """Pull each business date in [start, end], write one Parquet file per
    non-empty day under out_dir/business_date=YYYYMMDD/. Returns total rows."""
    total = 0
    for day in _daterange(start, end):
        bd = day.strftime("%Y%m%d")
        orders = client.get_paginated("/orders/v2/ordersBulk", {"businessDate": bd})
        rows = [flatten_order(o) for o in orders] if isinstance(orders, list) else []
        if not rows:
            continue
        part = out_dir / f"business_date={bd}"
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(part / "orders.parquet", index=False)
        total += len(rows)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Toast orders to Parquet.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    load_dotenv()
    client = ToastClient(load_toast_config())
    out_dir = Path("data/raw/orders")
    total = extract_range(
        client, date.fromisoformat(args.start), date.fromisoformat(args.end), out_dir
    )
    print(f"Wrote {total} order rows under {out_dir}")


if __name__ == "__main__":
    main()
