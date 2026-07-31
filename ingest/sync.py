"""Extract-then-upload in one process: the cloud-side replacement for
scripts/morning_extract.ps1. Runs anywhere with open egress (Cloud Run).

Modes:
  rfa-sync --refresh-days 3   # daily self-healing window (ends yesterday)
  rfa-sync --today            # intraday: current business date only
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.extract import extract_range
from ingest.to_volume import DEFAULT_VOLUME_ROOT, upload_partitions
from ingest.toast_client import ToastClient

BUSINESS_TZ = ZoneInfo("America/Chicago")


def business_today(now_fn=None) -> date:
    """Current business date. The restaurant closes at 20:00 local, so the
    business date is today's date in America/Chicago -- no midnight rollover."""
    now = (now_fn or (lambda: datetime.now(BUSINESS_TZ)))()
    return now.date()


def sync_window(refresh_days: int, today_mode: bool, today: date) -> tuple[date, date]:
    """Inclusive [start, end] to extract and upload."""
    if today_mode:
        return today, today
    end = today - timedelta(days=1)
    return end - timedelta(days=refresh_days - 1), end


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Extract Toast orders and upload partitions to the UC Volume."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--refresh-days",
        type=int,
        default=0,
        help="re-pull the trailing N days ending yesterday (self-healing window)",
    )
    mode.add_argument(
        "--today", action="store_true", help="pull only the current business date"
    )
    parser.add_argument("--out-dir", default="data/raw/orders")
    parser.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    args = parser.parse_args(argv)
    if not args.today and args.refresh_days < 1:
        parser.error("--refresh-days must be >= 1")

    load_dotenv()
    start, end = sync_window(args.refresh_days, args.today, business_today())
    client = ToastClient(load_toast_config())
    out_dir = Path(args.out_dir)
    print(f"Syncing {start.isoformat()} -> {end.isoformat()}")
    rows = extract_range(client, start, end, out_dir, overwrite=True, log=print)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    uploaded = upload_partitions(out_dir, days, args.volume_root, log=print)

    # Empty-window guard: the restaurant closes only on Mondays, so a refresh
    # window (>= 2 days) can never legitimately yield zero partitions -- zero
    # means extraction is broken, not that nothing happened. The 2026-07
    # incident was a silent no-op rather than a red run, so fail loudly here.
    # --today is exempt: pre-open or a Monday legitimately uploads nothing.
    if not args.today and uploaded == 0:
        print(
            f"ERROR: no partitions uploaded for {start.isoformat()} -> "
            f"{end.isoformat()}; extraction produced nothing.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Synced {rows} rows across {uploaded} partitions to {args.volume_root}")


if __name__ == "__main__":
    main()
