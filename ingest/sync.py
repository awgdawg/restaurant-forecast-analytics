"""Extract-then-upload in one process: the cloud-side replacement for
scripts/morning_extract.ps1. Runs anywhere with open egress (Cloud Run).

Modes:
  rfa-sync --refresh-days 3                  # daily self-healing window (ends yesterday)
  rfa-sync --refresh-days 3 --through-today  # evening run: window ends today (post-close)
  rfa-sync --today                           # intraday: current business date only
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


def sync_window(
    refresh_days: int, today_mode: bool, today: date, through_today: bool = False
) -> tuple[date, date]:
    """Inclusive [start, end] to extract and upload.

    through_today shifts the refresh window to end today instead of yesterday
    (same day-count; the whole window moves a day forward, so each business
    date's FINAL re-pull happens ~12h earlier than in the morning regime --
    pick N with that in mind) -- for post-close evening runs, where the day
    just finished is the one that most needs pulling.
    """
    if today_mode:
        return today, today
    end = today if through_today else today - timedelta(days=1)
    return end - timedelta(days=refresh_days - 1), end


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Extract Toast orders and upload partitions to the UC Volume."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    # default MUST stay None: argparse counts an option whose value IS the
    # action default as "not seen" when enforcing a required mutually-exclusive
    # group, so default=0 makes an explicit `--refresh-days 0` die with the
    # generic group message and renders the ">= 1" check below unreachable.
    mode.add_argument(
        "--refresh-days",
        type=int,
        default=None,
        help="re-pull the trailing N days ending yesterday (self-healing window)",
    )
    mode.add_argument(
        "--today", action="store_true", help="pull only the current business date"
    )
    parser.add_argument(
        "--through-today",
        action="store_true",
        help="end the refresh window today instead of yesterday (post-close evening runs)",
    )
    parser.add_argument("--out-dir", default="data/raw/orders")
    parser.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    args = parser.parse_args(argv)
    if not args.today and (args.refresh_days is None or args.refresh_days < 1):
        parser.error("--refresh-days must be >= 1")
    if args.through_today and args.today:
        parser.error("--through-today cannot be combined with --today")

    load_dotenv()
    start, end = sync_window(
        args.refresh_days or 0, args.today, business_today(), args.through_today
    )
    client = ToastClient(load_toast_config())
    out_dir = Path(args.out_dir)
    print(f"Syncing {start.isoformat()} -> {end.isoformat()}")
    rows = extract_range(client, start, end, out_dir, overwrite=True, log=print)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    uploaded = upload_partitions(out_dir, days, args.volume_root, log=print)

    # Empty-window guard: the restaurant closes only on Mondays, so a refresh
    # window (>= 2 days) can never legitimately be empty -- an empty result
    # means extraction is broken, not that nothing happened. The 2026-07
    # incident was a silent no-op rather than a red run, so fail loudly here.
    # BOTH conditions are checked: uploaded == 0 catches a cold working dir,
    # and rows == 0 catches the warm one, where a total extraction failure
    # leaves stale partitions from earlier runs on disk (extract_range skips
    # zero-order days without deleting them) that get re-uploaded and would
    # otherwise report success.
    # --today is exempt: pre-open or a Monday legitimately yields nothing.
    if not args.today and (uploaded == 0 or rows == 0):
        print(
            f"ERROR: refresh window {start.isoformat()} -> {end.isoformat()} "
            f"produced no data (rows={rows}, uploaded={uploaded}, "
            f"out_dir={out_dir}, volume_root={args.volume_root})",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Synced {rows} rows across {uploaded} partitions to {args.volume_root}")


if __name__ == "__main__":
    main()
