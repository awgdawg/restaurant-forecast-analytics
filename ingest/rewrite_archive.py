"""One-off: rewrite legacy inference-typed order partitions to ORDERS_SCHEMA.

Historical context (2026-07-31): pandas.to_parquet inference wrote INT64 money
columns on integral-sum days and DOUBLE elsewhere, so typed multi-file reads
(read_files on the Volume) rescue-NULLed deferred_amount on 718 of 796 days.
This rewrites every local partition in place with the explicit schema; the
Volume copy is then refreshed with the databricks CLI dir-copy. Kept (not
deleted after use) because any future schema migration will need the same
pattern.

Values are preserved exactly. cast() runs in safe mode, so a conversion that
would lose information raises ArrowInvalid instead of silently corrupting the
column: int64->float64 (the money columns -- the only drift the real archive
turned out to have, on 718 of 796 days) is exact, while float64->int64 and
null->typed are performed too and would fail loudly on a non-integral or
out-of-range value.

Each partition is rewritten atomically (write a sibling .tmp, then os.replace),
so an interrupted run leaves every file either wholly old or wholly new --
never the footerless stub a direct overwrite strands, since pq.write_table
truncates its destination the moment it opens the sink. rewrite_root aborts
loudly on a malformed partition dir (FileNotFoundError for a missing
orders.parquet); rewriting is idempotent, so the recovery is to fix the dir and
re-run.

Usage:
    python -m ingest.rewrite_archive              # data/raw/orders
    python -m ingest.rewrite_archive --root <dir>
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pyarrow.parquet as pq

from ingest.orders import ORDERS_SCHEMA


def rewrite_partition(path: Path) -> bool:
    """Rewrite one orders.parquet to ORDERS_SCHEMA iff its schema differs;
    returns whether it was rewritten. A missing column raises (KeyError from
    select) -- loud beats silent. An extra column cuts the other way and is
    dropped silently, the same asymmetry noted at ingest/extract.py's
    from_pylist write; no archive file has one, since every partition was
    written from flatten_order's fixed key set."""
    with pq.ParquetFile(path) as pf:
        # Metadata-only comparison: a conforming file is never read at all.
        if pf.schema_arrow.equals(ORDERS_SCHEMA):
            return False
        table = pf.read()
    # Handle closed before os.replace -- Windows refuses to replace an open file.
    table = table.select(ORDERS_SCHEMA.names).cast(ORDERS_SCHEMA)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)
    return True


def rewrite_root(root: Path, *, log=None) -> list[str]:
    """Rewrite every partition under root; returns the rewritten dir names."""
    emit = log or (lambda _m: None)
    # Sweep orphan .tmp files from an interrupted earlier run before rewriting:
    # the Volume refresh is a `databricks fs cp -r` of this whole tree, and a
    # stray .tmp would ride along and break read_files across the directory.
    for stale in sorted(root.glob("business_date=*/*.tmp")):
        stale.unlink()
        emit(f"removed stale {stale.name} in {stale.parent.name}")
    rewritten = []
    parts = sorted(root.glob("business_date=*"))
    for part in parts:
        if rewrite_partition(part / "orders.parquet"):
            rewritten.append(part.name)
            emit(f"rewrote {part.name}")
    emit(f"{len(rewritten)} of {len(parts)} partitions rewritten")
    return rewritten


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rewrite order parquet partitions to ORDERS_SCHEMA."
    )
    parser.add_argument("--root", default="data/raw/orders")
    args = parser.parse_args()
    rewrite_root(Path(args.root), log=print)


if __name__ == "__main__":
    main()
