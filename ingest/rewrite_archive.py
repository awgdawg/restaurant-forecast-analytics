"""One-off: rewrite legacy inference-typed order partitions to ORDERS_SCHEMA.

Historical context (2026-07-31): pandas.to_parquet inference wrote INT64 money
columns on integral-sum days and DOUBLE elsewhere, so typed multi-file reads
(read_files on the Volume) rescue-NULLed deferred_amount on 718 of 796 days.
This rewrites every local partition in place with the explicit schema; the
Volume copy is then refreshed with the databricks CLI dir-copy. Values are
preserved exactly (int->float widening only). Kept (not deleted after use)
because any future schema migration will need the same pattern.

Usage:
    python -m ingest.rewrite_archive              # data/raw/orders
    python -m ingest.rewrite_archive --root <dir>
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq

from ingest.orders import ORDERS_SCHEMA


def rewrite_partition(path: Path) -> bool:
    """Rewrite one orders.parquet in place iff its schema differs. A missing
    column raises (KeyError from select) -- loud beats silent. An extra column
    cuts the other way and is dropped silently, the same asymmetry noted at
    ingest/extract.py's from_pylist write; no archive file has one, since every
    partition was written from flatten_order's fixed key set."""
    table = pq.ParquetFile(path).read()
    if table.schema.equals(ORDERS_SCHEMA):
        return False
    table = table.select(ORDERS_SCHEMA.names).cast(ORDERS_SCHEMA)
    pq.write_table(table, path)
    return True


def rewrite_root(root: Path, log=None) -> list[str]:
    """Rewrite every partition under root; returns the rewritten dir names."""
    emit = log or (lambda _m: None)
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
