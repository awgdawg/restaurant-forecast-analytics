"""Upload local Parquet partitions to the UC Volume via the Files API.

The workspace's serverless egress wall blocks Toast (proven exhaustively;
see docs/superpowers/specs/2026-07-30-cloud-extraction-design.md), so
extraction runs OUTSIDE Databricks and pushes partitions here. The SDK import
is lazy and the client honors the repo's DBX_* env convention.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

DEFAULT_VOLUME_ROOT = "/Volumes/workspace/default/raw_orders"


def _workspace_client():
    import os

    from databricks.sdk import WorkspaceClient

    if "DBX_HOST" in os.environ:
        return WorkspaceClient(
            host=f"https://{os.environ['DBX_HOST']}",
            token=os.environ["DBX_TOKEN"],
        )
    return WorkspaceClient()


def local_partitions(root: Path, days: list[date]) -> list[tuple[str, Path]]:
    """(yyyymmdd, parquet path) for each day whose partition exists on disk.
    Zero-order days write no partition and are skipped silently."""
    out = []
    for day in days:
        bd = day.strftime("%Y%m%d")
        target = root / f"business_date={bd}" / "orders.parquet"
        if target.exists():
            out.append((bd, target))
    return out


def upload_partitions(
    root: Path,
    days: list[date],
    volume_root: str = DEFAULT_VOLUME_ROOT,
    *,
    client=None,
    log=None,
) -> int:
    """Upload each existing partition's orders.parquet (overwrite, idempotent).
    Parent directories are created explicitly -- the Files API does not
    reliably create them (the CLI single-file cp provably does not)."""
    emit = log or (lambda _m: None)
    parts = local_partitions(root, days)
    if not parts:
        return 0
    client = client or _workspace_client()
    for bd, path in parts:
        part_dir = f"{volume_root}/business_date={bd}"
        client.files.create_directory(part_dir)
        with open(path, "rb") as f:
            client.files.upload(f"{part_dir}/orders.parquet", f, overwrite=True)
        emit(f"uploaded business_date={bd}")
    return len(parts)
