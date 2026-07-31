"""The rewriter reads legacy inference-typed partitions and rewrites them
in place with ORDERS_SCHEMA. Legacy shapes covered: INT64 money columns
(integral-sum days), float64 num_guests (pandas int+None promotion),
null-typed columns (all-None days). Values must be preserved exactly."""

import pandas as pd
import pyarrow.parquet as pq

from ingest.orders import ORDERS_SCHEMA
from ingest.rewrite_archive import rewrite_partition, rewrite_root


def _legacy_row(**overrides):
    row = {
        "business_date": 20260625,
        "order_guid": "o1",
        "opened_date": "2026-06-25T17:00:00.000+0000",
        "closed_date": "2026-06-25T17:30:00.000+0000",
        "source": "In Store",
        "dining_option_guid": None,
        "num_guests": None,
        "num_checks": 1,
        "net_amount": 10,
        "total_amount": 11,
        "tax_amount": 1,
        "tip_amount": 0,
        "deferred_amount": 0,
        "voided": False,
        "deleted": False,
    }
    row.update(overrides)
    return row


def _write_legacy(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_rewrite_partition_coerces_legacy_types_and_preserves_values(tmp_path):
    target = tmp_path / "business_date=20260625" / "orders.parquet"
    _write_legacy(target, [_legacy_row()])
    assert not pq.read_schema(target).equals(ORDERS_SCHEMA)  # proves fixture is legacy

    assert rewrite_partition(target) is True

    assert pq.read_schema(target).equals(ORDERS_SCHEMA)
    row = pq.ParquetFile(target).read().to_pylist()[0]
    assert row["net_amount"] == 10.0 and isinstance(row["net_amount"], float)
    assert row["deferred_amount"] == 0.0
    assert row["num_guests"] is None
    assert row["dining_option_guid"] is None
    assert row["order_guid"] == "o1"
    assert row["voided"] is False


def test_rewrite_partition_skips_already_conforming_files(tmp_path):
    target = tmp_path / "business_date=20260626" / "orders.parquet"
    _write_legacy(target, [_legacy_row(business_date=20260626)])
    rewrite_partition(target)
    mtime = target.stat().st_mtime_ns

    assert rewrite_partition(target) is False  # idempotent: no second write
    assert target.stat().st_mtime_ns == mtime


def test_rewrite_root_reports_rewritten_partitions(tmp_path):
    legacy = tmp_path / "business_date=20260625" / "orders.parquet"
    _write_legacy(legacy, [_legacy_row()])
    conforming = tmp_path / "business_date=20260626" / "orders.parquet"
    _write_legacy(conforming, [_legacy_row(business_date=20260626)])
    rewrite_partition(conforming)

    rewritten = rewrite_root(tmp_path)

    assert rewritten == ["business_date=20260625"]
    assert pq.read_schema(legacy).equals(ORDERS_SCHEMA)
