from datetime import date

import pandas as pd

from ingest.extract import _day_has_orders, extract_range


class FakeClient:
    def __init__(self, orders_by_date):
        self._orders = orders_by_date
        self.seen = []

    def get_paginated(self, path, params=None, page_size=100):
        self.seen.append(params["businessDate"])
        return self._orders.get(params["businessDate"], [])


def test_extract_range_writes_one_partition_per_nonempty_day(tmp_path):
    order = {
        "guid": "o1",
        "businessDate": 20260625,
        "numberOfGuests": 2,
        "checks": [{"amount": 10.0, "totalAmount": 11.0, "taxAmount": 1.0}],
    }
    client = FakeClient({"20260625": [order], "20260626": []})

    n = extract_range(client, date(2026, 6, 25), date(2026, 6, 26), tmp_path)

    assert n == 1
    assert client.seen == ["20260625", "20260626"]  # both days queried
    df = pd.read_parquet(tmp_path / "business_date=20260625" / "orders.parquet")
    assert list(df["order_guid"]) == ["o1"]
    assert "customer" not in " ".join(df.columns)  # no PII columns
    # empty day creates no partition
    assert not (tmp_path / "business_date=20260626").exists()


def _order(guid, business_date):
    return {
        "guid": guid,
        "businessDate": business_date,
        "numberOfGuests": 2,
        "checks": [{"amount": 10.0, "totalAmount": 11.0, "taxAmount": 1.0}],
    }


def test_extract_skips_days_already_on_disk(tmp_path):
    client = FakeClient(
        {"20260625": [_order("o625", 20260625)], "20260626": [_order("o626", 20260626)]}
    )
    # pre-stage 6/25 as already downloaded
    part = tmp_path / "business_date=20260625"
    part.mkdir(parents=True)
    pd.DataFrame([{"order_guid": "old"}]).to_parquet(part / "orders.parquet", index=False)

    n = extract_range(client, date(2026, 6, 25), date(2026, 6, 26), tmp_path)

    assert client.seen == ["20260626"]  # 6/25 not re-fetched
    assert n == 1  # only the newly written day counts
    # existing file left untouched
    df625 = pd.read_parquet(part / "orders.parquet")
    assert list(df625["order_guid"]) == ["old"]


def test_extract_overwrite_refetches_existing_days(tmp_path):
    client = FakeClient({"20260625": [_order("o625", 20260625)]})
    part = tmp_path / "business_date=20260625"
    part.mkdir(parents=True)
    pd.DataFrame([{"order_guid": "old"}]).to_parquet(part / "orders.parquet", index=False)

    n = extract_range(
        client, date(2026, 6, 25), date(2026, 6, 25), tmp_path, overwrite=True
    )

    assert client.seen == ["20260625"]  # refetched despite existing file
    assert n == 1
    df625 = pd.read_parquet(part / "orders.parquet")
    assert list(df625["order_guid"]) == ["o625"]  # overwritten with fresh data


class ProbeClient:
    def __init__(self, batch):
        self._batch = batch
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        return self._batch


def test_day_has_orders_true_when_rows_returned():
    c = ProbeClient([{"guid": "x"}])

    assert _day_has_orders(c, date(2026, 6, 25)) is True
    path, params = c.calls[0]
    assert path == "/orders/v2/ordersBulk"
    assert params["businessDate"] == "20260625"
    assert params["pageSize"] == 1  # cheap probe, not a full page pull


def test_day_has_orders_false_when_empty():
    c = ProbeClient([])

    assert _day_has_orders(c, date(2026, 6, 25)) is False
