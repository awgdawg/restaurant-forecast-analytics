from datetime import date

import pandas as pd

from ingest.extract import extract_range


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
