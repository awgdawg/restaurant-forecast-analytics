import json
from pathlib import Path

from ingest.orders import flatten_order

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "sample_order.json").read_text()
)

EXPECTED_KEYS = {
    "business_date",
    "order_guid",
    "opened_date",
    "closed_date",
    "source",
    "dining_option_guid",
    "num_guests",
    "num_checks",
    "net_amount",
    "total_amount",
    "tax_amount",
    "tip_amount",
    "voided",
    "deleted",
}
PII_TOKENS = (
    "customer", "email", "phone", "first", "last", "name",
    "address", "delivery", "card", "digits", "payment",
)


def test_output_keys_are_exactly_the_allowlist():
    assert set(flatten_order(FIXTURE).keys()) == EXPECTED_KEYS


def test_no_key_looks_like_pii():
    for key in flatten_order(FIXTURE):
        assert not any(tok in key.lower() for tok in PII_TOKENS), key


def test_amounts_and_guests():
    row = flatten_order(FIXTURE)
    assert row["business_date"] == 20260626
    assert row["order_guid"] == "order-aaaa-0001"
    assert row["num_guests"] == 2
    assert row["num_checks"] == 1
    assert row["net_amount"] == 40.0
    assert row["total_amount"] == 43.4
    assert row["tax_amount"] == 3.4
    assert row["tip_amount"] == 8.0
    assert row["dining_option_guid"] == "dine-in-guid"
    assert row["voided"] is False
