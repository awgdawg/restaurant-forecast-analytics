import json
from pathlib import Path

import pyarrow as pa

from ingest.orders import ORDERS_SCHEMA, flatten_order
from load.load_to_delta import COLUMNS, _DDL_TYPES

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
    "deferred_amount",
    "voided",
    "deleted",
}
PII_TOKENS = (
    "customer",
    "email",
    "phone",
    "first",
    "last",
    "name",
    "address",
    "delivery",
    "card",
    "digits",
    "payment",
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


def test_voided_checks_are_excluded_from_sums():
    order = {
        "guid": "o2",
        "businessDate": 20260626,
        "numberOfGuests": 1,
        "checks": [
            {"amount": 10.0, "totalAmount": 11.0, "taxAmount": 1.0, "voided": False},
            {"amount": 99.0, "totalAmount": 99.0, "taxAmount": 0.0, "voided": True},
        ],
    }
    row = flatten_order(order)
    assert row["num_checks"] == 2  # count keeps both
    assert row["net_amount"] == 10.0  # sum excludes the voided check
    assert row["total_amount"] == 11.0


def test_multiple_live_checks_are_summed():
    order = {
        "guid": "o3",
        "businessDate": 20260626,
        "checks": [
            {"amount": 10.0, "totalAmount": 11.0, "taxAmount": 1.0},
            {"amount": 5.0, "totalAmount": 5.5, "taxAmount": 0.5},
        ],
    }
    row = flatten_order(order)
    assert row["net_amount"] == 15.0
    assert row["total_amount"] == 16.5
    assert row["num_guests"] is None  # missing optional field -> None, no crash


def test_deferred_amount_sums_deferred_selections():
    row = flatten_order(FIXTURE)
    assert row["deferred_amount"] == 25.0
    # net_amount is still the raw check amount; the subtraction happens in dbt
    assert row["net_amount"] == 40.0


def test_orders_schema_mirrors_bronze_ddl():
    """ORDERS_SCHEMA and load_to_delta's bronze DDL must agree by construction:
    same columns, same order, corresponding types. INT is widened to int64 in
    parquet (safe: bronze ingests via SQL INSERT, not direct parquet load)."""
    ddl_to_arrow = {
        "BIGINT": pa.int64(),
        "INT": pa.int64(),
        "DOUBLE": pa.float64(),
        "STRING": pa.string(),
        "BOOLEAN": pa.bool_(),
    }
    assert ORDERS_SCHEMA.names == COLUMNS
    for name in COLUMNS:
        assert ORDERS_SCHEMA.field(name).type == ddl_to_arrow[_DDL_TYPES[name]], name
        assert ORDERS_SCHEMA.field(name).nullable, name
    # ...and the schema must match what the builder actually emits. Order-sensitive
    # on purpose: pa.Table.from_pylist silently drops keys absent from the schema,
    # so a field added to flatten_order without touching COLUMNS/_DDL_TYPES would
    # vanish from every written parquet with no error anywhere.
    assert list(flatten_order(FIXTURE)) == ORDERS_SCHEMA.names


def test_money_fields_are_floats_even_from_integral_inputs():
    """Toast can send whole-dollar amounts as JSON ints; round(int, n) stays
    int, which is the writer-side half of the INT64/DOUBLE parquet drift."""
    order = {
        "guid": "o-int",
        "businessDate": 20260701,
        "checks": [
            {
                "amount": 10,
                "totalAmount": 11,
                "taxAmount": 1,
                "payments": [{"tipAmount": 2}],
                "selections": [{"price": 5, "deferred": True}],
            }
        ],
    }
    row = flatten_order(order)
    for field in (
        "net_amount",
        "total_amount",
        "tax_amount",
        "tip_amount",
        "deferred_amount",
    ):
        assert isinstance(row[field], float), field


def test_money_fields_are_floats_on_empty_sums():
    """Zero-deferred / zero-payment days sum empty iterables -> int 0 today."""
    order = {"guid": "o-empty", "businessDate": 20260701, "checks": [{"amount": 10.5}]}
    row = flatten_order(order)
    assert isinstance(row["deferred_amount"], float)
    assert isinstance(row["tip_amount"], float)

    # A fully-voided order leaves live_checks empty, so _sum itself sums an empty
    # iterable -- the shape behind the drifted rows in the live baseline.
    voided_row = flatten_order(
        {
            "guid": "o-voided",
            "businessDate": 20260701,
            "checks": [{"amount": 9.0, "voided": True}],
        }
    )
    for field in ("net_amount", "total_amount", "tax_amount"):
        assert isinstance(voided_row[field], float), field
