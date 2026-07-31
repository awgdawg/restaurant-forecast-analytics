"""Flatten a Toast order into one analytics row, keeping only allowlisted fields.

The output dict is built from a fixed set of keys and only reads non-PII fields,
so customer contact, delivery address, and card data cannot appear downstream.
"""

from __future__ import annotations

import pyarrow as pa

# The one authoritative parquet schema for order rows. Mirrors the bronze DDL
# (load/load_to_delta.py::_DDL_TYPES) so parquet and Delta agree by
# construction; a schema-inferring write is how deferred_amount drifted
# INT64/DOUBLE across days and got rescue-NULLed by typed multi-file reads
# (see models/marts/intraday_today.sql, 2026-07-31). Money is float64 even
# when a day's sums are integral; num_guests stays a nullable int64 even on
# all-None days (inference makes those null-typed).
ORDERS_SCHEMA = pa.schema(
    [
        ("business_date", pa.int64()),
        ("order_guid", pa.string()),
        ("opened_date", pa.string()),
        ("closed_date", pa.string()),
        ("source", pa.string()),
        ("dining_option_guid", pa.string()),
        ("num_guests", pa.int64()),
        ("num_checks", pa.int64()),
        ("net_amount", pa.float64()),
        ("total_amount", pa.float64()),
        ("tax_amount", pa.float64()),
        ("tip_amount", pa.float64()),
        ("deferred_amount", pa.float64()),
        ("voided", pa.bool_()),
        ("deleted", pa.bool_()),
    ]
)


def _is_live(node: dict) -> bool:
    return not node.get("voided", False) and not node.get("deleted", False)


def _deferred_amount(live_checks: list[dict]) -> float:
    """Sum prices of deferred (e.g. gift-card) selections — excluded from net sales."""
    return round(
        sum(
            (s.get("price") or 0.0)
            for c in live_checks
            for s in (c.get("selections") or [])
            if s.get("deferred") and not s.get("voided")
        ),
        4,
    )


def flatten_order(order: dict) -> dict:
    """Return one order-grain row. Amounts are summed across non-voided checks."""
    checks = order.get("checks") or []
    live_checks = [c for c in checks if _is_live(c)]

    def _sum(field: str) -> float:
        return round(sum((c.get(field) or 0.0) for c in live_checks), 4)

    tip_amount = round(
        sum(
            (p.get("tipAmount") or 0.0)
            for c in live_checks
            for p in (c.get("payments") or [])
        ),
        4,
    )

    dining_option = order.get("diningOption") or {}

    return {
        "business_date": order.get("businessDate"),
        "order_guid": order.get("guid"),
        "opened_date": order.get("openedDate"),
        "closed_date": order.get("closedDate"),
        "source": order.get("source"),
        "dining_option_guid": dining_option.get("guid"),
        "num_guests": order.get("numberOfGuests"),
        "num_checks": len(checks),
        "net_amount": _sum("amount"),
        "total_amount": _sum("totalAmount"),
        "tax_amount": _sum("taxAmount"),
        "tip_amount": tip_amount,
        "deferred_amount": _deferred_amount(live_checks),
        "voided": bool(order.get("voided", False)),
        "deleted": bool(order.get("deleted", False)),
    }
