"""Flatten a Toast order into one analytics row, keeping only allowlisted fields.

The output dict is built from a fixed set of keys and only reads non-PII fields,
so customer contact, delivery address, and card data cannot appear downstream.
"""

from __future__ import annotations


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
