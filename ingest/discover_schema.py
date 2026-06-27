"""One-shot: authenticate to Toast, pull one business date of orders, and
record the real response shape. Requires real credentials in .env.

Usage (PowerShell):
    python -m ingest.discover_schema 2026-06-26
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.toast_client import ToastClient


def shape(value: object, prefix: str = "") -> list[str]:
    """Return 'path: type' lines describing the structure of a value."""
    lines: list[str] = []
    if isinstance(value, dict):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else key
            lines += shape(value[key], child)
    elif isinstance(value, list):
        lines.append(f"{prefix}: list[{len(value)}]")
        if value:
            lines += shape(value[0], f"{prefix}[]")
    else:
        lines.append(f"{prefix}: {type(value).__name__}")
    return lines


def main(business_date: str) -> None:
    load_dotenv()
    cfg = load_toast_config()
    client = ToastClient(cfg)

    params = {"businessDate": business_date.replace("-", "")}
    orders = client.get("/orders/v2/ordersBulk", params=params)

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "sample_orders.json").write_text(json.dumps(orders, indent=2))

    is_list = isinstance(orders, list)
    count = len(orders) if is_list else "n/a (not a list)"
    shape_lines = shape(orders[0]) if is_list and orders else ["(no orders returned)"]

    body = "\n".join(shape_lines)
    doc = (
        "# Toast Orders response shape\n\n"
        f"- Source: `GET /orders/v2/ordersBulk?businessDate={params['businessDate']}`\n"
        f"- Orders returned: {count}\n\n"
        f"## Field paths (first order)\n\n```\n{body}\n```\n"
    )
    Path("docs/toast-orders-shape.md").write_text(doc)
    print(
        f"Orders returned: {count}. "
        "Wrote data/raw/sample_orders.json and docs/toast-orders-shape.md"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m ingest.discover_schema YYYY-MM-DD")
    main(sys.argv[1])
