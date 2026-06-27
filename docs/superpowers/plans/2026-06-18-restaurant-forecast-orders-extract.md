# Orders Extract & PII-Strip Implementation Plan (Plan 2 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn live Toast orders into clean, **PII-stripped**, analysis-ready Parquet for a date range — implementing the spec's §14 allowlist requirement with tests, built against the *real* Orders schema captured in `docs/toast-orders-shape.md`.

**Architecture:** A pure `flatten_order()` function (order-grain, allowlist-only output → PII cannot leak) + a range CLI that pulls each business date via the existing `ToastClient`, flattens, and writes one Parquet partition per day under `data/raw/orders/` (gitignored).

**Tech Stack:** Python 3.10, `pandas` + `pyarrow` (Parquet), existing `ingest.toast_client` / `ingest.config`, `pytest`.

---

## Scope (Plan 2 of 4)

This plan is deliberately the **fully-concrete, local, no-external-unknowns** slice:

- **In scope:** dependencies, a synthetic test fixture, `flatten_order()` (allowlist + PII strip, TDD), the range extractor CLI (TDD with a fake client), and one live smoke run.
- **Out of scope (later plans, because they have Free-Edition/Toast specifics worth verifying live):**
  - **Plan 3** — load Parquet → Delta **bronze** (via `databricks-sql-connector`), dbt staging + `fct_daily_sales` mart + the reconciliation test vs. Toast's sales summary.
  - **Plan 4** — baseline + Prophet forecast + rolling backtest → Tableau Public dashboard.

**Repo root for all paths/commands:** `E:\PyProj\restaurant-forecast-analytics` (PowerShell). The venv, `ingest/config.py`, and `ingest/toast_client.py` already exist on `main`. Activate the venv or call `.\.venv\Scripts\python.exe` directly.

**PII guarantee (the whole point):** `flatten_order()` builds its output dict from a *fixed* set of keys and only ever reads non-PII fields — so customer contact, delivery address, and card data are structurally impossible to include. Tests assert this.

---

## Task 1: Add Parquet dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add pandas + pyarrow to `requirements.txt`**

Append these two lines (keep existing lines):
```
pandas==2.2.3
pyarrow==17.0.0
```

- [ ] **Step 2: Install**

Run:
```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
Expected: completes; `.\.venv\Scripts\python.exe -c "import pandas, pyarrow; print('ok')"` prints `ok`.

- [ ] **Step 3: Commit**

```powershell
git add requirements.txt
git commit -m "chore: add pandas + pyarrow for Parquet output"
```

---

## Task 2: Synthetic order fixture (real schema shape, fake PII)

A committed fixture lets us test flattening without ever committing real customer data. It mirrors the captured schema and includes **fake** PII so tests can prove the PII is stripped.

**Files:**
- Create: `tests/fixtures/sample_order.json`

- [ ] **Step 1: Create `tests/fixtures/sample_order.json`**

```json
{
  "guid": "order-aaaa-0001",
  "businessDate": 20260626,
  "numberOfGuests": 2,
  "openedDate": "2026-06-26T17:32:00.000+0000",
  "closedDate": "2026-06-26T18:10:00.000+0000",
  "source": "In Store",
  "voided": false,
  "deleted": false,
  "diningOption": { "guid": "dine-in-guid", "entityType": "DiningOption" },
  "deliveryInfo": { "address1": "123 Fake St", "city": "Kansas City", "zipCode": "64111" },
  "checks": [
    {
      "guid": "check-0001",
      "amount": 40.0,
      "totalAmount": 43.4,
      "taxAmount": 3.4,
      "voided": false,
      "deleted": false,
      "customer": {
        "guid": "cust-1",
        "email": "jane@example.com",
        "firstName": "Jane",
        "lastName": "Doe",
        "phone": "+18165551234"
      },
      "payments": [
        { "guid": "pay-1", "amount": 43.4, "tipAmount": 8.0, "type": "CREDIT", "last4Digits": "4242" }
      ],
      "selections": [
        { "guid": "sel-1", "displayName": "Burger", "quantity": 1.0, "price": 12.0 }
      ]
    }
  ]
}
```

- [ ] **Step 2: Commit**

```powershell
git add tests/fixtures/sample_order.json
git commit -m "test: synthetic Toast order fixture (fake PII)"
```

---

## Task 3: `flatten_order()` — allowlist + PII strip

**Files:**
- Create: `ingest/orders.py`
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing test**

`tests/test_orders.py`:
```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'ingest.orders'`.

- [ ] **Step 3: Write the implementation**

`ingest/orders.py`:
```python
"""Flatten a Toast order into one analytics row, keeping only allowlisted fields.

The output dict is built from a fixed set of keys and only reads non-PII fields,
so customer contact, delivery address, and card data cannot appear downstream.
"""

from __future__ import annotations


def _is_live(node: dict) -> bool:
    return not node.get("voided", False) and not node.get("deleted", False)


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
        "voided": bool(order.get("voided", False)),
        "deleted": bool(order.get("deleted", False)),
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v
```
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```powershell
git add ingest/orders.py tests/test_orders.py
git commit -m "feat: flatten_order with allowlist (PII-stripped, order grain)"
```

---

## Task 4: Edge cases — voided checks and multiple checks

**Files:**
- Modify: `tests/test_orders.py`

- [ ] **Step 1: Add failing tests for voided + multi-check behavior**

Append to `tests/test_orders.py`:
```python
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
```

- [ ] **Step 2: Run the tests**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v
```
Expected: PASS (5 passed). The Task 3 implementation already handles these — this task locks the behavior in with tests. If any fail, fix `ingest/orders.py` until green.

- [ ] **Step 3: Commit**

```powershell
git add tests/test_orders.py
git commit -m "test: lock voided-exclusion and multi-check summing"
```

---

## Task 5: Range extractor CLI

**Files:**
- Create: `ingest/extract.py`
- Test: `tests/test_extract.py`

- [ ] **Step 1: Write the failing test (fake client, temp dir — no network)**

`tests/test_extract.py`:
```python
from datetime import date

import pandas as pd

from ingest.extract import extract_range


class FakeClient:
    def __init__(self, orders_by_date):
        self._orders = orders_by_date
        self.seen = []

    def get(self, path, params=None):
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_extract.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'ingest.extract'`.

- [ ] **Step 3: Write the implementation**

`ingest/extract.py`:
```python
"""Pull a date range of Toast orders, flatten + strip PII, write Parquet.

Usage:
    python -m ingest.extract --start 2026-06-20 --end 2026-06-26
"""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.orders import flatten_order
from ingest.toast_client import ToastClient


def _daterange(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def extract_range(client, start: date, end: date, out_dir: Path) -> int:
    """Pull each business date in [start, end], write one Parquet file per
    non-empty day under out_dir/business_date=YYYYMMDD/. Returns total rows."""
    total = 0
    for day in _daterange(start, end):
        bd = day.strftime("%Y%m%d")
        orders = client.get("/orders/v2/ordersBulk", params={"businessDate": bd})
        rows = [flatten_order(o) for o in orders] if isinstance(orders, list) else []
        if not rows:
            continue
        part = out_dir / f"business_date={bd}"
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(part / "orders.parquet", index=False)
        total += len(rows)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Toast orders to Parquet.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    load_dotenv()
    client = ToastClient(load_toast_config())
    out_dir = Path("data/raw/orders")
    total = extract_range(
        client, date.fromisoformat(args.start), date.fromisoformat(args.end), out_dir
    )
    print(f"Wrote {total} order rows under {out_dir}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_extract.py -v
```
Expected: PASS (1 passed).

- [ ] **Step 5: Run the full suite + commit**

```powershell
.\.venv\Scripts\python.exe -m pytest -q
git add ingest/extract.py tests/test_extract.py
git commit -m "feat: range extractor CLI -> partitioned Parquet"
```
Expected: all tests pass before committing.

---

## Task 6: Live smoke run *(requires Toast creds in `.env`)*

**Files:** none — this validates the real pipeline end-to-end.

- [ ] **Step 1: Extract a small real range**

Run (pick a few recent open days):
```powershell
.\.venv\Scripts\python.exe -m ingest.extract --start 2026-06-24 --end 2026-06-26
```
Expected: prints `Wrote <N> order rows under data/raw/orders` with N > 0.

- [ ] **Step 2: Verify the output is clean and PII-free**

Run:
```powershell
.\.venv\Scripts\python.exe -c "import pandas as pd, glob; f=glob.glob('data/raw/orders/**/orders.parquet', recursive=True); df=pd.concat(pd.read_parquet(x) for x in f); print(df.columns.tolist()); print(df.shape); print('PII cols:', [c for c in df.columns if any(t in c.lower() for t in ('customer','email','phone','address','card','name'))])"
```
Expected: columns are exactly the 14 allowlisted fields; `PII cols: []`; a sensible row count and daily totals.

- [ ] **Step 3: Confirm nothing under `data/` is tracked by git**

Run:
```powershell
git status --short
git check-ignore data/raw/orders
```
Expected: `git status` shows no `data/` entries; `git check-ignore` prints the path. *(No commit — outputs stay local.)*

---

## Self-Review (completed during authoring)

- **Spec coverage:** §14 "Customer PII — stripped from the forecast pipeline" is implemented and tested here (Tasks 3–5 build the allowlist; tests assert no PII keys survive). §16 M1 (range ingest) is realized as `extract.py`. Load→Delta and the marts/reconciliation (§10, §13, M2) are explicitly **Plan 3**; forecast/dashboard (§11, M3–M4) are **Plan 4** — stated in Scope.
- **Placeholder scan:** none — every code/command step is complete and runnable.
- **Type consistency:** `flatten_order(order: dict) -> dict` is defined in Task 3 and used identically in `extract_range` (Task 5) and the tests. `extract_range(client, start, end, out_dir)` matches between Task 5's test and implementation. Output keys are identical across the fixture test, edge-case tests, and the extractor's PII-column assertion.
- **Real-schema grounded:** field names (`businessDate`, `checks[].amount/totalAmount/taxAmount`, `payments[].tipAmount`, `numberOfGuests`, `diningOption.guid`, `voided/deleted`) all come from the captured `docs/toast-orders-shape.md`.
