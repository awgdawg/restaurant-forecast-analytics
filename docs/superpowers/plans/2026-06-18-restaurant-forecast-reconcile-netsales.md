# Reconcile Net Sales to Toast (Deferred-Revenue Fix) Implementation Plan (Plan 4 of 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `fct_daily_sales.net_sales` match Toast's reported "Net sales" to the penny by excluding **deferred** revenue (gift cards), and lock it with a dbt **reconciliation test** against Toast's Sales Summary export.

**Why:** Reconciliation showed our net was high by exactly the deferred amount ($0/$30/$50 over three days = $80), traced to a single `deferred=true` "Gift Card" selection ($50 on 6/26). Toast excludes deferred revenue from Net Sales; `check.amount` includes it. Counts, tax, and total already match exactly.

**Tech Stack:** existing Python (`ingest.orders`), `databricks-sql-connector` loader, dbt-databricks, `pytest`.

---

## Scope (Plan 4 of 5)

- **In scope:** capture `deferred_amount` in `flatten_order`; carry it through bronze + dbt; redefine `net_sales = net_amount − deferred_amount`; seed Toast's daily net sales; a dbt reconciliation test (`|ours − Toast| < $0.01`); rebuild and prove it passes.
- **Out of scope:**
  - **Backfill** months of history (just `python -m ingest.extract` + `python -m load.run_load` over a long range — the fast batched loader makes it viable). Do this between Plan 4 and Plan 5.
  - **Plan 5:** baseline + Prophet + rolling backtest → Tableau Public.

**Repo root:** `E:\PyProj\restaurant-forecast-analytics` (PowerShell; `.\.venv\Scripts\python.exe`). `.env` has working creds. Confirmed: deferred items are `selections[]` with `deferred=true` (e.g. "Gift Card", $50 on 6/26).

---

## Task 1: Capture `deferred_amount` in `flatten_order` (TDD)

**Files:** Modify `tests/fixtures/sample_order.json`, `tests/test_orders.py`, `ingest/orders.py`

- [ ] **Step 1: Add a deferred gift-card selection to the fixture**

In `tests/fixtures/sample_order.json`, add a second selection to the existing check's `selections` array (after the "Burger" object). The `selections` array becomes:
```json
      "selections": [
        { "guid": "sel-1", "displayName": "Burger", "quantity": 1.0, "price": 12.0 },
        { "guid": "sel-gc", "displayName": "Gift Card", "price": 25.0, "deferred": true }
      ]
```

- [ ] **Step 2: Update `tests/test_orders.py`** — add `deferred_amount` to the allowlist and assert it

In `EXPECTED_KEYS`, add `"deferred_amount"`. Then add a test:
```python
def test_deferred_amount_sums_deferred_selections():
    row = flatten_order(FIXTURE)
    assert row["deferred_amount"] == 25.0
    # net_amount is still the raw check amount; the subtraction happens in dbt
    assert row["net_amount"] == 40.0
```

- [ ] **Step 3: Run tests to verify they fail**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -q
```
Expected: FAIL — `test_output_keys_are_exactly_the_allowlist` (missing `deferred_amount`) and `test_deferred_amount_sums_deferred_selections` (KeyError).

- [ ] **Step 4: Implement in `ingest/orders.py`**

Add this helper above `flatten_order`:
```python
def _deferred_amount(live_checks: list[dict]) -> float:
    return round(
        sum(
            (s.get("price") or 0.0)
            for c in live_checks
            for s in (c.get("selections") or [])
            if s.get("deferred") and not s.get("voided")
        ),
        4,
    )
```
Then add one line to the returned dict in `flatten_order` (e.g. right after `tip_amount`):
```python
        "deferred_amount": _deferred_amount(live_checks),
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -q
```
Expected: PASS (all `test_orders` tests, including the new one).

- [ ] **Step 6: Commit**

```powershell
git add ingest/orders.py tests/test_orders.py tests/fixtures/sample_order.json
git commit -m "feat: capture deferred_amount (gift cards) in flatten_order"
```

---

## Task 2: Carry `deferred_amount` through the bronze loader

**Files:** Modify `load/load_to_delta.py`, `tests/test_load_to_delta.py`

- [ ] **Step 1: Update the expected columns test**

In `tests/test_load_to_delta.py`, add `"deferred_amount"` to the `expected` set in `test_columns_match_flatten_output`.

- [ ] **Step 2: Run to verify it fails**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_load_to_delta.py -q
```
Expected: FAIL — `test_columns_match_flatten_output` (COLUMNS missing `deferred_amount`).

- [ ] **Step 3: Update `load/load_to_delta.py`**

Add `"deferred_amount"` to the `COLUMNS` list (after `"tip_amount"`), and `"deferred_amount": "DOUBLE",` to the `_DDL_TYPES` dict.

- [ ] **Step 4: Run to verify it passes**

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_load_to_delta.py -q
```
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```powershell
git add load/load_to_delta.py tests/test_load_to_delta.py
git commit -m "feat: add deferred_amount column to bronze loader"
```

---

## Task 3: Re-extract + recreate bronze *(live — schema changed)*

The bronze table now needs a new column, and the existing Parquet predates `deferred_amount`, so re-extract and rebuild bronze.

- [ ] **Step 1: Re-extract the loaded days (now with `deferred_amount`)**

```powershell
.\.venv\Scripts\python.exe -m ingest.extract --start 2026-06-24 --end 2026-06-26
```
Expected: `Wrote 293 order rows under data/raw/orders`.

- [ ] **Step 2: Drop the old bronze table, then reload**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
.\.venv\Scripts\python.exe -c "from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('DROP TABLE IF EXISTS bronze_orders'); cur.close(); c.close(); print('dropped')"
.\.venv\Scripts\python.exe -m load.run_load
```
Expected: `dropped`, then `Loaded 293 rows into bronze_orders` (recreated with the 15-column schema).

- [ ] **Step 3: Verify the deferred column landed**

```powershell
.\.venv\Scripts\python.exe -c "from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('SELECT business_date, round(sum(net_amount),2) net, round(sum(deferred_amount),2) deferred FROM bronze_orders GROUP BY business_date ORDER BY business_date'); [print(r) for r in cur.fetchall()]; cur.close(); c.close()"
```
Expected: `deferred` per day = `0.00 / 30.00 / 50.00` (the gift-card amounts). *(No commit.)*

---

## Task 4: Redefine `net_sales` in dbt

**Files:** Modify `models/staging/stg_orders.sql`, `models/marts/fct_daily_sales.sql`

- [ ] **Step 1: Pass `deferred_amount` through `models/staging/stg_orders.sql`**

Add `deferred_amount,` to the select list (e.g. right after `tip_amount,`).

- [ ] **Step 2: Subtract it in `models/marts/fct_daily_sales.sql`**

Replace the `net_sales` line so net sales excludes deferred revenue, and expose deferred separately:
```sql
    round(sum(net_amount) - sum(deferred_amount), 2)  as net_sales,
    round(sum(deferred_amount), 2)                    as deferred_revenue,
```
(Keep `total_sales`, `tax`, `tips`, `order_count`, `guest_count` as they are.)

- [ ] **Step 3: Commit**

```powershell
git add models/staging/stg_orders.sql models/marts/fct_daily_sales.sql
git commit -m "feat: net_sales excludes deferred revenue (matches Toast definition)"
```

---

## Task 5: Seed Toast's reported net sales

**Files:** Create `seeds/toast_sales_summary.csv`

> Committed deliberately (owner publishes real figures; `.gitignore` allows `seeds/**/*.csv`). Values are from the export's `Sales by day.csv`.

- [ ] **Step 1: Create `seeds/toast_sales_summary.csv`**

```csv
business_date,net_sales
20260624,2134.11
20260625,2819.68
20260626,2634.59
```

- [ ] **Step 2: Load the seed**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
$env:DBT_PROFILES_DIR = (Get-Location).Path
.\.venv\Scripts\dbt.exe seed
```
Expected: `toast_sales_summary` seed loads.

- [ ] **Step 3: Commit**

```powershell
git add seeds/toast_sales_summary.csv
git commit -m "test: seed Toast reported daily net sales for reconciliation"
```

---

## Task 6: Reconciliation test

**Files:** Create `tests/assert_net_sales_reconciles_to_toast.sql`

- [ ] **Step 1: Write the singular test**

`tests/assert_net_sales_reconciles_to_toast.sql`:
```sql
-- fails for any day where our net_sales differs from Toast's reported net sales by > 1 cent
with ours as (
    select business_date, net_sales from {{ ref('fct_daily_sales') }}
),

toast as (
    select business_date, net_sales as toast_net_sales
    from {{ ref('toast_sales_summary') }}
)

select
    ours.business_date,
    ours.net_sales,
    toast.toast_net_sales,
    abs(ours.net_sales - toast.toast_net_sales) as diff
from ours
join toast using (business_date)
where abs(ours.net_sales - toast.toast_net_sales) > 0.01
```

- [ ] **Step 2: Commit**

```powershell
git add tests/assert_net_sales_reconciles_to_toast.sql
git commit -m "test: reconcile net_sales to Toast Sales Summary (<= 1 cent)"
```

---

## Task 7: Build and prove reconciliation *(live)*

- [ ] **Step 1: Build everything + run all tests**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
$env:DBT_PROFILES_DIR = (Get-Location).Path
.\.venv\Scripts\dbt.exe build
```
Expected: all models build and **all tests PASS**, including `assert_net_sales_reconciles_to_toast`.

- [ ] **Step 2: Eyeball the now-matching numbers**

```powershell
.\.venv\Scripts\dbt.exe show --inline "select o.business_date, o.net_sales, t.net_sales as toast_net from {{ ref('fct_daily_sales') }} o join {{ ref('toast_sales_summary') }} t using (business_date) order by 1" --limit 10
```
Expected: `net_sales` == `toast_net` per day (2134.11 / 2819.68 / 2634.59). *(No commit — dbt artifacts gitignored.)*

---

## Follow-on
- **Backfill**: `python -m ingest.extract --start <~2yr ago> --end <yesterday>` then `python -m load.run_load`, then `dbt build`. Confirm reconciliation still passes on spot-check days (extend the seed as desired).
- **Plan 5**: baseline (seasonal-naive) + Prophet + rolling backtest → Tableau Public dashboard + portfolio embed.

---

## Self-Review (completed during authoring)

- **Spec coverage:** §13/§18 #2 reconciliation ✓ (the gap deferred from Plan 3 is closed here, grounded in the real export). §10 mart gains a correct `net_sales` + `deferred_revenue`.
- **Placeholder scan:** none — every step is concrete; the deferred mechanism is confirmed against real data ($50 gift card on 6/26).
- **Type consistency:** `deferred_amount` flows identically through `flatten_order` (Task 1) → `COLUMNS`/DDL (Task 2) → `stg_orders` (Task 4) → `fct_daily_sales` net_sales (Task 4); the reconciliation test joins `fct_daily_sales` to the `toast_sales_summary` seed on `business_date`.
- **Grounded:** seed values from `Sales by day.csv`; deferred confirmed as `selections[].deferred` gift cards.
