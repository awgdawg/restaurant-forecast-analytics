# Load to Delta Bronze + Daily Sales Mart Implementation Plan (Plan 3 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load the clean, PII-stripped order Parquet into a Databricks Delta **bronze** table, then model it with dbt into a `fct_daily_sales` mart (one row per business date) with data-quality tests — the trustworthy daily series the forecast (Plan 4) consumes.

**Architecture:** Local Python loads Parquet → Delta bronze via `databricks-sql-connector` (idempotent per business date). dbt (already wired, `dbt debug` passes) reads bronze as a source → `stg_orders` (typed, voids excluded) → `fct_daily_sales` (aggregated). The load mechanism is **verified live by a spike before** the full loader is trusted (Free-Edition specifics).

**Tech Stack:** Python 3.10, `databricks-sql-connector`, `pandas`, existing `dbt-databricks` project, `pytest`.

---

## Scope (Plan 3 of 4)

- **In scope:** load Parquet → Delta bronze (spike-verified, idempotent), dbt `stg_orders` + `fct_daily_sales`, structural dbt tests (not_null/unique/non-negative).
- **Out of scope:**
  - **Reconciliation vs. Toast's own Sales Summary** — needs the owner to export a few days of Toast's reported totals; it's a **Plan 4** task (scaffold noted at the end here).
  - **Plan 4:** reconciliation + baseline + Prophet + backtest → Tableau Public.

**Prereqs:** `.env` has working `DBX_*` values (proven by `dbt debug` in Plan 1) and Toast creds; order Parquet exists under `data/raw/orders/` (run `python -m ingest.extract --start <s> --end <e>` first). Repo root: `E:\PyProj\restaurant-forecast-analytics` (PowerShell; `.\.venv\Scripts\python.exe`).

**Bronze grain & columns** (from `ingest.orders.flatten_order`, order grain):
`business_date BIGINT, order_guid STRING, opened_date STRING, closed_date STRING, source STRING, dining_option_guid STRING, num_guests INT, num_checks INT, net_amount DOUBLE, total_amount DOUBLE, tax_amount DOUBLE, tip_amount DOUBLE, voided BOOLEAN, deleted BOOLEAN`.

---

## Task 1: Add the Databricks SQL connector

**Files:** Modify `requirements.txt`

- [ ] **Step 1: Append to `requirements.txt`**

```
databricks-sql-connector==3.4.0
```

- [ ] **Step 2: Install + verify import**

Run:
```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -c "from databricks import sql; print('connector ok')"
```
Expected: prints `connector ok`.

- [ ] **Step 3: Commit**

```powershell
git add requirements.txt
git commit -m "chore: add databricks-sql-connector"
```

---

## Task 2: Connection helper

**Files:** Create `load/__init__.py` (empty), `load/databricks.py`

- [ ] **Step 1: Create `load/__init__.py`** (empty file)

- [ ] **Step 2: Write `load/databricks.py`**

```python
"""Open a Databricks SQL connection from environment variables.

Reuses the same DBX_* values proven by `dbt debug`. Call load_dotenv() in the
entrypoint first.
"""

from __future__ import annotations

import os

from databricks import sql


def connect():
    return sql.connect(
        server_hostname=os.environ["DBX_HOST"],
        http_path=os.environ["DBX_HTTP_PATH"],
        access_token=os.environ["DBX_TOKEN"],
        catalog=os.environ.get("DBX_CATALOG", "workspace"),
        schema=os.environ.get("DBX_SCHEMA", "default"),
    )
```

This thin wrapper is validated live in Task 4 (the spike), not unit-tested.

- [ ] **Step 3: Commit**

```powershell
git add load/__init__.py load/databricks.py
git commit -m "feat: Databricks SQL connection helper"
```

---

## Task 3: Loader pure logic (TDD)

The connection-free parts (DDL, INSERT SQL, DataFrame→rows) are unit-tested; the
execution is exercised live in Tasks 4–5.

**Files:** Create `load/load_to_delta.py`, `tests/test_load_to_delta.py`

- [ ] **Step 1: Write the failing test**

`tests/test_load_to_delta.py`:
```python
import pandas as pd

from load.load_to_delta import COLUMNS, bronze_ddl, insert_sql, rows_from_df


def test_columns_match_flatten_output():
    expected = {
        "business_date", "order_guid", "opened_date", "closed_date", "source",
        "dining_option_guid", "num_guests", "num_checks", "net_amount",
        "total_amount", "tax_amount", "tip_amount", "voided", "deleted",
    }
    assert set(COLUMNS) == expected


def test_bronze_ddl_lists_every_column():
    ddl = bronze_ddl("bronze_orders")
    assert "CREATE TABLE IF NOT EXISTS bronze_orders" in ddl
    assert "USING DELTA" in ddl
    for col in COLUMNS:
        assert col in ddl


def test_insert_sql_has_one_placeholder_per_column():
    sql = insert_sql("bronze_orders")
    assert sql.count("?") == len(COLUMNS)
    assert "INSERT INTO bronze_orders" in sql


def test_rows_from_df_are_tuples_in_column_order():
    df = pd.DataFrame(
        [{c: i for i, c in enumerate(COLUMNS)}]  # one row, value == column index
    )
    rows = rows_from_df(df)
    assert rows == [tuple(range(len(COLUMNS)))]
```

- [ ] **Step 2: Run to verify it fails**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_load_to_delta.py -q
```
Expected: FAIL — `ModuleNotFoundError: No module named 'load.load_to_delta'`.

- [ ] **Step 3: Write the implementation**

`load/load_to_delta.py`:
```python
"""Load order Parquet partitions into a Databricks Delta bronze table.

Idempotent per business date: each day's rows are deleted then re-inserted, so
re-running a date range never duplicates.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

COLUMNS = [
    "business_date", "order_guid", "opened_date", "closed_date", "source",
    "dining_option_guid", "num_guests", "num_checks", "net_amount",
    "total_amount", "tax_amount", "tip_amount", "voided", "deleted",
]

_DDL_TYPES = {
    "business_date": "BIGINT", "order_guid": "STRING", "opened_date": "STRING",
    "closed_date": "STRING", "source": "STRING", "dining_option_guid": "STRING",
    "num_guests": "INT", "num_checks": "INT", "net_amount": "DOUBLE",
    "total_amount": "DOUBLE", "tax_amount": "DOUBLE", "tip_amount": "DOUBLE",
    "voided": "BOOLEAN", "deleted": "BOOLEAN",
}


def bronze_ddl(table: str) -> str:
    cols = ",\n  ".join(f"{c} {_DDL_TYPES[c]}" for c in COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA"


def insert_sql(table: str) -> str:
    placeholders = ", ".join(["?"] * len(COLUMNS))
    cols = ", ".join(COLUMNS)
    return f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"


def rows_from_df(df: pd.DataFrame) -> list[tuple]:
    return [tuple(r) for r in df[COLUMNS].itertuples(index=False, name=None)]


def load_day(cursor, table: str, business_date: int, df: pd.DataFrame) -> int:
    cursor.execute(f"DELETE FROM {table} WHERE business_date = ?", [int(business_date)])
    if len(df):
        cursor.executemany(insert_sql(table), rows_from_df(df))
    return len(df)


def load_parquet_root(conn, root: str | Path, table: str = "bronze_orders") -> int:
    cursor = conn.cursor()
    cursor.execute(bronze_ddl(table))
    total = 0
    for part in sorted(Path(root).glob("business_date=*")):
        business_date = int(part.name.split("=")[1])
        df = pd.read_parquet(part / "orders.parquet")
        total += load_day(cursor, table, business_date, df)
    cursor.close()
    return total
```

- [ ] **Step 4: Run to verify it passes**

Run:
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_load_to_delta.py -q
```
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```powershell
git add load/load_to_delta.py tests/test_load_to_delta.py
git commit -m "feat: bronze loader pure logic (DDL, insert, rows) + tests"
```

---

## Task 4: Load-mechanism spike *(live — verifies the connector against Free Edition)*

Before trusting the loader, confirm the real connector can create a Delta table,
`executemany`-insert with `?` placeholders, and read back. **If the `?` paramstyle
fails here, that's the one thing to adjust** in `insert_sql`/`load_day` (e.g. to
`%s`) before Task 5.

**Files:** none (throwaway table).

- [ ] **Step 1: Run the spike**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
.\.venv\Scripts\python.exe -c "from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('CREATE TABLE IF NOT EXISTS _spike_orders (business_date BIGINT, order_guid STRING) USING DELTA'); cur.execute('DELETE FROM _spike_orders'); cur.executemany('INSERT INTO _spike_orders (business_date, order_guid) VALUES (?, ?)', [(20260626,'a'),(20260626,'b')]); cur.execute('SELECT count(*) FROM _spike_orders'); print('rows:', cur.fetchone()[0]); cur.execute('DROP TABLE _spike_orders'); cur.close(); c.close()"
```
Expected: prints `rows: 2`. If it errors on the INSERT placeholders, switch `?` → the connector's paramstyle in `load/load_to_delta.py` and re-run the spike.

---

## Task 5: Load the real Parquet into bronze *(live)*

**Files:** Create `load/run_load.py` (thin CLI)

- [ ] **Step 1: Write `load/run_load.py`**

```python
"""Load all order Parquet partitions into the Delta bronze table.

Usage:
    python -m load.run_load
"""

from __future__ import annotations

from dotenv import load_dotenv

from load.databricks import connect
from load.load_to_delta import load_parquet_root


def main() -> None:
    load_dotenv()
    conn = connect()
    try:
        total = load_parquet_root(conn, "data/raw/orders")
    finally:
        conn.close()
    print(f"Loaded {total} rows into bronze_orders")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

```powershell
.\.venv\Scripts\python.exe -m load.run_load
```
Expected: `Loaded <N> rows into bronze_orders` (N matches the Parquet row count from Plan 2's smoke run, e.g. 293).

- [ ] **Step 3: Verify bronze in Databricks matches the Parquet**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
.\.venv\Scripts\python.exe -c "from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('SELECT business_date, count(*), round(sum(total_amount),2) FROM bronze_orders GROUP BY business_date ORDER BY business_date'); [print(r) for r in cur.fetchall()]; cur.close(); c.close()"
```
Expected: per-day counts/totals match the Parquet table from Plan 2 (e.g. 20260625 → 109 rows).

- [ ] **Step 4: Re-run to prove idempotency**

Run `python -m load.run_load` again, then re-run Step 3. Expected: identical counts (no duplicates — the per-date DELETE+INSERT worked).

- [ ] **Step 5: Commit**

```powershell
git add load/run_load.py
git commit -m "feat: CLI to load order Parquet into Delta bronze"
```

---

## Task 6: dbt staging + daily sales mart

**Files:**
- Create: `models/staging/_sources.yml`, `models/staging/stg_orders.sql`
- Create: `models/marts/fct_daily_sales.sql`, `models/marts/_models.yml`
- Create: `tests/assert_non_negative_net_sales.sql`

- [ ] **Step 1: Declare the bronze source — `models/staging/_sources.yml`**

```yaml
version: 2

sources:
  - name: raw
    database: "{{ env_var('DBX_CATALOG', 'workspace') }}"
    schema: "{{ env_var('DBX_SCHEMA', 'default') }}"
    tables:
      - name: bronze_orders
```

- [ ] **Step 2: `models/staging/stg_orders.sql`** (type, exclude voids)

```sql
with source as (
    select * from {{ source('raw', 'bronze_orders') }}
)

select
    cast(business_date as int)        as business_date,
    order_guid,
    cast(opened_date as timestamp)    as opened_at,
    source                            as order_source,
    dining_option_guid,
    num_guests,
    num_checks,
    net_amount,
    total_amount,
    tax_amount,
    tip_amount
from source
where not voided and not deleted
```

- [ ] **Step 3: `models/marts/fct_daily_sales.sql`** (one row per business date)

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    business_date,
    count(*)                         as order_count,
    sum(num_guests)                  as guest_count,
    round(sum(net_amount), 2)        as net_sales,
    round(sum(total_amount), 2)      as total_sales,
    round(sum(tax_amount), 2)        as tax,
    round(sum(tip_amount), 2)        as tips
from orders
group by business_date
```

- [ ] **Step 4: Tests — `models/marts/_models.yml`**

```yaml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_guid
        data_tests: [not_null, unique]
      - name: business_date
        data_tests: [not_null]
  - name: fct_daily_sales
    columns:
      - name: business_date
        data_tests: [not_null, unique]
      - name: order_count
        data_tests: [not_null]
```

- [ ] **Step 5: Singular test — `tests/assert_non_negative_net_sales.sql`**

```sql
-- fails if any day has negative net sales
select business_date, net_sales
from {{ ref('fct_daily_sales') }}
where net_sales < 0
```

- [ ] **Step 6: Commit**

```powershell
git add models/staging/_sources.yml models/staging/stg_orders.sql models/marts/fct_daily_sales.sql models/marts/_models.yml tests/assert_non_negative_net_sales.sql
git commit -m "feat: dbt stg_orders + fct_daily_sales mart with data tests"
```

---

## Task 7: Build the models and run tests *(live)*

**Files:** none.

- [ ] **Step 1: Load env + point dbt at the local profile**

```powershell
Set-Location 'E:\PyProj\restaurant-forecast-analytics'
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim() }
$env:DBT_PROFILES_DIR = (Get-Location).Path
```

- [ ] **Step 2: `dbt build`** (runs models + tests)

```powershell
.\.venv\Scripts\dbt.exe build
```
Expected: `stg_orders` and `fct_daily_sales` build; all tests PASS. If schema-creation is denied, set `DBX_SCHEMA=default` (the schema you own) so dbt writes into `default_staging`/`default_marts` under your privileges — or grant CREATE SCHEMA on the catalog.

- [ ] **Step 3: Sanity-check the mart**

```powershell
.\.venv\Scripts\dbt.exe show --inline "select * from {{ ref('fct_daily_sales') }} order by business_date" --limit 10
```
Expected: one row per business date with sensible `net_sales`/`order_count`/`guest_count` matching the bronze totals (e.g. 20260625). *(No commit — dbt artifacts are gitignored.)*

---

## Follow-on (Plan 4): reconciliation vs. Toast's report

Not built here — it needs an **independent source of truth**: the owner exports Toast Web → **Sales Summary** for the loaded days. Plan 4 adds a `seeds/toast_sales_summary.csv` (business_date, reported_net_sales) and a singular dbt test asserting `abs(fct_daily_sales.net_sales − reported_net_sales) <= tolerance` per day — which is also where we'll nail the exact **net-sales definition** (pre-tax, discounts/comps/gift-cards handling) by matching Toast's number.

---

## Self-Review (completed during authoring)

- **Spec coverage:** §10 marts (`fct_daily_sales`, businessDate grain) ✓; §16 M2 (load + transform) ✓; §13 dbt tests (not_null/unique/non-negative) ✓ partial — the **reconciliation** test (§13, §18 #2) is explicitly deferred to Plan 4 (needs the Toast Sales Summary export), with the mechanism described. Forecast/dashboard (§11, §18 #3–4) = Plan 4.
- **Placeholder scan:** none — all code/commands are complete. The one live unknown (connector paramstyle) is isolated to the Task 4 spike with an explicit fix path.
- **Type consistency:** `COLUMNS` (Task 3) is the single source of column order, used by `bronze_ddl`, `insert_sql`, `rows_from_df`, and `load_day`; the dbt source/`stg_orders` reference the same 14 columns; `connect()` (Task 2) is used by `run_load.py` (Task 5) and the spike (Task 4).
- **Grounded:** bronze schema = `ingest.orders.flatten_order` output; `DBX_*` connection proven by `dbt debug` in Plan 1.
