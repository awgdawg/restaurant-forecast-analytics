# Parquet Writer Schema Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `orders.parquet` the pipeline writes (and every one already on the Volume) carries one explicit, bronze-mirroring pyarrow schema, so typed/inferred reads never rescue-and-NULL columns again — and the `intraday_today` rescue coalesce can be deleted.

**Architecture:** The writer bug is type drift: `ingest/orders.py` money helpers return `int` when sums are empty/integral (`round(int, n)` stays `int`), and `pandas.to_parquet` inference in `ingest/extract.py` then writes INT64 money columns on those days, DOUBLE on others. Typed reads over the multi-file dataset fail per-file and rescue the column (observed live 2026-07-31: `deferred_amount` rescued on 70,260 rows / 718 of 796 days; `net/total/tax/tip_amount` on 9 rows of one all-integral day). Fix at four levels: (1) explicit `ORDERS_SCHEMA` at the write (`pa.Table.from_pylist` + `pq.write_table`, no inference), (2) `float()` on the money helpers (defense in depth), (3) a written-parquet-schema regression test, (4) one-off rewrite of the ~796 existing local partitions + Volume re-upload + `:v2` container for both Cloud Run jobs, after which the `intraday_today` rescue coalesce is removed.

**Tech Stack:** pyarrow (already a dependency, >=17), pytest, Databricks CLI (`fs cp` dir-copy), gcloud (Cloud Build + Cloud Run Jobs), dbt via dbtRunner.

**Repo root:** `E:\PyProj\restaurant-forecast-analytics`. **Branch:** `parquet-schema-fix` (cut from `cloud-extraction`, which holds `intraday_today.sql` and is itself gated from main by the Phase 1 parity window — this branch merges back into `cloud-extraction` at the end and rides the same gate to main). Suite baseline **104 passed, 1 skipped**. NEVER echo secret values; SQL-connector scripts run via PowerShell script files with `load_dotenv` (never Git Bash — MSYS mangles `DBX_HTTP_PATH`).

**Live baseline (spiked 2026-07-31 13:50 CT, read-only):** `read_files` over `/Volumes/workspace/default/raw_orders/` shows 78,721 rows / 796 days, `_rescued_data` non-null on **all** rows — the hive `business_date` dir/file collision rescues the in-file copy on every row, independent of this bug. Consequence: the verification target is **zero rescued values among the 14 non-`business_date` columns**, not a bare `_rescued_data IS NOT NULL = 0` (unachievable while files legitimately carry `business_date`, which bronze's loader `load/load_to_delta.py::rows_from_df` requires). Baseline bug-class counts to drive to zero: `deferred_amount` 70,260; `net_amount`/`total_amount`/`tax_amount`/`tip_amount` 9 each; `num_guests`/`num_checks` 0.

**Operational context:** It is Friday during service hours — the `toast-sync-today` poller (image `:v1`) rewrites today's Volume partition every 15 min, so today's partition self-heals within one poll cycle after the jobs move to `:v2`. The PC Task Scheduler entry (08:45, `scripts/morning_extract.ps1`) runs whatever branch is checked out on E: — **keep this branch (or `cloud-extraction` after the merge-back) checked out**, or tomorrow's PC extract re-writes 3 partitions with the old inference and re-breaks the Volume.

---

### Task 1: `ORDERS_SCHEMA` in `ingest/orders.py`, mirroring the bronze DDL

**Files:**
- Modify: `ingest/orders.py`
- Modify: `load/load_to_delta.py` (cross-ref comment only)
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing test** — append to `tests/test_orders.py`:

```python
def test_orders_schema_mirrors_bronze_ddl():
    """ORDERS_SCHEMA and load_to_delta's bronze DDL must agree by construction:
    same columns, same order, corresponding types. INT is widened to int64 in
    parquet (safe: bronze ingests via SQL INSERT, not direct parquet load)."""
    import pyarrow as pa

    from ingest.orders import ORDERS_SCHEMA
    from load.load_to_delta import COLUMNS, _DDL_TYPES

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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_orders.py::test_orders_schema_mirrors_bronze_ddl -v`
Expected: FAIL — `ImportError: cannot import name 'ORDERS_SCHEMA'`.

- [ ] **Step 3: Implement** — in `ingest/orders.py`, add below the imports (`from __future__ import annotations`):

```python
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
```

And in `load/load_to_delta.py`, extend the comment position above `_DDL_TYPES = {` by inserting this line directly before it:

```python
# Mirrored by ingest/orders.py::ORDERS_SCHEMA (parquet) -- keep in sync;
# tests/test_orders.py::test_orders_schema_mirrors_bronze_ddl enforces it.
```

- [ ] **Step 4: Run the test again**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v`
Expected: all PASS (existing 6 + the new one).

- [ ] **Step 5: Commit**

```bash
git add ingest/orders.py load/load_to_delta.py tests/test_orders.py
git commit -m "feat: ORDERS_SCHEMA -- explicit parquet schema mirroring bronze DDL"
```

(Every commit in this plan ends with the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer via a Bash heredoc, per repo convention.)

### Task 2: `float()` defense in the money helpers

**Files:**
- Modify: `ingest/orders.py:14-42` (`_deferred_amount`, `_sum`, `tip_amount`)
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests** — append to `tests/test_orders.py`:

```python
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v -k floats`
Expected: both FAIL with `AssertionError` (int is not float).

- [ ] **Step 3: Implement** — in `ingest/orders.py`, wrap the three return expressions in `float()`:

In `_deferred_amount` (line 16): `return round(...)` becomes

```python
    return float(
        round(
            sum(
                (s.get("price") or 0.0)
                for c in live_checks
                for s in (c.get("selections") or [])
                if s.get("deferred") and not s.get("voided")
            ),
            4,
        )
    )
```

In `flatten_order`'s `_sum` (line 33):

```python
    def _sum(field: str) -> float:
        return float(round(sum((c.get(field) or 0.0) for c in live_checks), 4))
```

And `tip_amount` (line 35):

```python
    tip_amount = float(
        round(
            sum(
                (p.get("tipAmount") or 0.0)
                for c in live_checks
                for p in (c.get("payments") or [])
            ),
            4,
        )
    )
```

- [ ] **Step 4: Run the whole orders test file**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_orders.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add ingest/orders.py tests/test_orders.py
git commit -m "fix: money helpers always return float (round(int, n) stays int)"
```

### Task 3: explicit-schema parquet write in `ingest/extract.py`

**Files:**
- Modify: `ingest/extract.py:17,111-116`
- Test: `tests/test_extract.py`

- [ ] **Step 1: Write the failing test** — append to `tests/test_extract.py` (reuses the module's `FakeClient`):

```python
def test_written_parquet_schema_is_stable_across_day_shapes(tmp_path):
    """THE regression test for the 2026-07-31 type-drift bug: a value-only test
    passes straight through it. Day 1 has integral amounts, zero deferred
    selections, and no num_guests (the shapes that historically produced INT64
    money columns and a null-typed num_guests); day 2 is a normal float day.
    Both files must carry the identical explicit schema."""
    import pyarrow.parquet as pq

    from ingest.orders import ORDERS_SCHEMA

    int_day = {
        "guid": "o-int",
        "businessDate": 20260625,
        "checks": [{"amount": 10, "totalAmount": 11, "taxAmount": 1}],
    }
    float_day = {
        "guid": "o-float",
        "businessDate": 20260626,
        "numberOfGuests": 2,
        "checks": [
            {
                "amount": 10.5,
                "totalAmount": 11.55,
                "taxAmount": 1.05,
                "selections": [{"price": 25.0, "deferred": True}],
            }
        ],
    }
    client = FakeClient({"20260625": [int_day], "20260626": [float_day]})

    extract_range(client, date(2026, 6, 25), date(2026, 6, 26), tmp_path)

    for bd in ("20260625", "20260626"):
        schema = pq.read_schema(tmp_path / f"business_date={bd}" / "orders.parquet")
        assert schema.equals(ORDERS_SCHEMA), f"{bd} drifted:\n{schema}"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_extract.py::test_written_parquet_schema_is_stable_across_day_shapes -v`
Expected: FAIL on `20260625` — inferred schema has INT64 money columns (and a null/float `num_guests`), not `ORDERS_SCHEMA`. (Note: Task 2's `float()` fix alone must NOT make this pass — `num_guests` on day 1 is still all-None and would still infer null-typed. If it unexpectedly passes, stop and investigate before proceeding.)

- [ ] **Step 3: Implement** — in `ingest/extract.py`:

Replace the imports block at lines 17-22 (drop pandas, add pyarrow):

```python
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.orders import ORDERS_SCHEMA, flatten_order
from ingest.toast_client import ToastClient
```

Replace the write at line 116 (`pd.DataFrame(rows).to_parquet(target, index=False)`):

```python
        # Explicit schema, never inference: inferred writes drift INT64/DOUBLE
        # per day and typed multi-file reads then rescue-NULL whole columns
        # (models/marts/intraday_today.sql has the post-mortem).
        pq.write_table(pa.Table.from_pylist(rows, schema=ORDERS_SCHEMA), target)
```

- [ ] **Step 4: Run the extract tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_extract.py -v`
Expected: all PASS (the existing three still read their frames fine via `pd.read_parquet`).

- [ ] **Step 5: Full suite**

Run: `.\.venv\Scripts\python.exe -m pytest -q`
Expected: **108 passed, 1 skipped** (104 + 4 new).

- [ ] **Step 6: Commit**

```bash
git add ingest/extract.py tests/test_extract.py
git commit -m "fix: write order parquet with explicit ORDERS_SCHEMA, not inference"
```

### Task 4: one-off archive rewriter `ingest/rewrite_archive.py`

**Files:**
- Create: `ingest/rewrite_archive.py`
- Test: `tests/test_rewrite_archive.py`

- [ ] **Step 1: Write the failing tests** — create `tests/test_rewrite_archive.py`:

```python
"""The rewriter reads legacy inference-typed partitions and rewrites them
in place with ORDERS_SCHEMA. Legacy shapes covered: INT64 money columns
(integral-sum days), float64 num_guests (pandas int+None promotion),
null-typed columns (all-None days). Values must be preserved exactly."""

import pandas as pd
import pyarrow.parquet as pq

from ingest.orders import ORDERS_SCHEMA
from ingest.rewrite_archive import rewrite_partition, rewrite_root


def _legacy_row(**overrides):
    row = {
        "business_date": 20260625,
        "order_guid": "o1",
        "opened_date": "2026-06-25T17:00:00.000+0000",
        "closed_date": "2026-06-25T17:30:00.000+0000",
        "source": "In Store",
        "dining_option_guid": None,
        "num_guests": None,
        "num_checks": 1,
        "net_amount": 10,
        "total_amount": 11,
        "tax_amount": 1,
        "tip_amount": 0,
        "deferred_amount": 0,
        "voided": False,
        "deleted": False,
    }
    row.update(overrides)
    return row


def _write_legacy(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_rewrite_partition_coerces_legacy_types_and_preserves_values(tmp_path):
    target = tmp_path / "business_date=20260625" / "orders.parquet"
    _write_legacy(target, [_legacy_row()])
    assert not pq.read_schema(target).equals(ORDERS_SCHEMA)  # proves fixture is legacy

    assert rewrite_partition(target) is True

    assert pq.read_schema(target).equals(ORDERS_SCHEMA)
    row = pq.read_table(target).to_pylist()[0]
    assert row["net_amount"] == 10.0 and isinstance(row["net_amount"], float)
    assert row["deferred_amount"] == 0.0
    assert row["num_guests"] is None
    assert row["dining_option_guid"] is None
    assert row["order_guid"] == "o1"
    assert row["voided"] is False


def test_rewrite_partition_skips_already_conforming_files(tmp_path):
    target = tmp_path / "business_date=20260626" / "orders.parquet"
    _write_legacy(target, [_legacy_row(business_date=20260626)])
    rewrite_partition(target)
    mtime = target.stat().st_mtime_ns

    assert rewrite_partition(target) is False  # idempotent: no second write
    assert target.stat().st_mtime_ns == mtime


def test_rewrite_root_reports_rewritten_partitions(tmp_path):
    legacy = tmp_path / "business_date=20260625" / "orders.parquet"
    _write_legacy(legacy, [_legacy_row()])
    conforming = tmp_path / "business_date=20260626" / "orders.parquet"
    _write_legacy(conforming, [_legacy_row(business_date=20260626)])
    rewrite_partition(conforming)

    rewritten = rewrite_root(tmp_path)

    assert rewritten == ["business_date=20260625"]
    assert pq.read_schema(legacy).equals(ORDERS_SCHEMA)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_rewrite_archive.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingest.rewrite_archive'`.

- [ ] **Step 3: Implement** — create `ingest/rewrite_archive.py`:

```python
"""One-off: rewrite legacy inference-typed order partitions to ORDERS_SCHEMA.

Historical context (2026-07-31): pandas.to_parquet inference wrote INT64 money
columns on integral-sum days and DOUBLE elsewhere, so typed multi-file reads
(read_files on the Volume) rescue-NULLed deferred_amount on 718 of 796 days.
This rewrites every local partition in place with the explicit schema; the
Volume copy is then refreshed with the databricks CLI dir-copy. Values are
preserved exactly (int->float widening only). Kept (not deleted after use)
because any future schema migration will need the same pattern.

Usage:
    python -m ingest.rewrite_archive              # data/raw/orders
    python -m ingest.rewrite_archive --root <dir>
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq

from ingest.orders import ORDERS_SCHEMA


def rewrite_partition(path: Path) -> bool:
    """Rewrite one orders.parquet in place iff its schema differs. A missing
    or extra column raises (KeyError from select) -- loud beats silent."""
    table = pq.read_table(path)
    if table.schema.equals(ORDERS_SCHEMA):
        return False
    table = table.select(ORDERS_SCHEMA.names).cast(ORDERS_SCHEMA)
    pq.write_table(table, path)
    return True


def rewrite_root(root: Path, log=None) -> list[str]:
    """Rewrite every partition under root; returns the rewritten dir names."""
    emit = log or (lambda _m: None)
    rewritten = []
    parts = sorted(root.glob("business_date=*"))
    for part in parts:
        if rewrite_partition(part / "orders.parquet"):
            rewritten.append(part.name)
            emit(f"rewrote {part.name}")
    emit(f"{len(rewritten)} of {len(parts)} partitions rewritten")
    return rewritten


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rewrite order parquet partitions to ORDERS_SCHEMA."
    )
    parser.add_argument("--root", default="data/raw/orders")
    args = parser.parse_args()
    rewrite_root(Path(args.root), log=print)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_rewrite_archive.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Full suite + hooks**

Run: `.\.venv\Scripts\python.exe -m pytest -q` → **111 passed, 1 skipped**. Pre-commit (gitleaks, ruff, ruff-format) runs on commit and must be green.

- [ ] **Step 6: Commit**

```bash
git add ingest/rewrite_archive.py tests/test_rewrite_archive.py
git commit -m "feat: archive rewriter -- coerce legacy partitions to ORDERS_SCHEMA"
```

### Task 5 (ops): deploy `:v2`, rewrite + re-upload the archive, verify live

No repo changes except plan checkboxes. Order matters: image first (stops new bad writes), then history.

- [ ] **Step 1: Build `:v2`** (Git Bash; gcloud PATH prelude):

```bash
export PATH="/c/Users/auglt/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin:$PATH"
cd /e/PyProj/restaurant-forecast-analytics
gcloud builds submit --tag "us-central1-docker.pkg.dev/restaurant-forecast-ops/rfa/toast-sync:v2" .
```

Expected: `STATUS: SUCCESS`.

- [ ] **Step 2: Point both jobs at `:v2`** (image only; args/secrets are preserved by `jobs update`):

```bash
gcloud run jobs update toast-sync --region us-central1 \
  --image "us-central1-docker.pkg.dev/restaurant-forecast-ops/rfa/toast-sync:v2"
gcloud run jobs update toast-sync-today --region us-central1 \
  --image "us-central1-docker.pkg.dev/restaurant-forecast-ops/rfa/toast-sync:v2"
```

Verify: `gcloud run jobs describe toast-sync-today --region us-central1 --format "value(template.template.containers[0].image)"` → `...:v2` (and same for `toast-sync`).

- [ ] **Step 3: Rewrite the local archive** (~796 partitions, minutes):

Run: `.\.venv\Scripts\python.exe -m ingest.rewrite_archive`
Expected: `~79x of 796 partitions rewritten` (a handful of float-day files may already conform only if pandas happened to write int64 `num_guests` — most days have at least one None `num_guests`, so expect nearly all rewritten). Spot-check one: a PowerShell one-liner reading `pq.read_schema` on the oldest partition equals `ORDERS_SCHEMA`.

- [ ] **Step 4: Re-upload the archive to the Volume** (PowerShell, `morning_extract.ps1`'s proven env-bridge + CLI pattern; values never printed; ~796 dirs, allow many minutes):

```powershell
Set-Location E:\PyProj\restaurant-forecast-analytics
Get-Content .env | ForEach-Object {
    if ($_ -match '^([A-Z_]+)=(.*)$') { Set-Item -Path "env:$($Matches[1])" -Value $Matches[2] }
}
$env:DATABRICKS_HOST = "https://$($env:DBX_HOST)"
$env:DATABRICKS_TOKEN = $env:DBX_TOKEN
$dbx = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\databricks.exe"
& $dbx fs cp -r --overwrite "data\raw\orders" "dbfs:/Volumes/workspace/default/raw_orders"
if ($LASTEXITCODE -ne 0) { throw "archive upload failed" }
```

(The local root has no `business_date=20260731` dir, so today's live partition is untouched — it self-heals via the `:v2` poller, Step 5.)

- [ ] **Step 5: Today's partition on `:v2`** — during service hours the next scheduled poll (≤15 min) rewrites it; otherwise force one: `gcloud run jobs execute toast-sync-today --region us-central1 --wait` (business date stays "today" until midnight CT, so a post-close run still rewrites today's file). Expected: execution COMPLETE.

- [ ] **Step 6: Live verification** (PowerShell script file + `load_dotenv`, repo pattern). Query over `read_files('/Volumes/workspace/default/raw_orders/', format => 'parquet', rescuedDataColumn => '_rescued_data')`:

```sql
SELECT
  sum(case when get_json_object(_rescued_data,'$.deferred_amount') is not null then 1 else 0 end) as deferred,
  sum(case when get_json_object(_rescued_data,'$.net_amount')      is not null then 1 else 0 end) as net,
  sum(case when get_json_object(_rescued_data,'$.total_amount')    is not null then 1 else 0 end) as total,
  sum(case when get_json_object(_rescued_data,'$.tax_amount')      is not null then 1 else 0 end) as tax,
  sum(case when get_json_object(_rescued_data,'$.tip_amount')      is not null then 1 else 0 end) as tip,
  sum(case when get_json_object(_rescued_data,'$.num_guests')      is not null then 1 else 0 end) as guests,
  sum(case when get_json_object(_rescued_data,'$.num_checks')      is not null then 1 else 0 end) as checks,
  sum(case when get_json_object(_rescued_data,'$.order_guid')      is not null then 1 else 0 end) as guid,
  count(*) as rows, count(distinct business_date) as days,
  sum(case when _rescued_data is not null then 1 else 0 end) as any_rescued
FROM ...
```

Expected: **every per-column count 0** (down from deferred=70,260, net/total/tax/tip=9). `rows` ≈ 78,7xx (today's orders keep landing), `days` = 797 (796 + today). Record `any_rescued`: if the now-type-consistent files stop colliding on `business_date` it will be 0 and the bare `_rescued_data IS NOT NULL` check from the task brief holds verbatim; if the dir/file collision persists it stays ≈ `rows` — either is a PASS for this fix (the collision predates it and `intraday_today` handles it); record which for Task 6's comment wording. Also sanity-check values survived the rewrite:

```sql
SELECT round(sum(net_amount - deferred_amount), 2) FROM read_files(...) WHERE NOT voided AND NOT deleted AND business_date < 20260731
```

vs the same expression over `bronze_orders` for the same date range — must match to the cent (the model comment's original parity method).

- [ ] **Step 7: Check the checkboxes above in this plan and commit** `docs: plan progress -- v2 image live, archive rewritten + re-uploaded`.

### Task 6: remove the rescue coalesce from `intraday_today` (exit condition met)

**Files:**
- Modify: `models/marts/intraday_today.sql`
- Modify: `docs/superpowers/plans/2026-07-31-cloud-extraction-phase2.md` (hourly-bar SQL, Task 3)

- [ ] **Step 1: Capture the view's current row** (PowerShell script file): `SELECT * FROM workspace.default_marts.intraday_today` → save the values for the post-change comparison.

- [ ] **Step 2: Edit the model.** In `models/marts/intraday_today.sql`:

Replace the comment block at lines 20-44 with (keep the dir/file-collision paragraph, retire the rescue story to a shorter as-built note; adjust the final sentence per Task 5 Step 6's observed `any_rescued`):

```sql
-- read_files over the partitioned root infers ONE business_date (from the
-- directory name, as INT); the copy inside each parquet file loses the
-- collision and is diverted to the rescued-data column. Values agree, so the
-- derived one is authoritative -- we only widen it to BIGINT to match
-- forecast_date.
--
-- HISTORY (2026-07-31): deferred_amount used to need a _rescued_data
-- coalesce here -- inference-typed writes made it INT64 on zero-deferred
-- days and DOUBLE elsewhere, so the typed read rescue-NULLed 718 of 796
-- days' files. Fixed at the writer (ingest/orders.py::ORDERS_SCHEMA,
-- explicit-schema writes in ingest/extract.py) and the whole archive was
-- rewritten + re-uploaded the same day, verified zero rescued values across
-- all data columns. If deferred_amount ever NULLs again, check for a writer
-- bypass writing inference-typed parquet before touching this view.
```

Replace the `raw_orders` CTE (lines 45-64) with:

```sql
raw_orders as (
    select
        cast(business_date as bigint) as business_date,
        order_guid,
        opened_date,
        num_guests,
        net_amount,
        deferred_amount,
        voided,
        deleted
    from read_files(
        '/Volumes/workspace/default/raw_orders/',
        format => 'parquet'
    )
),
```

(The `rescuedDataColumn` pin goes away with its only consumer; `read_files` still rescues the `business_date` file-copy under its default column name if the collision persists, which the view doesn't read.)

- [ ] **Step 3: Amend the Phase 2 plan's hourly-bar SQL** — in `docs/superpowers/plans/2026-07-31-cloud-extraction-phase2.md` Task 3, replace the `-- AMENDED per Task 1 spike:` comment and the `round(sum(net_amount) - sum(coalesce(...)), 2)` line with:

```sql
-- RE-AMENDED 2026-07-31 (parquet-schema-fix): the writer now pins an explicit
-- schema and the archive was rewritten, so deferred_amount reads typed on
-- every file -- no rescue needed.
SELECT hour(from_utc_timestamp(to_timestamp(opened_date), 'America/Chicago')) AS hr,
       round(sum(net_amount) - sum(deferred_amount), 2) AS net_sales
FROM read_files('/Volumes/workspace/default/raw_orders/', format => 'parquet')
WHERE business_date = cast(date_format(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 'yyyyMMdd') as bigint)
  AND NOT voided AND NOT deleted
GROUP BY 1 ORDER BY 1
```

(If the dashboard's optional hourly-bar dataset was already created from the old SQL it keeps working — the coalesce short-circuits on the now-always-typed column — but note in the final report that the user can paste the simplified SQL.)

- [ ] **Step 4: Rebuild the view** — repo dbtRunner pattern:

Run: `.\.venv\Scripts\python.exe -c "from dbt.cli.main import dbtRunner; r = dbtRunner().invoke(['build', '--profiles-dir', '.', '--select', 'intraday_today'])"`
Expected: PASS for the view + its 2 tests (not_null, unique).

- [ ] **Step 5: Verify the view's row** (PowerShell script file): `SELECT * FROM workspace.default_marts.intraday_today` → one row; `net_sales_so_far` consistent with Step 1's capture (equal, or moved by new orders since — compare against `order_count` movement; it must NOT drop to 0 or NULL).

- [ ] **Step 6: Full suite + commit**

```bash
git add models/marts/intraday_today.sql docs/superpowers/plans/2026-07-31-cloud-extraction-phase2.md
git commit -m "fix: drop intraday_today rescue coalesce -- writer schema fix landed"
```

### Task 7: finish

- [ ] **Step 1:** Full suite (`111 passed, 1 skipped`) + hooks green; `git log --oneline` shows the 5 commits.
- [ ] **Step 2:** Two-stage review per repo convention (spec-compliance then code-quality) over the whole branch diff `cloud-extraction..parquet-schema-fix`; fix anything found.
- [ ] **Step 3:** Merge `parquet-schema-fix` → `cloud-extraction` (local; `cloud-extraction` is still gated from main by the Phase 1 parity window — it carries this fix to main when the gate clears). Leave `cloud-extraction` checked out on E: so tomorrow's 08:45 PC extract runs the fixed writer (an old-branch checkout would re-write 3 partitions inference-typed and re-break the Volume).
- [ ] **Step 4:** Memory update (fix landed, v2 image live, verify result, merge state) + final report.

---

## Self-review (spec coverage)

- Fix part 1 (explicit pyarrow schema at the write, mirroring bronze DDL) → Tasks 1 + 3. ✅
- Fix part 2 (`float()` on the three money helpers) → Task 2. ✅
- Fix part 3 (regression test asserting the WRITTEN schema, int-day + all-None-guests day) → Task 3 Step 1 (one fixture day carries both shapes; `pq.read_schema` asserted). ✅
- Fix part 4 (archive rewrite → re-upload → v2 image both jobs → rescue removal LAST, verified) → Tasks 4-6 in that order; verify query refined per the live baseline spike (bare `IS NOT NULL → 0` is unachievable while files carry `business_date` — per-column rescued counts → 0 is the equivalent, honest target; both recorded). ✅
- Constraints: SQL via PowerShell script files + `load_dotenv` (Tasks 5-6), no secret echoing (env-bridge pattern prints nothing), suite green each task, TDD throughout, two-stage review (Task 7). ✅
- Coordination: cloud-extraction unmerged (parity gate open) → branch cut from it, merge back into it, gate untouched; PC-scheduler branch-checkout hazard documented. ✅
