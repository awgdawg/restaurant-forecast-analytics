# Databricks Cloud Pipeline — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land every cloud-pipeline piece that runs on the current **Free** workspace at $0: forecast results become **Delta tables** (`forecast_daily_sales`, `model_metrics`) plus a dbt **`forecast_vs_actuals`** view; the bronze loader becomes **incremental**; and the **Databricks Asset Bundle** (nightly Workflow as code) is authored and `bundle validate`d — ready to deploy the moment the paid workspace exists.

**Why:** Implements milestones **C1 + C2** of the [cloud-pipeline spec](../specs/2026-06-29-databricks-cloud-pipeline-design.md). Phase 2 (deploy, in-cloud extraction, schedule, AI/BI dashboard, Sheets→Tableau) is blocked on the user provisioning paid Databricks; nothing here is.

**Architecture:** A new `load/load_forecast.py` writes the two forecast tables into the default schema (same env-driven schema as `bronze_orders`, via the existing `load.databricks.connect()`); dbt declares them as a **source** and serves a marts **view** joining actuals to the latest forecast — dbt convention: tables dbt doesn't build are sources, and the marts-grade surface is the view (this refines spec §7's "(marts)" heading). The loader gains count-based incremental selection (the exact reconcile logic that caught the corrupted 2026-04-12 day) plus a trailing reload window, because Toast post-close edits can change values without changing row counts. A `pyproject.toml` gives the repo console entry points so the Asset Bundle can run the same tested code as `python_wheel_task`s.

**Tech Stack:** existing Python pkg (`ingest`/`load`/`forecast`), `databricks-sql-connector`, dbt-databricks, pytest, setuptools/`build`, Databricks CLI (Asset Bundles).

---

## Scope (Phase 1 of the cloud spec)

- **In scope (C1):** `load/load_forecast.py` (TDD); wire `forecast.run_forecast` to write both tables; dbt `forecast` source + `forecast_vs_actuals` view + tests.
- **In scope (C2):** incremental `load_parquet_root` (+ `--full-refresh`, `--root`, `--window` trailing reload); `--out-dir` + `--refresh-days` on extract; `pyproject.toml` with entry points; `databricks.yml` bundle authored + validated (schedule declared **PAUSED**).
- **Out of scope (Phase 2 / C3–C5):** deploying the bundle, UC Volume, secret scope, in-cloud extraction, unpausing the schedule, `publish.to_sheets`, AI/BI dashboard, Tableau reconnect, CI. CSV exports are **retained** during transition (spec §6).

**Repo root:** `E:\PyProj\restaurant-forecast-analytics` (PowerShell; `.\.venv\Scripts\python.exe`). `.env` has working DBX_* creds. Branch: `cloud-phase1`. Remote `origin` exists (public GitHub) — push after merge.

**Canonical shapes (used across tasks — keep exact):**
- `forecast_daily_sales` cols: `forecast_date` BIGINT (YYYYMMDD), `yhat`/`yhat_lower`/`yhat_upper` DOUBLE, `model` STRING, `run_ts` TIMESTAMP. **Overwritten** each run.
- `model_metrics` cols: `model` STRING, `mae`/`rmse`/`mape`/`wape` DOUBLE, `horizon`/`n_folds` INT, `run_ts` TIMESTAMP. **Append-only.**
- Forecast df in code: `ds` (Timestamp), `yhat` (+ `yhat_lower`/`yhat_upper` when Prophet; absent for baseline → NULLs in the table).

---

## Task 0: Branch

- [ ] **Step 1:**
```powershell
git checkout -b cloud-phase1
```

---

## Task 1: `load/load_forecast.py` — Delta writers for forecast + metrics (TDD)

**Files:** Create `load/load_forecast.py`, `tests/test_load_forecast.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_load_forecast.py`

```python
from datetime import datetime, timezone

import pandas as pd

from load.load_forecast import (
    FORECAST_COLUMNS,
    METRICS_COLUMNS,
    forecast_ddl,
    forecast_rows,
    metrics_ddl,
    metrics_rows,
    write_forecast,
    write_metrics,
)

RUN_TS = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


def test_ddls_list_every_column():
    fddl = forecast_ddl()
    for col in FORECAST_COLUMNS:
        assert col in fddl
    assert "USING DELTA" in fddl
    mddl = metrics_ddl()
    for col in METRICS_COLUMNS:
        assert col in mddl


def test_forecast_rows_convert_ds_and_null_missing_band():
    fc = pd.DataFrame(
        {"ds": pd.date_range("2026-06-29", periods=2, freq="D"), "yhat": [100.0, 200.0]}
    )  # baseline shape: no band columns

    rows = forecast_rows(fc, model="baseline", run_ts=RUN_TS)

    assert rows == [
        (20260629, 100.0, None, None, "baseline", RUN_TS),
        (20260630, 200.0, None, None, "baseline", RUN_TS),
    ]


def test_metrics_rows_one_per_model_in_column_order():
    metrics = {
        "baseline": {"mae": 434.0, "rmse": 605.0, "mape": 18.0, "wape": 16.6},
        "prophet": {"mae": 351.0, "rmse": 456.0, "mape": 31.6, "wape": 13.4},
    }

    rows = metrics_rows(metrics, horizon=14, n_folds=8, run_ts=RUN_TS)

    assert ("prophet", 351.0, 456.0, 31.6, 13.4, 14, 8, RUN_TS) in rows
    assert len(rows) == 2


def test_write_forecast_overwrites_then_inserts():
    fc = pd.DataFrame(
        {
            "ds": pd.date_range("2026-06-29", periods=2, freq="D"),
            "yhat": [100.0, 200.0],
            "yhat_lower": [90.0, 190.0],
            "yhat_upper": [110.0, 210.0],
        }
    )
    cur = FakeCursor()

    n = write_forecast(cur, fc, model="prophet", run_ts=RUN_TS)

    assert n == 2
    sqls = [s for s, _ in cur.calls]
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS forecast_daily_sales") for s in sqls)
    assert "DELETE FROM forecast_daily_sales" in sqls  # overwrite semantics
    insert_sql, params = cur.calls[-1]
    assert insert_sql.count("?") == 2 * len(FORECAST_COLUMNS)
    assert params[0] == 20260629 and params[1] == 100.0


def test_write_metrics_appends_without_delete():
    cur = FakeCursor()

    n = write_metrics(
        cur,
        {"prophet": {"mae": 1.0, "rmse": 2.0, "mape": 3.0, "wape": 4.0}},
        horizon=14,
        n_folds=8,
        run_ts=RUN_TS,
    )

    assert n == 1
    sqls = [s for s, _ in cur.calls]
    assert not any(s.startswith("DELETE") for s in sqls)  # append-only
    assert cur.calls[-1][0].count("?") == len(METRICS_COLUMNS)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_load_forecast.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'load.load_forecast'`.

- [ ] **Step 3: Implement** — `load/load_forecast.py`

```python
"""Write forecast outputs to Delta: forecast_daily_sales + model_metrics.

forecast_daily_sales is overwritten each run (it holds only the latest
horizon); model_metrics is append-only (a history of backtest results).
Tables land in the connection's default schema, alongside bronze_orders.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

FORECAST_TABLE = "forecast_daily_sales"
METRICS_TABLE = "model_metrics"

_FORECAST_TYPES = {
    "forecast_date": "BIGINT",
    "yhat": "DOUBLE",
    "yhat_lower": "DOUBLE",
    "yhat_upper": "DOUBLE",
    "model": "STRING",
    "run_ts": "TIMESTAMP",
}
FORECAST_COLUMNS = list(_FORECAST_TYPES)

_METRICS_TYPES = {
    "model": "STRING",
    "mae": "DOUBLE",
    "rmse": "DOUBLE",
    "mape": "DOUBLE",
    "wape": "DOUBLE",
    "horizon": "INT",
    "n_folds": "INT",
    "run_ts": "TIMESTAMP",
}
METRICS_COLUMNS = list(_METRICS_TYPES)


def _ddl(table: str, types: dict[str, str]) -> str:
    cols = ",\n  ".join(f"{c} {t}" for c, t in types.items())
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA"


def forecast_ddl(table: str = FORECAST_TABLE) -> str:
    return _ddl(table, _FORECAST_TYPES)


def metrics_ddl(table: str = METRICS_TABLE) -> str:
    return _ddl(table, _METRICS_TYPES)


def _insert_sql(table: str, columns: list[str], n_rows: int) -> str:
    row = "(" + ", ".join(["?"] * len(columns)) + ")"
    values = ", ".join([row] * n_rows)
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values}"


def forecast_rows(fc: pd.DataFrame, model: str, run_ts: datetime) -> list[tuple]:
    """fc: df[ds, yhat(, yhat_lower, yhat_upper)] -> FORECAST_COLUMNS-ordered rows.
    Baseline forecasts carry no band; missing columns become SQL NULLs."""
    out = fc.reindex(columns=["ds", "yhat", "yhat_lower", "yhat_upper"])
    rows = []
    for r in out.itertuples(index=False):
        vals = [None if pd.isna(v) else float(v) for v in (r.yhat, r.yhat_lower, r.yhat_upper)]
        rows.append((int(r.ds.strftime("%Y%m%d")), *vals, model, run_ts))
    return rows


def metrics_rows(
    metrics_by_model: dict, horizon: int, n_folds: int, run_ts: datetime
) -> list[tuple]:
    return [
        (name, m["mae"], m["rmse"], m["mape"], m["wape"], horizon, n_folds, run_ts)
        for name, m in metrics_by_model.items()
    ]


def write_forecast(
    cursor, fc: pd.DataFrame, model: str, run_ts: datetime, table: str = FORECAST_TABLE
) -> int:
    """Replace the table's contents with this run's horizon (latest-only)."""
    cursor.execute(forecast_ddl(table))
    cursor.execute(f"DELETE FROM {table}")
    rows = forecast_rows(fc, model, run_ts)
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, FORECAST_COLUMNS, len(rows)), params)
    return len(rows)


def write_metrics(
    cursor, metrics_by_model: dict, horizon: int, n_folds: int, run_ts: datetime,
    table: str = METRICS_TABLE,
) -> int:
    """Append this run's backtest metrics (keeps history across runs)."""
    cursor.execute(metrics_ddl(table))
    rows = metrics_rows(metrics_by_model, horizon, n_folds, run_ts)
    params = [v for row in rows for v in row]
    cursor.execute(_insert_sql(table, METRICS_COLUMNS, len(rows)), params)
    return len(rows)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_load_forecast.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```powershell
git add load/load_forecast.py tests/test_load_forecast.py
git commit -m "feat: Delta writers for forecast_daily_sales + model_metrics"
```

---

## Task 2: Wire `run_forecast` to write the tables (live-verified)

**Files:** Modify `forecast/run_forecast.py`

- [ ] **Step 1: Add imports** — in `forecast/run_forecast.py`, extend the import block:

```python
from datetime import datetime, timezone
```
and (with the other `load.` import):
```python
from load.load_forecast import write_forecast, write_metrics
```

- [ ] **Step 2: Write tables after the CSV export** — replace the final three lines of `main()` (the closing `fva` / `write_exports` / `print` block):

```python
    fva = build_forecast_vs_actuals(series, forecast, model_name=winner)
    write_exports(fva, build_metrics_frame(metrics))
    print(f"Wrote exports/forecast_vs_actuals.csv ({len(fva)} rows) + exports/backtest_metrics.csv")
```
with:
```python
    fva = build_forecast_vs_actuals(series, forecast, model_name=winner)
    write_exports(fva, build_metrics_frame(metrics))
    print(f"Wrote exports/forecast_vs_actuals.csv ({len(fva)} rows) + exports/backtest_metrics.csv")

    run_ts = datetime.now(timezone.utc)
    conn = connect()
    try:
        cur = conn.cursor()
        n_fc = write_forecast(cur, forecast, winner, run_ts)
        n_m = write_metrics(cur, metrics, args.horizon, args.folds, run_ts)
        cur.close()
    finally:
        conn.close()
    print(f"Wrote {n_fc} rows to forecast_daily_sales, {n_m} rows to model_metrics")
```

- [ ] **Step 3: Full suite still green**

Run: `.\.venv\Scripts\python.exe -m pytest -q`
Expected: PASS (45 passed — 40 prior + 5 from Task 1).

- [ ] **Step 4: Live run + verify tables**

Run: `.\.venv\Scripts\python.exe -m forecast.run_forecast` (takes ~1–2 min; expect the familiar backtest lines, then `Wrote 14 rows to forecast_daily_sales, 2 rows to model_metrics`).
Then verify:
```powershell
.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('select count(*), min(forecast_date), max(forecast_date), any_value(model) from forecast_daily_sales'); print('forecast:', cur.fetchone()); cur.execute('select model, wape, run_ts from model_metrics order by run_ts desc limit 4'); [print('metrics:', r) for r in cur.fetchall()]; cur.close(); c.close()"
```
Expected: `forecast: (14, <tomorrow-ish YYYYMMDD>, <+14d>, 'prophet')` and two metrics rows (baseline + prophet) sharing one `run_ts`.

- [ ] **Step 5: Commit**

```powershell
git add forecast/run_forecast.py exports/forecast_vs_actuals.csv exports/backtest_metrics.csv
git commit -m "feat: run_forecast persists forecast + metrics to Delta tables"
```
(Include the two CSVs only if the live run changed them.)

---

## Task 3: dbt `forecast` source + `forecast_vs_actuals` view (live-verified)

**Files:** Modify `models/staging/_sources.yml`, `models/marts/_models.yml`; Create `models/marts/forecast_vs_actuals.sql`

- [ ] **Step 1: Declare the source** — append to `models/staging/_sources.yml` (matching the existing `raw` block's env_var pattern):

```yaml
  - name: forecast
    database: "{{ env_var('DBX_CATALOG', 'workspace') }}"
    schema: "{{ env_var('DBX_SCHEMA', 'default') }}"
    tables:
      - name: forecast_daily_sales
      - name: model_metrics
```

- [ ] **Step 2: Create the view** — `models/marts/forecast_vs_actuals.sql`:

```sql
{{ config(materialized='view') }}

with actuals as (
    select business_date, net_sales
    from {{ ref('fct_daily_sales') }}
),

forecast as (
    select
        forecast_date as business_date,
        yhat,
        yhat_lower,
        yhat_upper,
        model,
        run_ts
    from {{ source('forecast', 'forecast_daily_sales') }}
)

select
    coalesce(a.business_date, f.business_date)                as business_date,
    to_date(
        cast(coalesce(a.business_date, f.business_date) as string), 'yyyyMMdd'
    )                                                         as date_day,
    a.net_sales                                               as net_sales_actual,
    f.yhat,
    f.yhat_lower,
    f.yhat_upper,
    f.model,
    f.run_ts,
    (a.business_date is null and f.business_date is not null) as is_forecast
from actuals a
full outer join forecast f
    on a.business_date = f.business_date
```

(Marts default to `+materialized: table` in `dbt_project.yml`; the config line overrides to a view per spec §7. `forecast_daily_sales` holds only the latest horizon — it's overwritten each run — so no run_ts filtering is needed.)

- [ ] **Step 3: Add tests** — append to `models/marts/_models.yml`:

```yaml
  - name: forecast_vs_actuals
    columns:
      - name: business_date
        data_tests: [not_null, unique]
```

- [ ] **Step 4: Build + verify live**

Run:
```powershell
.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from dbt.cli.main import dbtRunner; import sys; r=dbtRunner().invoke(['build','--profiles-dir','.']); sys.exit(0 if r.success else 1)"
```
Expected: `Done. PASS=14 ... TOTAL=14` — 11 prior nodes (2 models + 1 seed + 8 tests) + 3 new (the view + its 2 tests) — incl. `OK created sql view model default_marts.forecast_vs_actuals`.
Then verify the view **relationally** (no hardcoded counts — closed days are absent from the mart, so view rows must equal mart days + the 14 forecast days):
```powershell
.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('select count(*) from workspace.default_marts.fct_daily_sales'); mart=cur.fetchone()[0]; cur.execute('select count(*), sum(cast(is_forecast as int)) from workspace.default_marts.forecast_vs_actuals'); total, fc = cur.fetchone(); print(f'mart={mart} view={total} forecast_rows={fc}'); assert total == mart + 14 and fc == 14; print('view = mart days + 14 forecast rows -- OK'); cur.close(); c.close()"
```
Expected: prints the three counts and `OK` (the assert enforces `view = mart + 14` and `forecast_rows = 14`).

- [ ] **Step 5: Commit**

```powershell
git add models/staging/_sources.yml models/marts/forecast_vs_actuals.sql models/marts/_models.yml
git commit -m "feat: forecast source + forecast_vs_actuals marts view"
```

---

## Task 4: Incremental bronze load (TDD)

The loader currently reloads **every** partition (minutes of DELETE+INSERT; an interruption left 2026-04-12 half-loaded). Make it load only days that are missing from bronze or whose row counts differ from the Parquet on disk — the same reconcile logic that found and repaired 4/12.

**Files:** Modify `load/load_to_delta.py`, `load/run_load.py`, `tests/test_load_to_delta.py`

- [ ] **Step 1: Write the failing tests** — append to `tests/test_load_to_delta.py`:

```python
from load.load_to_delta import days_needing_load, parquet_day_counts


def test_days_needing_load_flags_missing_and_mismatched_only():
    parquet = {20260626: 100, 20260627: 179, 20260628: 90, 20260412: 102}
    bronze = {20260626: 100, 20260627: 179, 20260412: 0}  # 4/12 partial, 6/28 missing

    todo = days_needing_load(parquet, bronze)

    assert todo == [20260412, 20260628]  # sorted; matching days skipped


def test_days_needing_load_window_forces_recent_days():
    parquet = {20260626: 100, 20260627: 179, 20260628: 90}
    bronze = dict(parquet)  # counts all match -> nothing stale by count

    assert days_needing_load(parquet, bronze) == []
    # a trailing window still reloads the most recent days: post-close edits
    # (refunds, tip adjustments) can change values without changing counts
    assert days_needing_load(parquet, bronze, window=2) == [20260627, 20260628]


def test_parquet_day_counts_reads_metadata(tmp_path):
    for bd, n in [(20260627, 3), (20260628, 2)]:
        part = tmp_path / f"business_date={bd}"
        part.mkdir(parents=True)
        pd.DataFrame({"order_guid": [f"o{i}" for i in range(n)]}).to_parquet(
            part / "orders.parquet", index=False
        )

    counts = parquet_day_counts(tmp_path)

    assert counts == {20260627: 3, 20260628: 2}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_load_to_delta.py -q`
Expected: FAIL — `ImportError: cannot import name 'days_needing_load'`.

- [ ] **Step 3: Implement** — in `load/load_to_delta.py`:

Add to the imports:
```python
import pyarrow.parquet as pq
```
Add above `load_parquet_root`:
```python
def parquet_day_counts(root: str | Path) -> dict[int, int]:
    """business_date -> row count on disk, via Parquet metadata (no data read)."""
    return {
        int(p.name.split("=")[1]): pq.read_metadata(p / "orders.parquet").num_rows
        for p in sorted(Path(root).glob("business_date=*"))
    }


def bronze_day_counts(cursor, table: str) -> dict[int, int]:
    cursor.execute(f"SELECT business_date, COUNT(*) FROM {table} GROUP BY business_date")
    return {int(bd): int(n) for bd, n in cursor.fetchall()}


def days_needing_load(
    parquet_counts: dict[int, int], bronze_counts: dict[int, int], window: int = 0
) -> list[int]:
    """Days on disk that bronze is missing or holds with a different row count
    (an interrupted DELETE+INSERT leaves a partial day; the mismatch catches it).
    window > 0 additionally forces the most recent `window` days on disk, since
    post-close edits (refunds, tip adjustments) can change values without
    changing row counts."""
    stale = {bd for bd, n in parquet_counts.items() if bronze_counts.get(bd) != n}
    if window:
        stale.update(sorted(parquet_counts)[-window:])
    return sorted(stale)
```
Replace `load_parquet_root` with:
```python
def load_parquet_root(
    conn, root: str | Path, table: str = "bronze_orders",
    full_refresh: bool = False, window: int = 0, log=None,
) -> int:
    emit = log or (lambda _msg: None)
    cursor = conn.cursor()
    cursor.execute(bronze_ddl(table))
    on_disk = parquet_day_counts(root)
    todo = (
        sorted(on_disk)
        if full_refresh
        else days_needing_load(on_disk, bronze_day_counts(cursor, table), window)
    )
    emit(f"{len(on_disk)} days on disk; loading {len(todo)}")
    total = 0
    for business_date in todo:
        df = pd.read_parquet(Path(root) / f"business_date={business_date}" / "orders.parquet")
        total += load_day(cursor, table, business_date, df)
        emit(f"{business_date}: {len(df)} rows")
    cursor.close()
    return total
```

- [ ] **Step 4: Update the CLI** — replace `load/run_load.py` contents:

```python
"""Load order Parquet partitions into the Delta bronze table.

Incremental by default: only days missing from bronze (or with mismatched row
counts) are loaded. Use --full-refresh to reload every day on disk.

Usage:
    python -m load.run_load [--root data/raw/orders] [--full-refresh] [--window N]
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from load.databricks import connect
from load.load_to_delta import load_parquet_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Load order Parquet into Delta bronze.")
    parser.add_argument(
        "--root", default="data/raw/orders",
        help="Parquet root (local path or /Volumes/... in-cloud)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="reload every day on disk"
    )
    parser.add_argument(
        "--window", type=int, default=0,
        help="always reload the most recent N days on disk (captures same-count edits)",
    )
    args = parser.parse_args()

    load_dotenv()
    conn = connect()
    try:
        total = load_parquet_root(
            conn, args.root, full_refresh=args.full_refresh, window=args.window, log=print
        )
    finally:
        conn.close()
    print(f"Loaded {total} rows into bronze_orders")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Verify — tests + a live no-op run**

Run: `.\.venv\Scripts\python.exe -m pytest -q`
Expected: PASS (48 passed).
Then live (bronze is current, so incremental should load nothing — and fast):
```powershell
.\.venv\Scripts\python.exe -m load.run_load
```
Expected output ends: `... days on disk; loading 0` / `Loaded 0 rows into bronze_orders`, in seconds not minutes.

- [ ] **Step 6: Commit**

```powershell
git add load/load_to_delta.py load/run_load.py tests/test_load_to_delta.py
git commit -m "feat: incremental bronze load (count-reconciled) + --full-refresh"
```

---

## Task 5: Packaging — `pyproject.toml` entry points + extract `--out-dir` / `--refresh-days`

The Asset Bundle runs pipeline steps as `python_wheel_task`s, which need a wheel with console entry points. The extract CLI also needs an output-path flag so Phase 2 can point it at a UC Volume.

**Files:** Create `pyproject.toml`; Modify `ingest/extract.py`, `.gitignore`

- [ ] **Step 1: Add `--out-dir` and `--refresh-days` to extract** — replace the whole `main()` function in `ingest/extract.py` with:

```python
def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Toast orders to Parquet.")
    parser.add_argument(
        "--start", help="YYYY-MM-DD; if omitted, auto-detect earliest day with orders"
    )
    parser.add_argument("--end", help="YYYY-MM-DD; default = yesterday")
    parser.add_argument(
        "--overwrite", action="store_true", help="re-fetch days already on disk"
    )
    parser.add_argument(
        "--out-dir", default="data/raw/orders",
        help="output root (local path or /Volumes/... in-cloud)",
    )
    parser.add_argument(
        "--refresh-days", type=int, default=0,
        help="re-pull the last N days with overwrite (captures post-close edits); "
        "overrides --start and auto-detect",
    )
    args = parser.parse_args()

    load_dotenv()
    client = ToastClient(load_toast_config())
    out_dir = Path(args.out_dir)

    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    overwrite = args.overwrite or args.refresh_days > 0
    if args.refresh_days:
        start = end - timedelta(days=args.refresh_days - 1)
    elif args.start:
        start = date.fromisoformat(args.start)
    else:
        print("Auto-detecting earliest business date with orders...")
        start = find_earliest_business_date(
            lambda d: _day_has_orders(client, d), date.today()
        )
        print(f"Detected start: {start.isoformat()}")

    print(f"Extracting {start.isoformat()} -> {end.isoformat()} into {out_dir}/")
    total = extract_range(
        client, start, end, out_dir, overwrite=overwrite, log=print
    )
    print(f"Wrote {total} order rows under {out_dir}")
```
Also add this usage line to the module docstring at the top of the file:
```
    python -m ingest.extract --refresh-days 3        # nightly: re-pull the last 3 days
```

- [ ] **Step 2: Create `pyproject.toml`** (repo root):

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "restaurant-forecast-analytics"
version = "0.1.0"
description = "Toast POS -> Databricks + dbt -> Prophet sales forecasting pipeline"
requires-python = ">=3.10"
dependencies = [
    "requests>=2.32",
    "pandas>=2.2",
    "pyarrow>=17",
    "python-dotenv>=1.0",
    "databricks-sql-connector>=4.0",
    "prophet==1.1.6",
    "cmdstanpy==1.2.4",
    "holidays>=0.50",
]

[project.scripts]
rfa-extract = "ingest.extract:main"
rfa-load = "load.run_load:main"
rfa-forecast = "forecast.run_forecast:main"

[tool.setuptools]
packages = ["ingest", "load", "forecast"]
```

- [ ] **Step 3: Ignore build outputs** — in `.gitignore`, under the `# ── Python ──` section, add:

```
dist/
build/
```

- [ ] **Step 4: Verify the wheel builds and the suite is green**

```powershell
.\.venv\Scripts\python.exe -m pip install build
.\.venv\Scripts\python.exe -m build --wheel
```
Expected: `Successfully built restaurant_forecast_analytics-0.1.0-py3-none-any.whl` (in `dist/`).
Run: `.\.venv\Scripts\python.exe -m pytest -q` → PASS (48), and `.\.venv\Scripts\python.exe -m ingest.extract --help` shows `--out-dir` and `--refresh-days`.

- [ ] **Step 5: Commit**

```powershell
git add pyproject.toml ingest/extract.py .gitignore
git commit -m "build: package entry points (rfa-extract/load/forecast) + extract --out-dir"
```

---

## Task 6: Asset Bundle — `databricks.yml` authored + validated

**Files:** Create `databricks.yml`

- [ ] **Step 1: Install the Databricks CLI** (the new Go CLI, not the legacy pip one):

```powershell
winget install --id Databricks.DatabricksCLI -e --accept-source-agreements --accept-package-agreements
```
Then locate it (new shells get it on PATH; this one may not):
```powershell
(Get-Command databricks -ErrorAction SilentlyContinue).Source
```
If empty, use the WinGet link path: `$env:LOCALAPPDATA\Microsoft\WinGet\Links\databricks.exe`.

- [ ] **Step 2: Create `databricks.yml`** (repo root):

```yaml
# Databricks Asset Bundle: the nightly pipeline as code (spec 2026-06-29, §5-6).
# Phase 1: authored + validated only. Phase 2 (paid workspace) deploys it,
# adds the publish task + secret scope, and unpauses the schedule.
bundle:
  name: restaurant-forecast-analytics

artifacts:
  default:
    type: whl
    build: python -m build --wheel
    path: .

resources:
  jobs:
    restaurant_forecast_nightly:
      name: restaurant-forecast-nightly
      schedule:
        # 04:30 America/Chicago daily -- after the business date closes.
        # PAUSED until Phase 2 deploys to the paid workspace.
        quartz_cron_expression: "0 30 4 * * ?"
        timezone_id: America/Chicago
        pause_status: PAUSED
      environments:
        - environment_key: default
          spec:
            client: "2"
            dependencies:
              - ./dist/*.whl
      tasks:
        # --refresh-days / --window 3: self-healing freshness. Toast allows
        # post-close edits (refunds, tip adjustments) that change values
        # without changing row counts, so the last 3 days are always re-pulled
        # and re-loaded regardless of count reconciliation.
        - task_key: extract
          environment_key: default
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-extract
            parameters: ["--refresh-days", "3", "--out-dir", "/Volumes/workspace/default/raw_orders"]
        - task_key: load
          depends_on:
            - task_key: extract
          environment_key: default
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-load
            parameters: ["--root", "/Volumes/workspace/default/raw_orders", "--window", "3"]
        - task_key: dbt_build
          depends_on:
            - task_key: load
          environment_key: default
          dbt_task:
            project_directory: .
            commands:
              - dbt build
        - task_key: forecast
          depends_on:
            - task_key: dbt_build
          environment_key: default
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-forecast

targets:
  dev:
    default: true
```

(No hardcoded host: `bundle validate` resolves the workspace from `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars; Phase 2 adds an explicit paid-workspace target. The Volume path and `dbt_task` warehouse wiring are finalized at deploy time — spec §12.)

- [ ] **Step 3: Validate** (Git Bash; sources `.env` without echoing secrets):

```bash
cd /e/PyProj/restaurant-forecast-analytics && set -a && source .env && set +a && DBX_CLI="$(command -v databricks || echo "$LOCALAPPDATA/Microsoft/WinGet/Links/databricks.exe")" && DATABRICKS_HOST="https://$DBX_HOST" DATABRICKS_TOKEN="$DBX_TOKEN" "$DBX_CLI" bundle validate
```
(The `DBX_CLI` fallback covers this shell not having the fresh winget PATH; new shells can just call `databricks`.)
Expected: `Validation OK!` (warnings acceptable; **errors are not** — fix schema errors per the CLI's messages before proceeding).

- [ ] **Step 4: Commit**

```powershell
git add databricks.yml
git commit -m "feat: Databricks Asset Bundle -- nightly Workflow as code (paused, Phase 2 deploys)"
```

---

## Task 7: README + finish the branch

**Files:** Modify `README.md`

- [ ] **Step 1: Update README** — in the Layout section, update the `load/` and add bundle lines:

```markdown
- `load/` — raw Parquet → Databricks Delta bronze (incremental by default; `--full-refresh` to reload) + forecast/metrics Delta writers
- `databricks.yml` — Asset Bundle: the nightly extract → load → dbt → forecast Workflow as code (deploys to a paid workspace; schedule ships paused)
```
And after the Forecasting result section, add:

```markdown
## Cloud pipeline

The forecast now lands in the lakehouse itself — `forecast_daily_sales` + `model_metrics`
Delta tables and a `forecast_vs_actuals` dbt view — and the whole nightly pipeline
(extract → load → dbt → forecast) is defined as code in a Databricks Asset Bundle
([`databricks.yml`](databricks.yml)). See the
[cloud design spec](docs/superpowers/specs/2026-06-29-databricks-cloud-pipeline-design.md):
Phase 2 deploys it to a paid workspace with in-cloud extraction, a live AI/BI dashboard,
and a daily-refreshing Tableau Public feed.
```

- [ ] **Step 2: Full suite + commit**

```powershell
.\.venv\Scripts\python.exe -m pytest -q
git add README.md
git commit -m "docs: README -- incremental load, forecast tables, Asset Bundle"
```

- [ ] **Step 3: Finish the branch** — use **superpowers:finishing-a-development-branch**: verify tests on `cloud-phase1`, merge to `main`, verify again, delete the branch, then `git push` (remote `origin` is live — keep GitHub current).

---

## Self-review (spec coverage)

- **C1 forecast tables** → Tasks 1–2 (writers TDD'd; live-verified). ✅
- **C1 `forecast_vs_actuals` view** → Task 3 (source + view + 2 dbt tests). ✅
- **C2 incremental load** → Task 4 (count-reconcile, proven against the real 4/12 corruption, **plus** the spec §6 bounded trailing window — `--window`/`--refresh-days` guard against same-count post-close edits). ✅
- **C2 cloud entrypoints** → Task 5 (console scripts; `--out-dir`/`--refresh-days`/`--root`/`--window`). ✅
- **C2 bundle authored + validated** → Task 6 (5-task Workflow minus Phase-2 publish; schedule PAUSED). ✅
- **Spec §6 "CSV retained during transition"** → Task 2 keeps `write_exports`. ✅
- **Spec §7 refinement** — tables written to the default schema as a dbt **source**, view in marts (noted in Architecture; dbt-idiomatic). ✅
- **Out of scope** — publish task, deploy, secrets, AI/BI, Sheets, CI: all Phase 2 (spec §8). ✅
