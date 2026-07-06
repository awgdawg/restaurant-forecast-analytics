# Databricks Cloud Pipeline — Phase 2 Implementation Plan (Deploy + Serve)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the Phase-1 Asset Bundle to a **paid Databricks trial workspace** so the entire nightly pipeline (Toast extract → bronze → dbt → forecast → publish) runs **in the cloud on a schedule**, then stand up the serving layer: a live **AI/BI dashboard**, a **Google-Sheets bridge**, and an auto-refreshing **Tableau Public** dashboard — plus CI.

**Why:** Implements milestones **C3 (deploy) + C4 (serving) + C5 (polish/CI)** of the [cloud-pipeline spec](../specs/2026-06-29-databricks-cloud-pipeline-design.md) §8/§13, completing spec success criteria 2, 3, and 4. Paid is required for exactly two reasons (verified 2026-07-06 against current docs): Free Edition restricts serverless **outbound internet to trusted domains** (the Toast API is unreachable in-cloud) and is **non-commercial** (this becomes the restaurant's operational tool).

**Architecture:** The **trial workspace is a NEW account/workspace** (Free Edition cannot be upgraded in place), so the lakehouse is **re-seeded from the local Parquet archive** (uploaded to a UC Volume — no Toast API re-pull). The bundle gains a `prod` target (explicit host, dbt `warehouse_id`, task retries, **UNPAUSED** schedule) parameterized by bundle variables; secrets move to a **Databricks secret scope**; the one hardcoded catalog reference in code (`forecast/data.py`) becomes env-driven. Serving reads the `forecast_vs_actuals` view — AI/BI natively (live), Tableau Public via a Sheets bridge refreshed by the nightly job's new `publish` task. Local `.env` cuts over to the new workspace (the Free workspace remains an unused sandbox).

**Tech Stack:** everything from Phase 1 + Databricks CLI (bundles, secrets, fs, jobs), `gspread` + `google-auth`, GitHub Actions.

---

## Scope

- **Part A (C3):** cut over config → deploy `prod` → seed data → in-cloud extraction w/ secrets → scheduled nightly run verified.
- **Part B (C4):** `publish.to_sheets` job task → AI/BI dashboard → Tableau Public on the Sheet → retire the CSV exports (spec §6's transition ends).
- **Part C (C5):** GitHub Actions CI (pytest + `bundle validate`), an **observed scheduled-run freshness proof** (spec criterion 3), README/writeup + cost notes.
- **Out of scope:** labor/inventory/near-real-time (roadmap); anonymization (real figures published by owner decision); paid Tableau tiers.
- **Carry-over minors from Phase-1 review (both land in Task 2):** dist/ clean before wheel build; Volume path → bundle variable.

**Repo root:** `E:\PyProj\restaurant-forecast-analytics` (PowerShell; `.\.venv\Scripts\python.exe`). Branch: `cloud-phase2`. Remote `origin` = public GitHub; push after merge.

**CLI prelude (REQUIRED before any `"$DBX_CLI"` step; Git Bash; sources `.env` without echoing values):**
```bash
cd /e/PyProj/restaurant-forecast-analytics && set -a && source .env && set +a \
  && DBX_CLI="$(command -v databricks || echo "$LOCALAPPDATA/Microsoft/WinGet/Links/databricks.exe")" \
  && export DATABRICKS_HOST="https://$DBX_HOST" DATABRICKS_TOKEN="$DBX_TOKEN"
```
The Databricks CLI reads `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (NOT the `DBX_*` names in `.env`) — this prelude bridges them. Every CLI step below assumes it ran in the current shell. NEVER echo secret values.

**User-gate:** **Task 0, Task 6 Step 1, Tasks 7–8 (GUI parts), and Task 9 Step 1 are user-driven**; everything else is agent-executable. Agent steps never sit blocked on a UI — each has a CLI verification.

---

## Task 0 (USER): Provision the trial workspace + gather 4 values

- [ ] **Step 1 (user):** Sign up for the free trial (express setup) at databricks.com/try-databricks — email-only, no credit card to start, 14 days of credits. This creates a NEW account + serverless workspace (do NOT use the Free Edition login flow).
- [ ] **Step 2 (user):** In the NEW workspace, gather:
  1. **Workspace URL** — `https://dbc-….cloud.databricks.com` (bare hostname → `DBX_HOST`).
  2. **PAT** — Settings → Developer → Access tokens → Generate, scope **All APIs** → into `.env` as `DBX_TOKEN` (never into chat).
  3. **SQL warehouse `http_path` + warehouse id** — SQL Warehouses → starter warehouse → Connection details (`/sql/1.0/warehouses/<id>`; the trailing hex is the warehouse id — this one is safe to report in chat).
  4. **Default catalog name** — Catalog Explorer (typically `workspace` or `main`) — safe to report.
- [ ] **Step 3 (user):** Update `.env`: `DBX_HOST`, `DBX_HTTP_PATH`, `DBX_TOKEN` → new values; `DBX_CATALOG` → the catalog from 2.4; `DBX_SCHEMA` stays `default`. Keep the Free-workspace values as comments if wanted.
- [ ] **Step 4 (agent):** Verify connectivity: `.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from load.databricks import connect; c=connect(); cur=c.cursor(); cur.execute('select current_catalog(), current_schema()'); print(cur.fetchone()); cur.close(); c.close()"` → prints the new catalog + `default`. If the schema is missing, create it: same route, `CREATE SCHEMA IF NOT EXISTS <catalog>.default`.

---

## PART A — C3: Deploy the pipeline to the trial workspace

### Task 1: De-hardcode the mart catalog reference (TDD)

`forecast/data.py` pins `MART = "workspace.default_marts.fct_daily_sales"` — correct on the Free workspace, wrong anywhere else.

**Files:** Modify `forecast/data.py`, `tests/test_forecast_data.py`

- [ ] **Step 1: Failing tests** — append to `tests/test_forecast_data.py`:

```python
from forecast.data import mart_table


def test_mart_table_builds_from_env(monkeypatch):
    monkeypatch.setenv("DBX_CATALOG", "maincat")
    monkeypatch.setenv("DBX_SCHEMA", "restaurant")
    assert mart_table() == "maincat.restaurant_marts.fct_daily_sales"


def test_mart_table_defaults_match_free_workspace(monkeypatch):
    monkeypatch.delenv("DBX_CATALOG", raising=False)
    monkeypatch.delenv("DBX_SCHEMA", raising=False)
    assert mart_table() == "workspace.default_marts.fct_daily_sales"
```

- [ ] **Step 2: RED** — `.\.venv\Scripts\python.exe -m pytest tests/test_forecast_data.py -q` → ImportError (`mart_table`).
- [ ] **Step 3: GREEN** — in `forecast/data.py`: add `import os` to the imports; replace the `MART` constant with:

```python
def mart_table() -> str:
    """Fully-qualified fct_daily_sales, honoring the same env vars as connect().
    dbt's generate_schema_name appends the custom suffix: <schema>_marts."""
    catalog = os.environ.get("DBX_CATALOG", "workspace")
    schema = os.environ.get("DBX_SCHEMA", "default")
    return f"{catalog}.{schema}_marts.fct_daily_sales"
```
and change `def load_daily_series(conn, table: str = MART)` to:
```python
def load_daily_series(conn, table: str | None = None) -> pd.DataFrame:
    """Query the daily-sales mart and clean it into a forecasting series."""
    table = table or mart_table()
```
(body otherwise unchanged).

- [ ] **Step 4:** Full suite → **51 passed** (49 + 2). **Step 5: Commit** `feat: env-driven mart reference (portable across workspaces)`.

### Task 2: Bundle `prod` target + variables + retries + clean wheel builds

**Files:** Modify `databricks.yml`; Create `scripts/build_wheel.py`

- [ ] **Step 1: Build script** (kills the stale-wheel glob risk) — `scripts/build_wheel.py`:

```python
"""Clean dist/ then build the wheel -- keeps the bundle's dist/*.whl glob unambiguous."""

import shutil
import subprocess
import sys

shutil.rmtree("dist", ignore_errors=True)
sys.exit(subprocess.call([sys.executable, "-m", "build", "--wheel"]))
```

- [ ] **Step 2: Replace `databricks.yml`** with this full file (genuinely complete — all variables declared inside):

```yaml
# Databricks Asset Bundle: the nightly pipeline as code (spec 2026-06-29, §5-6).
# dev = authoring/validation target (schedule PAUSED); prod = the paid/trial
# workspace running nightly (schedule LIVE, dbt wired to a SQL warehouse).
bundle:
  name: restaurant-forecast-analytics

variables:
  raw_volume:
    description: UC Volume path where Toast order Parquet lands
    default: /Volumes/workspace/default/raw_orders
  dbt_warehouse_id:
    description: SQL warehouse id for the prod dbt task (pass via --var at deploy)
    default: ""
  prod_host:
    description: Paid/trial workspace URL incl. https:// (pass via --var at deploy)
    default: ""

artifacts:
  default:
    type: whl
    build: python scripts/build_wheel.py
    path: .

resources:
  jobs:
    restaurant_forecast_nightly:
      name: restaurant-forecast-nightly
      schedule:
        # 04:30 America/Chicago daily -- after the business date closes.
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
        # Retries (spec §11): extract faces the network -> 2 retries; the rest
        # are idempotent (DELETE+INSERT / overwrite) so 1 retry each is safe.
        - task_key: extract
          environment_key: default
          max_retries: 2
          min_retry_interval_millis: 300000
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-extract
            parameters: ["--refresh-days", "3", "--out-dir", "${var.raw_volume}"]
        - task_key: load
          depends_on:
            - task_key: extract
          environment_key: default
          max_retries: 1
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-load
            parameters: ["--root", "${var.raw_volume}", "--window", "3"]
        - task_key: dbt_build
          depends_on:
            - task_key: load
          environment_key: default
          max_retries: 1
          dbt_task:
            project_directory: .
            commands:
              - dbt build
        - task_key: forecast
          depends_on:
            - task_key: dbt_build
          environment_key: default
          max_retries: 1
          python_wheel_task:
            package_name: restaurant_forecast_analytics
            entry_point: rfa-forecast

targets:
  dev:
    default: true
  prod:
    mode: production
    workspace:
      host: ${var.prod_host}
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    resources:
      jobs:
        restaurant_forecast_nightly:
          schedule:
            pause_status: UNPAUSED
          tasks:
            - task_key: dbt_build
              dbt_task:
                warehouse_id: ${var.dbt_warehouse_id}
```
**Sanctioned adaptation (broad):** the installed CLI's schema is authoritative for the ENTIRE prod-target shape — target-level task overrides, production-mode requirements (`root_path`/`run_as`), retry field names, and variable plumbing. If `bundle validate` rejects any of it, make the minimal change its error directs (keeping semantics: live schedule + warehouse-wired dbt + retries + variables), and document every deviation.

- [ ] **Step 3: Validate BOTH targets** (after the CLI prelude): `"$DBX_CLI" bundle validate` and `"$DBX_CLI" bundle validate -t prod --var prod_host="https://$DBX_HOST" --var dbt_warehouse_id="<warehouse id from Task 0>" --var raw_volume="/Volumes/<catalog>/default/raw_orders"` → both `Validation OK!`.
- [ ] **Step 4: Commit** `feat: bundle prod target (live schedule, dbt warehouse, retries, vars) + clean wheel builds`.

### Task 3: Workspace setup — Volume + secret scope

All CLI (prelude assumed).

- [ ] **Step 1: Volume** — `"$DBX_CLI" volumes create <catalog> default raw_orders MANAGED` (create `default` schema first if Task 0 Step 4 didn't). Verify: `"$DBX_CLI" volumes read <catalog>.default.raw_orders`.
- [ ] **Step 2: Secret scope** — `"$DBX_CLI" secrets create-scope restaurant-forecast`; then for each of `TOAST_CLIENT_ID`→`toast-client-id`, `TOAST_CLIENT_SECRET`→`toast-client-secret`, `TOAST_RESTAURANT_GUID`→`toast-restaurant-guid`: load the value from the already-sourced env WITHOUT echoing (exact stdin/`--json` syntax per `"$DBX_CLI" secrets put-secret --help`; adapt, don't echo). Verify: `"$DBX_CLI" secrets list-secrets restaurant-forecast` shows the 3 keys (names only).
- [ ] **Step 3:** No repo changes to commit.

### Task 4: Seed the lakehouse from the local Parquet archive

- [ ] **Step 1: Upload Parquet → Volume** — `"$DBX_CLI" fs cp -r data/raw/orders "dbfs:/Volumes/<catalog>/default/raw_orders"` (~768 partition dirs; minutes; re-runnable with `--overwrite`). Verify: `"$DBX_CLI" fs ls "dbfs:/Volumes/<catalog>/default/raw_orders" | wc -l` ≈ 768.
- [ ] **Step 2: Load bronze from the LOCAL archive** — `.\.venv\Scripts\python.exe -m load.run_load` (the local process reads the local `data/raw/orders`; `dbfs:/Volumes` paths are only readable in-cloud — the Volume copy from Step 1 is for the nightly job). Writes to the NEW workspace via the cut-over `.env`. Expected: `768 days on disk; loading 768` then `Loaded 75659 rows into bronze_orders` (fresh workspace = full load; allow minutes).
- [ ] **Step 3: Pre-create the forecast tables, then dbt build.** On a fresh workspace the `forecast_vs_actuals` view's source tables don't exist until the first forecast run, and Databricks validates relations at CREATE VIEW — so ensure the (empty) tables first:
```powershell
.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from load.databricks import connect; from load.load_forecast import forecast_ddl, metrics_ddl; c=connect(); cur=c.cursor(); cur.execute(forecast_ddl()); cur.execute(metrics_ddl()); print('forecast tables ensured'); cur.close(); c.close()"
```
Then the standard dbt invocation (`dbtRunner ['build','--profiles-dir','.']` one-liner) → expected `PASS=14 TOTAL=14` (view sees empty tables → 0 forecast rows; its tests pass).
- [ ] **Step 4: Forecast + relational verify** — `.\.venv\Scripts\python.exe -m forecast.run_forecast` (backtest + 14-day forecast + table writes), then:
```powershell
.\.venv\Scripts\python.exe -c "from dotenv import load_dotenv; load_dotenv(); from load.databricks import connect; from forecast.data import mart_table; c=connect(); cur=c.cursor(); cur.execute(f'select count(*) from {mart_table()}'); mart=cur.fetchone()[0]; cur.execute(f\"select count(*), sum(cast(is_forecast as int)) from {mart_table().replace('fct_daily_sales','forecast_vs_actuals')}\"); total, fc = cur.fetchone(); print(f'mart={mart} view={total} forecast_rows={fc}'); assert total == mart + 14 and fc == 14; print('OK'); cur.close(); c.close()"
```
Expected: counts print and `OK`.
- [ ] **Step 5: Cross-workspace reconciliation** — authoritative expectations (plan-recorded facts, verified against the local Parquet archive): bronze = **75,659 rows / 768 days**; mart total net_sales ≈ **$1.93M** (matches `docs/forecast-writeup.md`). Counts must match exactly (same Parquet, deterministic transforms); Prophet forecast VALUES may differ slightly run-to-run — counts must not.
- [ ] **Step 6: Commit** doc notes only (if any).

### Task 5: In-cloud secrets + prophet-env verification + deploy + first cloud run

Two named risks, both with prepared fallbacks:
- **(a) Secrets for extract** — the task needs `TOAST_*` in the serverless job env. **Discovery first** (CLI/API authoritative): does the serverless `environments` spec / task definition support env-var injection with `{{secrets/restaurant-forecast/<key>}}` references? **Primary:** declare them in the bundle. **Fallback:** a ~10-line shim in `ingest/config.py` — when `TOAST_CLIENT_ID` is absent from env, read the three values via `databricks.sdk` `WorkspaceClient().dbutils.secrets.get("restaurant-forecast", key)` (databricks-sdk is preinstalled on serverless). TDD the shim if taken (env wins; secrets only when env missing; RuntimeError preserved when neither).
- **(b) Prophet on serverless** (spec §12 open question): the wheel's declared deps (`prophet==1.1.6`, `cmdstanpy==1.2.4`) are installed into the job environment from `./dist/*.whl` metadata — the serverless equivalent of the `%pip` check. The first forecast-task run proves it. **Fallbacks if it fails:** pin/adjust versions in the environment spec's `dependencies`, or (worst case) run the forecast task on a small jobs cluster with an init script — document whichever is needed.

- [ ] **Step 1:** Secrets discovery → implement primary or fallback (+ tests if shim). Document the path taken.
- [ ] **Step 2: Deploy** — `"$DBX_CLI" bundle deploy -t prod --var prod_host="https://$DBX_HOST" --var dbt_warehouse_id="<id>" --var raw_volume="/Volumes/<catalog>/default/raw_orders"`.
- [ ] **Step 3: First manual cloud run** — `"$DBX_CLI" bundle run restaurant_forecast_nightly -t prod`. Watch all 4 tasks: extract logging `skip/0 orders/N orders` for the 3-day window **proves outbound internet + secrets in-cloud** (the thing Free Edition cannot do); forecast task completing proves prophet/cmdstan on serverless (risk (b)). Capture the run URL. Any task failure → apply the matching fallback, redeploy, re-run.
- [ ] **Step 4: Schedule check (agent-executable)** — from the deploy output get the job id, then `"$DBX_CLI" jobs get <job_id>` → assert `pause_status: UNPAUSED`, cron `0 30 4 * * ?`, timezone `America/Chicago`. (User may also eyeball the Jobs UI, not required.)
- [ ] **Step 5: Commit** (shim/bundle adjustments) `feat: in-cloud Toast secrets + first cloud pipeline run` — **checkpoint: Part A / C3 done.**

---

## PART B — C4: Serving

### Task 6: `publish/to_sheets.py` + job task (TDD) — the Tableau bridge

**Files:** Create `publish/__init__.py`, `publish/to_sheets.py`, `tests/test_to_sheets.py`; Modify `forecast/data.py` (+1 helper + test), `pyproject.toml`, `requirements.txt`, `databricks.yml`, `.env` (local only)

- [ ] **Step 1 (user, can run parallel to Part A):** Google Cloud: project → enable Sheets API → **service account** → download JSON key to a local path OUTSIDE the repo. Create Sheet `restaurant-forecast-serving` with empty tabs `forecast_vs_actuals` + `model_metrics`; share (Editor) with the service-account email. Report the **Sheet ID** (not sensitive).
- [ ] **Step 2 (agent): Failing tests first** — `tests/test_to_sheets.py`:
  1. `frame_to_rows` shape tests (pure, no fakes): header row = column names in order; NaN → `""`; dates stringified.
  2. A separate publish-routine test with a `FakeWorksheet` (records `clear()` / `update(values)` calls) + fake client: assert each tab is cleared then updated with `frame_to_rows` output.
  3. Also append to `tests/test_forecast_data.py`: `metrics_table()` returns `"<catalog>.<schema>.model_metrics"` from env (NOTE: `model_metrics` lives in the base schema — a forecast-written source table — NOT in `<schema>_marts`).
  Run → RED (ImportErrors).
- [ ] **Step 3 (agent): GREEN** — implement:
  - `forecast/data.py`: `metrics_table()` helper beside `mart_table()` → `f"{catalog}.{schema}.model_metrics"`.
  - `publish/to_sheets.py`: `frame_to_rows(df)`; `main()` reads `SELECT * FROM {mart_table().replace('fct_daily_sales','forecast_vs_actuals')} ORDER BY business_date` and `SELECT * FROM {metrics_table()} ORDER BY run_ts`, auths gspread from `GCP_SA_JSON` (accepts a **file path or inline JSON** — detect a leading `{`), opens `GSHEET_ID`, clears + updates the two tabs by name.
  - `pyproject.toml`: deps += `gspread>=6`, `google-auth>=2`; scripts += `rfa-publish = "publish.to_sheets:main"`; **`[tool.setuptools] packages = ["ingest", "load", "forecast", "publish"]`** (without this the wheel omits the module → ModuleNotFoundError in-cloud). `requirements.txt`: pinned equivalents.
  - Suite green (**expected: 51 + ~4 new = 55**; state the actual count in the report).
- [ ] **Step 4 (agent): Cloud + local plumbing (concrete):**
  - Secret scope: add `gcp-sa-json` (the JSON key content, from the local file, never echoed).
  - `GSHEET_ID` is NOT sensitive → a new bundle variable `gsheet_id` (empty default), passed at deploy.
  - Bundle: add `publish` task after `forecast` (python_wheel_task, `rfa-publish`, `max_retries: 1`), with `GSHEET_ID`/`GCP_SA_JSON` reaching the task via whichever mechanism Task 5 established (env-var injection with `{{secrets/restaurant-forecast/gcp-sa-json}}` under the primary; extend the Task-5 shim pattern with a publish-side lookup under the fallback — implement + test it, don't hand-wave).
  - Local `.env` additions: `GSHEET_ID=<id>` and `GCP_SA_JSON=<local path to the key file>`.
- [ ] **Step 5 (agent): Live verify** — local: run `rfa-publish` once, verify both tabs populate (row counts = view rows / metrics rows). Cloud: redeploy prod, `bundle run` once, confirm the publish task greens and the Sheet updated (check the Sheet's edit timestamp or row counts again).
- [ ] **Step 6: Commit** `feat: publish task -- forecast_vs_actuals + metrics to Google Sheets nightly`.

### Task 7 (user-driven, guided): AI/BI dashboard — the LIVE view

- [ ] In the trial workspace: New → Dashboard (AI/BI). Datasets: `forecast_vs_actuals` + `model_metrics`. Recreate the approved mockup: line chart `date_day` × (`net_sales_actual`, `yhat`) with `yhat_lower/upper` band, KPI counters (next-14-days sum where `is_forecast`; latest WAPE per model), filters. Publish (workspace-internal). **User saves a screenshot; agent commits it** to `docs/` for the writeup. *(In-UI per spec §12.)*

### Task 8: Tableau Public on the Sheet + retire the CSV exports

- [ ] **Step 1 (user):** Tableau Public Desktop → Connect → **Google Sheets** → the serving Sheet → build per the mockup → publish with **"Keep my data in sync"** enabled (Tableau Public re-syncs from the Sheet daily; with the nightly publish task the public dashboard now self-updates). Report the public URL.
- [ ] **Step 2 (agent):** Add URL + screenshots to README + `docs/forecast-writeup.md`; portfolio embed.
- [ ] **Step 3 (agent): Retire the CSV transition path (deterministic — spec §6's transition ends; `to_sheets` reads the view server-side, so the pandas builders have no remaining consumer):**
  - Delete `forecast/export_tableau.py` and `tests/test_export_tableau.py` (−2 tests).
  - In `forecast/run_forecast.py`: remove the `export_tableau` import and the three `fva`/`write_exports`/`print` lines (the Delta-write block stays).
  - `git rm exports/forecast_vs_actuals.csv exports/backtest_metrics.csv`; remove the `!exports/*.csv` exception + its comment from `.gitignore`.
  - README: drop the `exports/` layout bullet + CSV mentions.
  - Full suite green (**expected: 55 − 2 = 53**; state actual). Commit `refactor: retire CSV exports -- Sheets bridge is the serving path` — **checkpoint: Part B / C4 done.**

---

## PART C — C5: CI + freshness proof + polish

### Task 9: GitHub Actions CI

**Files:** Create `.github/workflows/ci.yml`

- [ ] **Step 1 (user, 2 min):** GitHub repo → Settings → Secrets and variables → Actions: add `DATABRICKS_HOST` (`https://<new host>`) and `DATABRICKS_TOKEN` (a **separate, short-lived** PAT minted for CI only).
- [ ] **Step 2 (agent):** `ci.yml`: on push/PR to main — job `tests`: setup-python 3.10, `pip install -r requirements.txt`, `pytest -q` *(installs prophet/cmdstan on the runner — if slow/flaky, the documented fallback is a pytest marker excluding the one Prophet fit test)*; job `bundle-validate`: official Databricks setup-cli action, `databricks bundle validate` with the two secrets as env. README badge.
- [ ] **Step 3 (agent):** Push branch; confirm both jobs green on GitHub before merge.

### Task 10: Freshness proof, docs, cost notes, finish

- [ ] **Step 1 (agent, the morning after a scheduled run — spec criterion 3 observed, not assumed):** `"$DBX_CLI" jobs list-runs --job-id <id>` → confirm a run with **SCHEDULED trigger** succeeded end-to-end; then query `select max(business_date) from bronze_orders` → ≤ 1 day stale on an open day. Record both in the writeup.
- [ ] **Step 2:** README cloud section → "deployed" state (nightly schedule, AI/BI + Tableau links, CI badge). `docs/forecast-writeup.md`: cloud chapter + dashboards + the freshness proof.
- [ ] **Step 3:** Cost notes per spec §15/§10: observed trial burn rate (workspace usage page) → projected $/mo (serverless nightly job + warehouse auto-stop) → **explicit decision point before trial day 14** (add payment method or wind back to Free).
- [ ] **Step 4:** Full suite + dbt build green; **finishing-a-development-branch**: merge `cloud-phase2` → main, verify, delete, push.

---

## Self-review (spec coverage)

- **C3** → Tasks 0–5 (deploy, Volume, secrets, seed, in-cloud extract, live schedule). Criterion 2 ✅.
- **Criterion 3 (freshness)** → structurally at Task 5, **observed** at Task 10 Step 1 ✅.
- **C4** → Tasks 6–8 (Sheets bridge with concrete env/secret plumbing, AI/BI, Tableau auto-refresh, deterministic CSV retirement per §6). Criterion 4 ✅.
- **C5** → Tasks 9–10 (CI incl. `bundle validate`, docs/cost + day-14 decision). Criteria 5/6 ✅.
- **§11 reliability** → task retries in the bundle (Task 2 YAML), idempotent re-runs unchanged ✅. **§11 new tests** → to_sheets shaping + fake-worksheet publish + metrics_table (Task 6) ✅.
- **§9 secrets** → scope holds Toast + GCP SA; CI PAT separate/short-lived; nothing committed; PII rules untouched ✅.
- **§12 open questions carried as explicit discovery/fallbacks:** serverless env-var secrets (Task 5a), **prophet on serverless** (Task 5b), trial default catalog (Task 0), Prophet-on-CI runtime (Task 9).
- **Carry-over minors** → Task 2 (dist clean via `scripts/build_wheel.py`, `raw_volume` variable) ✅. **Portability** → Task 1 (env-driven mart) + Task 6 (`metrics_table`) ✅.
- **Checkpoints:** Part A end (Task 5 Step 5), Part B end (Task 8 Step 3), plan-complete (Task 10 Step 4).
