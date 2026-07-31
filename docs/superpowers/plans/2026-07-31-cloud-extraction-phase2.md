# Cloud Extraction Phase 2 (Intraday Freshness) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Today's sales appear on the dashboard within 15 minutes of the register, via a fast lane that adds zero Databricks job runs.

**Architecture:** A second Cloud Run job (`toast-sync-today`, same image, `--args="--today"`) polls Toast every 15 minutes during service hours and refreshes today's Volume partition; a new dbt view `intraday_today` reads that partition **directly** (`read_files`) and joins today's forecast, so the dashboard tile is live on every view. The nightly slow lane (bronze/marts/forecast/Sheet) is untouched and stays authoritative. Spec: `docs/superpowers/specs/2026-07-30-cloud-extraction-design.md` (Phase 2 sections).

**Tech Stack:** existing image `us-central1-docker.pkg.dev/restaurant-forecast-ops/rfa/toast-sync:v1` (no rebuild — `--today` mode shipped in Phase 1), Cloud Run Jobs + Scheduler, dbt view over `read_files`, AI/BI dashboard.

**Repo root:** `E:\PyProj\restaurant-forecast-analytics`. **Branch:** continue on `cloud-extraction` (merges together with Phase 1 after the parity gate). Suite baseline **104 passed, 1 skipped**. NEVER echo secret values.

**Facts:** GCP project `restaurant-forecast-ops` (us-central1); SAs `toast-sync` (runtime) + `sync-scheduler` (invoker) exist; secrets bound as in Phase 1. Restaurant hours: Tue–Fri 11:00–20:00, Sat 10:00–20:00, Sun 10:00–15:00, Mon closed. Deliberate simplicity: ONE intraday window (10:00–20:59, Tue–Sun) for all open days — off-hour polls are no-op `--today` runs (0 orders, exit 0, no Databricks client built). GCP eventual-consistency lesson from Phase 1: allow ~20s after creating IAM-adjacent resources; a first scheduler attempt may silently fail then self-retry.

**gcloud prelude (Git Bash):** `export PATH="/c/Users/auglt/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin:$PATH"` (POSIX-form path — the Windows-form breaks the wrapper's self-location).

---

## Task 0 (agent + 1 user check): intraday job + schedule

No repo changes.

- [ ] **Step 1: Second job from the same image** (NEVER mutate `toast-sync` — its image CMD is the daily `--refresh-days 3`; `--args` here replaces CMD only, ENTRYPOINT `rfa-sync` stays):

```bash
gcloud run jobs create toast-sync-today \
  --image "us-central1-docker.pkg.dev/restaurant-forecast-ops/rfa/toast-sync:v1" \
  --region us-central1 \
  --service-account "toast-sync@restaurant-forecast-ops.iam.gserviceaccount.com" \
  --set-secrets "TOAST_CLIENT_ID=toast-client-id:latest,TOAST_CLIENT_SECRET=toast-client-secret:latest,TOAST_RESTAURANT_GUID=toast-restaurant-guid:latest,DBX_HOST=dbx-host:latest,DBX_TOKEN=dbx-token:latest" \
  --args="--today" \
  --max-retries 1 --task-timeout 600 --memory 1Gi
```

(`--args="--today"` attached form — a detached `--args --today` makes gcloud parse the flag itself. If gcloud rejects the leading dash, use the escaped-list form `--args="^@^--today"`.)

- [ ] **Step 2: Invoker binding** (existing SA — no propagation wait needed):

```bash
gcloud run jobs add-iam-policy-binding toast-sync-today --region us-central1 \
  --member "serviceAccount:sync-scheduler@restaurant-forecast-ops.iam.gserviceaccount.com" \
  --role roles/run.invoker
```

- [ ] **Step 3: Schedule — every 15 min, 10:00–20:59 CT, Tue–Sun:**

```bash
gcloud scheduler jobs create http intraday-toast-sync \
  --location us-central1 \
  --schedule "*/15 10-20 * * 0,2-6" --time-zone "America/Chicago" \
  --uri "https://run.googleapis.com/v2/projects/restaurant-forecast-ops/locations/us-central1/jobs/toast-sync-today:run" \
  --http-method POST \
  --oauth-service-account-email "sync-scheduler@restaurant-forecast-ops.iam.gserviceaccount.com"
```

(Cron day-of-week: `0`=Sun, `2-6`=Tue–Sat; Monday `1` excluded. If the parser rejects the mixed list, use `0,2,3,4,5,6`.)

- [ ] **Step 4: Prove one execution now** — `gcloud run jobs execute toast-sync-today --region us-central1 --wait`, then logs: `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="toast-sync-today"' --limit 10 --format "value(textPayload)" --freshness 15m`. Expected during service hours: `Syncing <today> -> <today>`, `<today>: N orders`, `uploaded business_date=<today>`, `Synced N rows across 1 partitions...`. Pre-open/Monday: `0 orders`, `Synced 0 rows across 0 partitions`, exit 0 (guard exempts `--today`) — also a PASS.
- [ ] **Step 5 (user, ~1 min): Alert coverage** — open the Monitoring alert policy created in Phase 1 and confirm its Cloud Run Job `result=failed` condition has NO `job_name` filter (then it already covers `toast-sync-today`); if it is filtered to `toast-sync`, either remove the filter or duplicate the condition for the new job. Report which.

## Task 1: `intraday_today` dbt view (spike first, then model)

**Files:** Create `models/marts/intraday_today.sql`; Modify `models/marts/_models.yml`.

- [ ] **Step 1: LIVE SPIKE (before writing the model — this is the discovery step):** `read_files` over the hive-partitioned Volume root may derive `business_date` from directory names while the parquet files ALSO contain a `business_date` column — behavior (duplicate column? silent preference?) must be observed, not assumed. Run via the PowerShell + script-file pattern (Git Bash mangles `DBX_HTTP_PATH` — never run connector one-liners from bash):

```python
"""Spike: read_files behavior over the partitioned Volume root."""
from dotenv import load_dotenv

load_dotenv("E:/PyProj/restaurant-forecast-analytics/.env")
from load.databricks import connect

c = connect()
cur = c.cursor()
cur.execute(
    "SELECT business_date, count(*) AS n, round(sum(net_amount - deferred_amount), 2) AS net "
    "FROM read_files('/Volumes/workspace/default/raw_orders/', format => 'parquet') "
    "WHERE NOT voided AND NOT deleted GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
)
for r in cur.fetchall():
    print(r)
cur.close()
c.close()
```

Record verbatim: does it run? one `business_date` or a conflict error? types (int vs string from path inference)? If it errors on the duplicate column, adapt: try `read_files(..., schemaHints => 'business_date BIGINT')`, or read with explicit column selection, or point at `'/Volumes/workspace/default/raw_orders/business_date=*/orders.parquet'`. The working form becomes the view's FROM clause.

- [ ] **Step 2: Read `models/staging/stg_orders.sql`** and mirror its exact live-order filter and net-sales expression (expected: `NOT voided AND NOT deleted`, net = `net_amount - deferred_amount` — but MATCH THE FILE, do not trust this plan).
- [ ] **Step 3: Write the model** — `models/marts/intraday_today.sql` (adjust the `raw_orders` CTE to the spike's working form):

```sql
{{ config(materialized='view') }}

-- Fast-lane intraday view (Phase 2 of the 2026-07-30 cloud-extraction spec).
-- Reads today's raw partition STRAIGHT off the Volume that the Cloud Run
-- poller refreshes every 15 minutes -- freshness comes from the upload
-- cadence, with no Databricks job run in between. Bronze and the marts stay
-- nightly-authoritative; nothing downstream reads this view.
-- Anchored on a constant "today" row and LEFT JOINed both ways, so it
-- returns EXACTLY ONE ROW even pre-open, on closed Mondays, or if last
-- night's forecast is missing (zeros/nulls, never an empty result).

with today as (
    select cast(
        date_format(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 'yyyyMMdd')
        as bigint
    ) as business_date
),

raw_orders as (
    select *
    from read_files(
        '/Volumes/workspace/default/raw_orders/',
        format => 'parquet'
    )
),

today_orders as (
    select
        o.business_date,
        count(*)                                        as order_count,
        sum(o.num_guests)                               as guest_count,
        round(sum(o.net_amount - o.deferred_amount), 2) as net_sales_so_far,
        max(o.opened_date)                              as last_order_opened_at
    from raw_orders o
    join today t on o.business_date = t.business_date
    where not o.voided and not o.deleted
    group by o.business_date
),

forecast as (
    select forecast_date as business_date, yhat, yhat_lower, yhat_upper
    from {{ source('forecast', 'forecast_daily_sales') }}
)

select
    t.business_date,
    coalesce(o.order_count, 0)                        as order_count,
    coalesce(o.guest_count, 0)                        as guest_count,
    coalesce(o.net_sales_so_far, 0)                   as net_sales_so_far,
    o.last_order_opened_at,
    f.yhat                                            as forecast_total,
    f.yhat_lower,
    f.yhat_upper,
    case
        when f.yhat > 0
        then round(coalesce(o.net_sales_so_far, 0) / f.yhat * 100, 1)
    end                                               as pct_of_forecast
from today t
left join today_orders o on t.business_date = o.business_date
left join forecast f on t.business_date = f.business_date
```

- [ ] **Step 4: `_models.yml` entry** (match the file's existing style):

```yaml
  - name: intraday_today
    description: >
      Fast-lane single-row view: today-so-far from the raw Volume partition
      (15-min Cloud Run polling) vs today's forecast. Not read by anything
      downstream; dashboard-only.
    columns:
      - name: business_date
        tests: [not_null, unique]
```

- [ ] **Step 5: Local dbt build** (creates the view now — the nightly dbt task rebuilds it from the bundle only AFTER the branch merges and deploys; building locally makes it live immediately): the repo's dbtRunner one-liner pattern (`['build', '--profiles-dir', '.']`). Expected: PASS count grows by the new view + its 2 tests (14 → 17 total). If CREATE VIEW fails on the read_files form, return to Step 1's alternatives.
- [ ] **Step 6:** Full pytest suite unchanged (**104 passed, 1 skipped**) + hooks green. Commit `models/marts/intraday_today.sql` + `models/marts/_models.yml`: `feat: intraday_today fast-lane view (today-so-far vs forecast)` + Co-Authored-By trailer (Bash heredoc for the message).

## Task 2 (agent): Live freshness verification during service hours

No repo changes. Run only when the restaurant is open (Tue–Sun, after opening).

- [ ] **Step 1:** Latest intraday execution is recent and green: `gcloud run jobs executions list --job toast-sync-today --region us-central1 --limit 2` → newest COMPLETE 1/1, created within the last 15 min (after the schedule's first firing window).
- [ ] **Step 2:** Partition mtime proves the poll landed (Git Bash prelude from Phase 1 plan Task 3 Step 4, then): `MSYS_NO_PATHCONV=1 "$DBX_CLI" fs ls -l "dbfs:/Volumes/workspace/default/raw_orders/business_date=<today>"` → `orders.parquet` mtime within 15 min.
- [ ] **Step 3:** The view reflects it (PowerShell + script file): `SELECT * FROM workspace.default_marts.intraday_today` → exactly 1 row, `business_date` = today, `net_sales_so_far` > 0 and plausible, `forecast_total` populated, `pct_of_forecast` sensible. Record the row (it is publishable aggregate data).
- [ ] **Step 4:** Re-query ~20 min later (spanning ≥1 poll): `net_sales_so_far`/`order_count` moved (or provably no new orders). This is spec success criterion 3 observed. Record both readings.

## Task 3 (USER, guided): dashboard "Today" tile

- [ ] In the AI/BI dashboard: new dataset `SELECT * FROM workspace.default_marts.intraday_today`. Add: counter `net_sales_so_far` (currency, title "Today so far"), counter `pct_of_forecast` (suffix %, title "of today's forecast"), counter `order_count` ("Orders today"). Optional context: small text widget "refreshes every 15 min during service hours". Optional hourly bar (second dataset, inline SQL — adapt the FROM clause to Task 1's spike result):

```sql
SELECT hour(from_utc_timestamp(to_timestamp(opened_date), 'America/Chicago')) AS hr,
       round(sum(net_amount - deferred_amount), 2) AS net_sales
FROM read_files('/Volumes/workspace/default/raw_orders/', format => 'parquet')
WHERE business_date = cast(date_format(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 'yyyyMMdd') as bigint)
  AND NOT voided AND NOT deleted
GROUP BY 1 ORDER BY 1
```

- [ ] Screenshot for the docs pass; report done.

## Task 4: docs + memory + merge gate

- [ ] **Step 1:** README: extend the cloud section with the fast lane (two-speed diagram sentence: 15-min intraday polling → `intraday_today` view → dashboard; nightly lane authoritative). Spec: mark success criterion 3 observed with the Task 2 readings (add an as-built note; do not rewrite the spec).
- [ ] **Step 2:** Commit docs: `docs: two-speed as-built -- intraday fast lane live`.
- [ ] **Step 3 (controller):** memory update (Phase 2 live: job/schedule names, view, freshness evidence).
- [ ] **Step 4:** Merge gate unchanged from Phase 1: parity criterion met (2+ clean mornings) → user disables the PC Task Scheduler entry → final whole-branch review of `cloud-extraction` → merge to `main` + push (finishing-a-development-branch). Phases 1+2 merge together.

---

## Self-review (spec coverage)

- Intraday schedule 15-min/service-hours/Tue–Sun → Task 0 (single-window simplicity per spec §3). ✅
- Today-view over `read_files`, empty-partition graceful → Task 1 (anchored constant row; LEFT JOINs; coalesce; the read_files duplicate-column risk is a spike, not an assumption). ✅
- Dashboard tile live-on-view → Task 3. ✅
- Spec success criterion 3 (≤15 min staleness, observed) → Task 2 Steps 2–4. ✅
- Zero extra Databricks job runs → no databricks.yml change anywhere; view is dbt-built. ✅
- Alert coverage for the new job → Task 0 Step 5. ✅
- No plan step mutates `toast-sync` (review caution honored); image reused, no rebuild. ✅
- Types consistent: `business_date` BIGINT everywhere (cast in `today`, parquet int64, `forecast_date` BIGINT). ✅
