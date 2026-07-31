# Cloud-Native Toast Extraction with Two-Speed Freshness — Design

**Date:** 2026-07-30 · **Status:** Approved

## Problem

Toast extraction is the pipeline's only non-cloud step: a Windows Task
Scheduler job on the owner's PC (08:45 daily). When the PC is off, data
silently stops arriving (proven 2026-07-17..29: 11 business days missing until
manually backfilled). The Databricks workspace itself cannot host extraction:
its serverless egress is DNS-allowlisted below every user-configurable layer —
exhaustively proven by probing after (a) "Allow all destinations" network
policy, (b) enforced RESTRICTED policy with `ws-api.toasttab.com` explicitly
allowlisted, and (c) Unity Catalog HTTP-connection registration of the Toast
host. Ten consecutive probes across three configuration states: identical
block. Extraction must therefore run outside Databricks serverless, and the
owner additionally wants **near-real-time intraday freshness** for business
use.

## Architecture: two speeds, one extractor

```
GCP project: restaurant-forecast-ops
  Artifact Registry image  <- Dockerfile (pip install .)
  Cloud Run Job "toast-sync" -> rfa-sync
      Scheduler 1 (daily 08:30 CT):      rfa-sync --refresh-days 3
      Scheduler 2 (15-min, Tue-Sun):     rfa-sync --today
  Secret Manager: TOAST_* + dedicated DBX upload PAT -> injected as env vars
        |
        v  Parquet partitions (Files API upload)
  /Volumes/workspace/default/raw_orders/business_date=YYYYMMDD/
        |
  SLOW LANE (unchanged): 09:15 nightly job load->dbt->forecast->publish
  FAST LANE (new): dashboard "Today" tile -> SQL warehouse read_files()
                   over today's partition (no extra job runs)
```

- **Slow lane is authoritative.** Bronze and the marts are written only by the
  nightly job; the morning `--refresh-days 3` re-pull finalizes each day
  (post-close edits), exactly as today. Intraday partials never enter the
  analytical tables.
- **Fast lane is read-through.** Intraday freshness = upload frequency; the
  dashboard queries the raw partition live. Zero additional Databricks job
  runs.

## Components

### 1. `rfa-sync` entry point (repo addition)

Extract-then-upload in one process. Extraction reuses `ingest.extract`
verbatim. Upload is a new thin module (`ingest/to_volume.py`) using the
`databricks-sdk` **Files API** (replaces the CLI `fs cp` in
`morning_extract.ps1`), uploading only partitions the extract run touched.
Modes:

- `--refresh-days N` — daily self-healing window (identical semantics to
  today's local run).
- `--today` — current business date only (America/Chicago; the restaurant
  closes at 20:00, so no after-midnight business-date edge). Fast: one
  paginated day pull.

Config is env-first via the existing shim pattern (`TOAST_*`, `DBX_HOST`,
`DBX_TOKEN`) — Cloud Run env injection needs no code changes.

### 2. Container

`Dockerfile`: `python:3.11-slim`, `pip install .` plus `databricks-sdk`,
entrypoint `rfa-sync`. No secrets in the image; generic and public-repo-safe.
Built/pushed to Artifact Registry.

### 3. Schedules (Cloud Scheduler, America/Chicago)

| Job | Cron | Args | Purpose |
|---|---|---|---|
| daily-sync | 08:30 daily | `--refresh-days 3` | Feeds 09:15 nightly; replaces the PC |
| intraday-sync | every 15 min, 10:00–20:59, Tue–Sun | `--today` | Fast lane |

Deliberate simplicity: one intraday window for all open days (hours 11–20
Tue–Fri, 10–20 Sat, 10–15 Sun). Off-hour polls return zero orders in seconds;
Monday is excluded. Restaurant hours: Tue–Fri 11:00–20:00, Sat 10:00–20:00,
Sun 10:00–15:00, Mon closed.

### 4. Intraday serving

A dbt view over `read_files()` on today's partition (current business date
computed in America/Chicago), aggregated to today-so-far — net sales, order
count, by-hour cumulative — joined to `forecast_daily_sales` for today's
predicted total and band. Dashboard "Today" tile: sales-so-far vs. forecast,
live on every view via the existing warehouse (auto-start/stop unchanged).
Empty-partition (closed day / pre-open) must render gracefully (zeros, not
errors).

### 5. Security

- Toast credentials + a **dedicated new Databricks PAT** (used only for Volume
  uploads; own rotation cadence) live in **GCP Secret Manager**, injected as
  env vars into the Cloud Run Job.
- Container service account: minimal roles (run the job, read its secrets).
- Nothing sensitive in image, repo, or logs (extraction logs day/row counts
  only — consistent with the repo's public-financials stance).
- Local `.env` remains for dev; PII stripping at extraction unchanged.

### 6. Failure handling

- Cloud Scheduler retry on transient failure; **Cloud Monitoring alert →
  email on daily-job failure** (mirror of the Databricks-side job alerts).
- Missed intraday poll: healed by the next poll. Missed day: healed by the
  next morning's 3-day window. Both lanes independent.
- Task Scheduler + `morning_extract.ps1` retired only after a 2–3 day
  parallel-run parity check; script stays in the repo as documented manual
  fallback (it remains the proven gap-backfill tool).

### 7. Cost

~45 short runs/day ≈ <1 vCPU-hour/day — inside Cloud Run free tier. Scheduler:
2 of 3 free jobs. Registry: pennies/month. Databricks: unchanged. Net new
spend ≈ $0–2/month.

## Phasing

- **Phase 1 (PC retirement):** Task 0 guided GCP setup (project, billing link,
  APIs, Artifact Registry, Secret Manager, service account) → `rfa-sync` +
  `to_volume.py` + Dockerfile (TDD) → deploy → daily schedule live → parallel
  run vs. PC extract 2–3 days → parity verified → retire Task Scheduler.
- **Phase 2 (intraday):** intraday schedule → today-view dbt model → dashboard
  tile → live verification during service hours.

Each phase gets its own implementation plan and the standard two-stage review
discipline.

## Success criteria

1. Daily partitions land on the Volume with the PC off (proven across ≥3
   consecutive days).
2. Nightly job consumes them unchanged; freshness identical to current state.
3. During service hours, today's partition on the Volume is ≤15 min stale and
   the dashboard "Today" tile reflects it.
4. Failure of the daily sync produces an email within the hour.
5. No secrets in image/repo/logs; Secret Manager is the only credential store
   on the GCP side.

## Out of scope

Toast webhooks (future push-based upgrade — Cloud Run service endpoint is the
natural home when wanted); intraday updates to marts/forecast; labor/inventory
sources; any Databricks workspace migration.
