# Databricks Cloud Pipeline (Productionization) — Design Spec

- **Date:** 2026-06-29
- **Status:** Draft (awaiting review)
- **Author:** August Turner
- **Repo:** `restaurant-forecast-analytics` (`E:\PyProj\`)
- **Builds on:** [2026-06-18 forecasting design spec](2026-06-18-restaurant-sales-forecast-design.md). This spec evolves the working local-batch pipeline into a scheduled, cloud-native Databricks pipeline.

---

## 1. Summary

Move the **entire ETL onto a paid Databricks workspace** as a single scheduled **Workflow** —
Toast extraction, bronze load, dbt transforms, and the Prophet forecast all run in the cloud,
nightly, defined as code via **Databricks Asset Bundles**. The forecast outputs become **Delta
tables** in the data model (replacing the two CSVs). A free **Databricks AI/BI (Lakeview)
dashboard** reads the marts live; **Tableau Public** remains the public portfolio piece, fed a
daily extract via Google Sheets.

Goal: demonstrate end-to-end **cloud analytics-engineering** capability (lakehouse + IaC +
orchestration + serving + BI) for the [[project_job_search]] target role, while producing a
pipeline that can become the restaurant's real operational tool.

## 2. Motivation

- **Cloud demonstration.** The current pipeline is excellent but runs locally and on-demand.
  Putting the whole thing in a scheduled Databricks Workflow, as code, is the Analytics-Engineer
  signal worth showing.
- **Freshness.** Today the lakehouse only advances when the CLIs are run by hand (it sat 3 days
  stale). A nightly Workflow keeps `fct_daily_sales` and the forecast current automatically.
- **Real-business path.** A paid (commercial) workspace removes the Free-Edition non-commercial
  limit, so this can graduate into the restaurant's actual forecasting tool.

## 3. Constraints (the facts that shape the design)

- **Databricks Free Edition blocks outbound internet** → a Free notebook/job cannot call the
  Toast API. This is *why* extraction currently runs locally, and why "the whole ETL on
  Databricks" **requires a paid workspace** (paid compute has outbound internet).
- **Free Edition is non-commercial** → an automated pipeline for an operating business needs the
  paid tier regardless.
- **Tableau Public cannot hold a live Databricks connection** (it requires extracts; live
  Databricks needs Tableau Cloud/Desktop, ~$70+/mo — out of budget). So Tableau Public is fed a
  **daily Google-Sheets extract**, and the *live* view is a free **Databricks AI/BI dashboard**.
- **Budget: ~$50/mo.** Achievable: Databricks Premium serverless, one small nightly job for one
  restaurant, auto-stop → realistically low-tens of $/mo. Google Sheets, Tableau Public, and
  AI/BI dashboards are $0.

## 4. Goals / Non-goals

**Goals**
- Full ETL (extract → load → dbt → forecast → publish) as one scheduled Databricks Workflow.
- Forecast results as first-class **Delta tables** in the marts, not files.
- Pipeline defined **as code** (Databricks Asset Bundle) and version-controlled.
- A live **AI/BI dashboard** + an auto-refreshing **Tableau Public** dashboard.
- **Reuse the existing tested code** (`ingest`/`load`/`forecast`, 40 pytest) — tasks are thin wrappers.

**Non-goals (YAGNI)**
- No rewrite of the modeling logic — Prophet config and the backtest are unchanged.
- No streaming / sub-daily (still the deferred Phase-3 item in the prior spec §19).
- No migration off dbt — dbt stays as a Workflow task (it's a target skill and already built).
- No Tableau-paid live connection (explicitly out of budget; AI/BI covers "live").
- No multi-restaurant / multi-tenant.

## 5. Target architecture

A single Databricks **Workflow** (Job), scheduled nightly, with five tasks plus two read surfaces:

```
Databricks Workflow: restaurant_forecast_nightly  (cron, nightly, serverless)
  task extract  → ingest.extract     → Toast API (paid: outbound internet) → Parquet in a UC Volume
  task load     → load.run_load      → UC Volume Parquet → bronze_orders (Delta, incremental)
  task dbt      → dbt build          → stg_orders → fct_daily_sales (+ tests)
  task forecast → forecast.run_forecast → forecast_daily_sales + model_metrics (Delta)
  task publish  → publish.to_sheets  → push forecast_vs_actuals + model_metrics → Google Sheets

Read surfaces:
  Databricks AI/BI (Lakeview) dashboard → reads marts LIVE (free, native)
  Tableau Public workbook               → connects to the Google Sheet, auto-refreshes daily
```

Tasks run in dependency order (`extract → load → dbt → forecast → publish`). Secrets live in a
**Databricks secret scope**. The Job, tasks, schedule, and (optionally) the dashboard are declared
in `databricks.yml` (Asset Bundle) and deployed with `databricks bundle deploy`.

## 6. Components

- **`ingest.extract`** (reused) — runs in-cloud unchanged except `out_dir` points at a **UC Volume**
  path instead of local `data/raw/`. Toast creds come from Databricks Secrets (via env).
- **`load.run_load`** (reused, + **incremental**) — today it reloads *every* partition; for a nightly
  job it should load only new/changed business dates. Add an incremental mode (load dates present in
  the Volume but absent/stale in bronze, or a bounded last-N-days window).
- **dbt** (reused) — `dbt build` as a Workflow **dbt task**; profile targets the workspace warehouse
  via the job's identity.
- **`forecast.run_forecast`** (reused) — same backtest + Prophet, but **writes Delta tables** (see §7)
  via a new `load.load_forecast` writer. CSV export retained during transition, removed once the
  Sheets bridge is live.
- **`publish.to_sheets`** (new) — reads `forecast_vs_actuals` + `model_metrics`, writes them to a
  Google Sheet via a service account (`gspread`). Tableau Public refreshes from that Sheet.
- **Asset Bundle (`databricks.yml`)** (new) — declares the Job (tasks via `python_wheel_task` against
  the packaged repo + a `dbt_task`), the nightly schedule, and serverless compute. Reproducible IaC.
- **AI/BI dashboard** (new) — a Lakeview dashboard over `forecast_vs_actuals` and `model_metrics`;
  built in-workspace (optionally captured as a bundle resource).

## 7. Data model additions

The two tables are **written by the forecast step into the default schema** (alongside
`bronze_orders`) and declared to dbt as a **source** — dbt owns only what it builds. The
marts-grade serving surface is the `forecast_vs_actuals` **view**.

- **`forecast_daily_sales`** — grain: `forecast_date`. Columns: `forecast_date` (INT YYYYMMDD),
  `yhat`, `yhat_lower`, `yhat_upper` (DOUBLE), `model` (STRING), `run_ts` (TIMESTAMP). Overwritten
  each run (latest 14-day horizon).
- **`model_metrics`** — grain: `model` × `run_ts`. Columns: `model` (STRING), `mae`, `rmse`, `mape`,
  `wape` (DOUBLE), `horizon`, `n_folds` (INT), `run_ts` (TIMESTAMP). Append-only (keeps a history of
  backtest results across runs).
- **`forecast_vs_actuals`** — a dbt **view** joining `fct_daily_sales` (actuals) to the latest
  `forecast_daily_sales` (forecast + band) on date. This replaces the `export_tableau` CSV shape and
  becomes the single source for both the AI/BI dashboard and the Google-Sheets/Tableau feed.

`forecast_lower`/`upper` are already clipped at $0 by `clip_nonnegative` (carried from the prior work).

## 8. Phasing

The paid workspace is a hard dependency I cannot provision; work splits cleanly:

- **Phase 1 — now, on the current Free workspace (forward-compatible):**
  - `load.load_forecast` + write `forecast_daily_sales` / `model_metrics` from `run_forecast`
    (works on Free today via the SQL connector — the win requested).
  - `forecast_vs_actuals` dbt view.
  - Author + `databricks bundle validate` the Asset Bundle and cloud task entrypoints (committed,
    not yet deployed).
  - Incremental `load` mode.
- **Phase 2 — after upgrade to paid (user provisions Premium / 14-day trial):**
  - Create the UC Volume + secret scope; deploy the bundle; point `extract` at the Volume; schedule
    the Job; verify `%pip install prophet` on the chosen compute (fallback: job-cluster init script
    or bundle the wheel with prophet).
  - Build the AI/BI dashboard; set up the Google service account + Sheet; connect Tableau Public.

## 9. Security & secrets

- **Databricks secret scope** holds Toast `clientId`/`clientSecret`/restaurant GUID and the Google
  service-account JSON. No secrets in the bundle or repo. `.env` remains for local dev only.
- **Google service account** has edit access to a single dedicated Sheet (least privilege).
- PII handling is unchanged — extraction still allowlists, so bronze/marts/Sheet/Tableau hold **zero PII**.
- The published figures remain the owner's real aggregates (per the prior spec §14).

## 10. Cost

Databricks Premium, serverless job compute with auto-stop, one nightly run over one restaurant's
data → realistically **low-tens of $/mo**. AI/BI dashboards, Google Sheets, and Tableau Public are
**$0**. Total comfortably within the ~$50/mo budget. Evaluate on the **14-day trial** before committing.

## 11. Testing & reliability

- **Reuse** the 40 pytest (ingest/load/forecast logic unchanged).
- **New tests:** `load_forecast` (DDL + insert shape, like `load_to_delta`); the incremental-load
  date-selection logic; `publish.to_sheets` frame shaping (mock the Sheets client).
- **`databricks bundle validate`** in CI for the IaC.
- **Idempotency:** `forecast_daily_sales` overwritten per run; bronze incremental load is
  DELETE+INSERT per business date (already idempotent); the Job is safe to re-run.
- **Failure handling:** Toast retries/backoff already exist; Workflow task retries configured in the
  bundle; a failed task fails the run without partial-publishing stale data downstream.

## 12. Open questions (resolve at Phase-2 kickoff)

- Does the chosen compute allow `%pip install prophet` (cmdstanpy needed the `==1.2.4` pin locally)?
  If not: job-cluster init script, or ship a wheel with prophet pinned.
- Are AI/BI dashboards + Jobs both available on the selected paid tier? (Premium: yes.)
- Extract-direct-to-bronze vs keep the Parquet→Volume landing? (Lean: keep the landing for parity
  and replayability.)
- Capture the AI/BI dashboard as a bundle resource (full IaC) or build it in-UI? (Start in-UI.)

## 13. Milestones

- **C1 — Forecast tables (Phase 1):** `forecast_daily_sales`, `model_metrics`, `forecast_vs_actuals`
  view; `run_forecast` writes them; tests green. *(No paid dependency.)*
- **C2 — Incremental load + cloud entrypoints (Phase 1):** incremental `load`; task entrypoints; the
  Asset Bundle authored + validated.
- **C3 — Deploy on paid (Phase 2):** Volume + secrets; bundle deployed; extraction in-cloud; nightly
  schedule; one successful end-to-end cloud run.
- **C4 — Serving (Phase 2):** AI/BI dashboard; Google Sheets publish task; Tableau Public connected +
  auto-refreshing.
- **C5 — Polish:** README/writeup updated with the cloud architecture + the paid-tier cost notes; CI runs `bundle validate`.

## 14. Success criteria (verifiable)

1. `forecast_daily_sales` and `model_metrics` exist in the lakehouse (default schema, declared as a
   dbt source), the `forecast_vs_actuals` view exists in marts, and the forecast run populates them
   (provable on Free in Phase 1).
2. The Asset Bundle `validate`s; on paid it `deploy`s and the nightly Workflow completes end-to-end
   (extract → publish) in the cloud with no local steps.
3. The lakehouse advances automatically each night (freshness ≤ 1 day on open days).
4. A live AI/BI dashboard and an auto-refreshing Tableau Public dashboard both read Databricks-produced data.
5. No secrets in the repo/bundle; zero PII downstream; CI green incl. `bundle validate`.
