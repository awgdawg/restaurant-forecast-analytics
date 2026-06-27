# Restaurant Sales & Labor Forecasting — Design Spec

- **Date:** 2026-06-18
- **Status:** Draft (awaiting review)
- **Author:** August Turner
- **Repo:** `restaurant-forecast-analytics` (new project on `E:\PyProj\`)

---

## 1. Summary

Build an end-to-end analytics-engineering pipeline that pulls a single restaurant's
**live Toast POS data via API**, models it with **dbt on a Databricks lakehouse**,
**forecasts daily sales** (and, in Phase 2, labor demand) with Python time-series
models, and publishes an interactive **forecast-vs-actuals dashboard to Tableau Public**
that is embedded in the portfolio site.

The project targets two named skill gaps — **forecasting/predictive modeling** and
**Tableau** — in one build, and is architected so it can later graduate into an
operational tool for the business on a paid Databricks workspace.

## 2. Goals

- Demonstrate **live REST API ingestion** (Toast Standard API Access, read-only).
- Demonstrate a modern **lakehouse + dbt** transformation layer (Databricks + `dbt-databricks`).
- Demonstrate **time-series forecasting that beats a naive baseline** (forecasting gap).
- Demonstrate **Tableau** via a published, embedded dashboard (Tableau gap).
- Produce a polished, **public portfolio artifact** (real results shown; secrets never committed).
- Preserve a clean **upgrade path** to a paid/operational Databricks deployment.

## 3. Non-Goals (YAGNI)

- No write-back to Toast — read-only only.
- No real-time/streaming — daily batch is sufficient.
- No multi-location/multi-tenant design — single restaurant.
- No commercial/operational deployment in v1 (Free Edition is non-commercial).
- **Shopify deferred** — reserved for a future channel/cohort/financial-attribution project.
- **Labor forecasting deferred to Phase 2** — daily sales forecast is the MVP.
- **Inventory / recipe forecasting deferred** — high-value future direction (see §19).

## 4. Skill-gap mapping

| Skill gap | How this project hits it |
| --- | --- |
| Forecasting / predictive modeling | Daily sales forecast: baseline → Prophet/SARIMA, rolling backtest |
| Tableau | Published Tableau Public dashboard, embedded in the portfolio site |
| *(future)* Financial attribution | Recipe/BOM → COGS & food-cost %; menu/channel margin (see §19) |
| *(future)* Inventory / purchasing | Recipe/BOM × item sales → ingredient demand forecast & PO recommendations (see §19) |
| *(future)* Cohort analysis | Shopify customer/order cohorts (separate project) |

## 5. Data sources

### Toast (primary)

- **Access:** Toast **Standard API Access** (read-only) — self-serve for U.S. customers on
  **RMS Essentials or higher** with the **Manage Integrations** permission. Generated from
  Toast Web → Toast Partner Integrations. Yields a `clientId` + `clientSecret` and the
  **restaurant GUID**.
- **Auth:** `POST {base}/authentication/v1/authentication/login` with client credentials →
  dynamic **Bearer token** (refresh on expiry). Restaurant scoping via the
  `Toast-Restaurant-External-ID` header.
- **Base URL:** production `https://ws-api.toasttab.com`.
- **Endpoints** *(confirm exact paths + granted scopes against the dev docs at build time)*:
  - **Orders** — `/orders/v2/ordersBulk` → checks, line items, amounts, timestamps,
    dining option (dine-in / takeout / delivery), daypart → core sales signal.
  - **Labor** — `/labor/v1/timeEntries`, `/labor/v1/shifts`, `/labor/v1/employees`
    → hours worked, labor cost (Phase 2).
  - **Config / Menus** — `/config/v2/menus` → item ↔ category mapping.
  - **Restaurant** — restaurant metadata / GUID confirmation.

### Shopify (deferred)

- Self-serve Admin API token (custom app). **Out of scope for v1**; noted as a future
  online-channel / cohort extension.

## 6. Architecture & data flow

```
Toast API ──(local) ingest/──▶ raw Parquet ──load──▶ Databricks Delta (bronze)
   └▶ dbt-databricks: staging (silver) ──▶ marts (gold: daily & daypart sales, labor)
        └▶ notebook: Prophet / SARIMA forecast ──▶ forecast table (gold)
             └▶ export ──▶ CSV/Hyper extract ──▶ Tableau Public ──▶ embed in portfolio
```

**Why extraction runs locally (not inside Databricks):** Free Edition restricts outbound
internet, so the Toast API pulls run on the local machine via `requests`, then the raw
output is loaded **into** the lakehouse. This also keeps extraction portable/testable and
ports cleanly to in-platform ingestion on a paid workspace.

## 7. Tech stack

- **Python 3.10+** — `requests`, `pandas`, `pyarrow`, `python-dotenv`, `prophet`,
  `statsmodels` / `pmdarima`.
- **Databricks Free Edition** — Delta Lake, Unity Catalog volume (raw landing),
  serverless compute, the single pre-provisioned **2X-Small Serverless Starter Warehouse**
  (dbt target).
- **dbt-databricks** — staging + marts; auth via a Databricks **PAT** stored in an env var.
- **Tableau Public** (free) — dashboard + publishing.
- **GitHub Actions** (CI), **pre-commit** (`ruff`, `sqlfluff`).

## 8. Repository structure

```
restaurant-forecast-analytics/
├─ ingest/
│  ├─ toast_client.py      # auth → Bearer; paginated GET (orders, labor, config); retry
│  └─ extract.py           # CLI: pull a date range → raw Parquet (partitioned by date)
├─ load/
│  └─ load_to_delta.py     # raw Parquet → Delta bronze (MERGE, idempotent by date)
├─ models/                 # dbt
│  ├─ staging/             # stg_* (typed, cleaned)
│  └─ marts/               # fct_daily_sales, fct_daypart_sales, fct_labor, dims
├─ notebooks/
│  └─ forecast_sales.py    # baseline → Prophet/SARIMA + rolling backtest → forecast table
├─ export/
│  └─ export_extract.py    # marts/forecast → CSV/Hyper extract for Tableau Public
├─ tableau/                # .twb workbook
├─ tests/                  # pytest (client/transforms)
├─ seeds/                  # dbt seeds
├─ .github/workflows/      # CI
├─ docs/                   # spec, writeup, screenshots
├─ .env.example
├─ profiles.yml.example
├─ dbt_project.yml
├─ requirements.txt
├─ .pre-commit-config.yaml
├─ .gitignore
└─ README.md
```

## 9. Components

Each unit has one clear purpose, a defined interface, and explicit dependencies.

- **`toast_client.py`** — *What:* authenticate and fetch raw Toast data.
  *Interface:* `ToastClient(creds).get_orders(start, end)`, `.get_labor(...)`, `.get_menus()`.
  *Depends on:* `requests`, env-supplied credentials + restaurant GUID.
- **`extract.py`** — *What:* CLI to pull a date range and write partitioned raw Parquet.
  *Interface:* `python -m ingest.extract --start 2025-01-01 --end 2025-01-31 [--full-refresh]`.
  *Depends on:* `toast_client`, local filesystem (`data/raw/`, gitignored).
- **`load_to_delta.py`** — *What:* land raw Parquet into Delta **bronze**, idempotent by
  business date. *Depends on:* Databricks workspace + UC volume.
- **dbt `staging/`** — *What:* type/clean/standardize bronze into `stg_*` silver views.
- **dbt `marts/`** — *What:* business-grain gold tables (see §10).
- **`forecast_sales.py`** (notebook) — *What:* train baseline + models, backtest, write
  `forecast_daily_sales`. *Depends on:* marts, `prophet`/`statsmodels`.
- **`export_extract.py`** — *What:* export marts + forecast to a Tableau extract
  (CSV/Hyper). *Depends on:* marts + forecast table.
- **`tableau/` workbook** — *What:* forecast-vs-actuals dashboard on the exported extract.

## 10. Data model (marts)

- **`dim_date`** — calendar with day-of-week, US holiday flags, daypart boundaries.
- **`dim_menu_item` / `dim_menu_category`** — menu hierarchy (from config API).
- **`fct_daily_sales`** — grain: `business_date`. Measures: `net_sales`, `gross_sales`,
  `order_count`, `guest_count`, `discount_total`, `void_total`, channel split
  (dine-in / takeout / delivery).
- **`fct_daypart_sales`** — grain: `business_date` × `daypart`.
- **`fct_labor`** *(Phase 2)* — grain: `business_date` (× `role`). Measures: `hours`,
  `labor_cost`, `headcount`.
- **`forecast_daily_sales`** — grain: `forecast_date` × `model`. Columns: `yhat`,
  `yhat_lower`, `yhat_upper`, `model`, `run_ts`.

**Reconciliation rule:** `fct_daily_sales.net_sales` per day must tie to Toast's own daily
sales-summary total within tolerance (see §13).

## 11. Forecasting approach

- **Target (MVP):** daily `net_sales`. **Extension:** daypart-level. **Phase 2:** labor hours/demand.
- **Seasonality / regressors:** weekly (day-of-week), annual (if history allows), US holidays,
  known closures/events; weather is a future enhancement.
- **Models:**
  1. **Baseline** — seasonal-naive (same weekday last week / last year).
  2. **Prophet** — additive, weekly + yearly seasonality, holiday regressors.
  3. **SARIMA / `auto_arima`** — comparison model.
- **Validation:** rolling-origin (expanding-window) backtest; forecast horizon **14 days**;
  metrics **MAPE / RMSE / MAE** vs. baseline.
- **History requirement:** confirm available Toast history at ingest (M1). `< 12 mo` →
  weekly-only seasonality; `≥ 18–24 mo` → enable yearly seasonality. Document the actual depth.
- **Execution:** runs in a Databricks notebook if Free Edition permits the needed `%pip`
  installs (e.g. `prophet`); otherwise the *same code* runs locally against the exported
  marts — so a library/outbound constraint never becomes a hard blocker.

## 12. Reliability & error handling

- **API:** exponential backoff + jitter on `429`/`5xx`; re-auth on `401`/token expiry;
  honor rate limits; paginate via cursor/page tokens; bounded date-window pulls.
- **Idempotency:** raw pulls keyed by business date; re-runs overwrite/MERGE that date's
  partition (no duplicates).
- **Incremental:** default last-N-days incremental; `--full-refresh` for backfill.
- **Load:** raw written as date-partitioned Parquet → `MERGE` into Delta bronze.

## 13. Testing & quality

- **pytest** — `toast_client` (auth, pagination, retry, parsing) against mocked HTTP;
  transform helpers.
- **dbt tests** — `not_null`/`unique` on keys; relationships; `accepted_values`
  (daypart, channel); a **custom reconciliation test** (marts vs. Toast daily summary,
  tolerance ≤ 0.5%); freshness/row-count.
- **pre-commit** — `ruff` (Python), `sqlfluff` (SQL), whitespace/EOF.
- **CI (GitHub Actions)** — lint + pytest + `dbt build` against a CI target (or compile-only
  if no warehouse is wired into CI) on each PR.

## 14. Security, secrets & data handling

- **Secrets:** `clientId`/`clientSecret`, Databricks **PAT**, restaurant GUID → `.env`
  (gitignored) locally and **Databricks Secrets** in-platform. Never committed.
  `.env.example` documents required variables. dbt `profiles.yml` uses `env_var()`; only
  `profiles.yml.example` is committed.
- **`.gitignore` from the first commit** covers `.env*`, `*.pem`/`*.key`/`*.p12`,
  `credentials.json`, `token.json`, `secrets/`, plus `data/`/`raw/`/`*.parquet`/`*.hyper`/
  `*.twbx` (the last group for **repo hygiene** — bulk data lives in the lakehouse, not git).
- **Business financials are published as real numbers.** Per the owner's decision, the
  dashboard and writeup use the restaurant's **actual** figures (no scaling or relabeling) —
  it's the owner's data and their call to share. Code, methodology, and real results are public.
- **Customer PII — stripped from the forecast pipeline.** Toast orders include
  `checks[].customer.{email,firstName,lastName,phone}`, `deliveryInfo.*` addresses, and
  payment-card fields. The forecast needs none of it, so `extract.py` applies an **allowlist**
  (businessDate, check/selection amounts, quantities, `numberOfGuests`, `diningOption`,
  void/delete flags, timestamps) and **drops everything else before writing to disk**. An
  allowlist (not a blocklist) ignores any new PII field Toast adds, and excludes card/PCI data
  for free. A unit test asserts no contact keys survive → bronze, marts, and Tableau Public
  hold **zero PII**.
- **Using customer data later = a separate, isolated system (privacy-by-design; see §19).**
  Three zones: (1) **public** = aggregates only; (2) **private customer analytics** keyed on a
  **pseudonymized** id (HMAC-SHA256 + a secret salt stored outside the data) for
  repeat/cohort/RFM/profiling — never raw contacts, never individual rows in Tableau Public;
  (3) a **managed ESP** holds real email/name/phone + consent and does the sending. Real
  contacts are joined only at send time and never enter the lakehouse, repo, or (non-commercial)
  Free Edition. Email marketing must honor CAN-SPAM / applicable privacy law and verified opt-in.
- **Employee PII (recommended guard, not a hard requirement):** labor data can include
  employee names and wages. Recommend masking/aggregating individual-employee identifiers in
  any *public* labor view (roll up to totals/roles), since that's staff personal data rather
  than the owner's own figures. Decide at the labor phase (M5).
- **Repo visibility:** intended **public** (portfolio). Acceptable because secrets are
  gitignored; confirm public vs. private at remote-creation time.

## 15. Environment & cost

- **Free Edition:** $0, **non-commercial**. Constraints: serverless-only; single 2X-Small
  Starter SQL Warehouse (dbt target); restricted outbound internet (→ local extract);
  daily quota (ample for one restaurant); no SLA.
- **Paid-upgrade path** (if the business adopts it): commercial **Premium** workspace; add a
  scheduled **Job/Workflow** for nightly refresh; optionally move ingestion in-platform;
  realistically **low-tens of dollars/month** with serverless + auto-stop + Jobs compute
  (cost discipline matters more than data size). Decide via the **14-day trial** after the MVP.
  The architecture ports with minimal rework.

## 16. Milestones

- **M0 — Access & accounts** *(real-world, can start now, in parallel)*: confirm Toast plan
  (RMS Essentials+) and Manage Integrations permission → generate read-only credentials +
  capture restaurant GUID; sign up for Databricks Free Edition (note workspace URL, Starter
  Warehouse HTTP path, create a UC volume + a PAT); create a Tableau Public account.
- **M1 — Ingest:** `toast_client` + `extract.py`; pull a date range of orders (+labor) to
  local Parquet; verify totals against the Toast dashboard; record actual history depth.
- **M2 — Load + transform:** land raw → Delta bronze; dbt staging → marts (daily/daypart
  sales); reconciliation test passes.
- **M3 — Forecast MVP (sales):** baseline + Prophet (+ SARIMA); rolling backtest; beat the
  baseline; write `forecast_daily_sales`.
- **M4 — Tableau + portfolio:** export marts → extract → Tableau Public dashboard
  (forecast vs. actuals, filters, KPI cards); embed/link from the portfolio; README + writeup.
- **M5 — Labor forecast (Phase 2):** `fct_labor` + labor/staffing-demand forecast; extend dashboard.
- **M6 — Polish:** CI green; docs; cost notes; optional paid-trial evaluation.

## 17. Assumptions & open questions

- **Assumption:** portfolio/learning use (Free Edition, non-commercial). Commercial/operational
  decision deferred to post-MVP.
- **Assumption:** project name `restaurant-forecast-analytics` (rename freely).
- **Open:** actual Toast history depth (drives yearly seasonality) — confirm at M1.
- **Open:** exact Toast endpoint paths + scopes for the granted credential set — confirm at M1.
- **Open:** daypart definitions matching the restaurant's actual service windows.
- **Open:** employee-PII handling in published labor views (mask/aggregate) — confirm at M5.
- **Open:** whether Free Edition serverless allows `%pip install` of `prophet`/`pmdarima`;
  if not, forecasting runs locally against exported marts (see §11).

## 18. Success criteria (verifiable)

1. `extract.py` performs a **real Toast API round-trip** — pulls live orders for a date range
   with a valid token.
2. `dbt build` produces the marts and the **reconciliation test ties** daily net sales to
   Toast's summary within tolerance (≤ 0.5%).
3. The forecast **beats the seasonal-naive baseline** on a 14-day rolling backtest (lower MAPE),
   documented with the numbers.
4. A **Tableau Public dashboard** is published and embedded/linked from the portfolio site.
5. Repo is **public-safe**: no secrets committed and no bulk raw-data dumps in git (hygiene);
   **CI green**.
6. `README` documents the architecture, how to run it, the results, and the paid-upgrade path.

## 19. Future roadmap (post-v1)

Captured here so the architecture leaves room for them; not in scope for v1.

- **Labor / staffing forecast** *(Phase 2, M5)* — forecast staffing demand and turn it into a
  recommended schedule. Data-source options: Toast **Labor API** (actual clocked hours) and/or
  the **Sling API** (`api.getsling.com`, self-serve token auth, public docs + examples). Sling
  is **"by Toast"** (the integration syncs employees, wages, schedules, timesheets, and summary
  sales every ~15 min; timesheets in real time) and holds the **planned schedules/shifts** plus
  built-in **scheduled-vs-actual** labor comparison — a strong fit for "forecast demand →
  recommend a schedule → compare to what was actually scheduled." Note: Sling's sales data is
  *summary only*, so the **Toast Orders API stays the source for sales forecasting**.
- **Shopify online channel** — add Shopify Admin API ingestion for online orders; enables
  channel comparison and customer **cohort/retention** analysis (separate skill gaps).
- **Near-real-time intraday view** *(Phase 3)* — a "today vs. forecast" pacing dashboard
  refreshed every ~5–15 min. Freshness is capped by Toast (poll the Orders API every N
  minutes, or webhooks if Toast grants them — webhooks need a hosted public receiver).
  **Tableau Public can't refresh sub-daily**, so this view uses a separate surface.
  Recommended **$0 path:** a local scheduled poller, gated to **operating hours on open days
  only**, → BigQuery free tier → **Looker Studio**. A paid Databricks serverless micro-batch
  (~15–30 min, with a 1-min billing minimum) fits the ~$50/mo budget if ever wanted. Tableau
  Public stays the daily deliverable.
- **Customer Data Platform & marketing** *(privacy-by-design; requested)* — repeat-customer
  analytics, trending, profiling, and email campaigns *without* exposing PII:
  1. **Pseudonymize** `customer.guid`/email via **HMAC-SHA256** (secret salt kept outside the
     data) → a `customer_key` for all customer analytics.
  2. **Private customer-analytics layer** — RFM (recency/frequency/monetary), cohorts,
     repeat-rate, segments — keyed on `customer_key`, no raw contacts; commercial use, so a
     private/paid store (not Free Edition).
  3. **Managed ESP** (e.g. Mailchimp / Klaviyo / Square / Toast Marketing) is the system of
     record for real email/name/phone + **consent** + unsubscribe handling.
  4. **Flow:** compute a target segment (set of `customer_key`s) in analytics → resolve to the
     ESP audience → ESP sends and manages compliance; contacts are joined only at send time.
  5. **Guardrails:** public dashboard shows aggregates only (e.g. % repeat, avg visits); honor
     **CAN-SPAM** / CCPA-CPRA (if applicable) + opt-in; support deletion across the analytics
     store and the ESP.
- **Inventory & ingredient demand forecasting** *(requested)* — the high-value extension:
  1. **Recipe / bill-of-materials (BOM) model** — enter each menu item's component
     ingredients with quantities + units (manual entry → a maintained table/seed).
  2. **Theoretical usage** — join the BOM to **item-level sales** (Toast order lines) to
     compute how much of each ingredient was consumed over any period (depletion).
  3. **Ingredient demand forecast** — extend the sales forecast to **item level**, then
     explode item forecasts through the BOM → forecast **ingredient/supply demand**.
  4. **Purchasing recommendations** — combine forecast demand with on-hand counts, par
     levels, pack sizes, and supplier lead times → recommend **how much of each ingredient
     to order and when**.
  5. **Variance / waste analysis** *(optional)* — compare theoretical usage vs. actual
     inventory counts → surface shrinkage, over-portioning, and food-cost variance.
  - **New data required:** recipes/BOM, ingredient units + pack sizes, supplier/purchasing
    data, and periodic on-hand counts (Toast menu/inventory data may cover part of this).
  - **Skill-gap value:** deepens forecasting (hierarchical / item-level), adds **financial
    attribution** (COGS, food-cost %), and demonstrates operational optimization — strong
    portfolio value *and* real day-to-day value for the business.
