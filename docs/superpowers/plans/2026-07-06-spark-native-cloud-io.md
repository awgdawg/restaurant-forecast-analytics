# Spark-Native Cloud I/O Implementation Plan (Phase 2b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the in-cloud `load` and `forecast` tasks **Spark-native** — reading the UC Volume and reading/writing UC tables through the job's own Spark session instead of the `databricks-sql-connector` — so they run on the trial workspace's serverless compute, whose egress allowlist blocks even the workspace's public SQL endpoint.

**Why (evidence):** Cloud runs prove serverless job compute cannot open a SQL-warehouse session (`thrift ... OpenSession` network retries; same warehouse reachable from local). The SQL-connector path is caged by the same trial-tier egress allowlist that blocks the Toast API (probes: only pypi/googleapis resolve). Spark-native I/O needs no network egress — the compute plane talks to UC directly. This is also the more Databricks-idiomatic architecture. **Scope honesty: this does NOT fix the extract task** (Toast API egress) — the local 08:45 morning script remains the interim extractor, and the Databricks support ticket for full egress stays open.

**Architecture:** A small **I/O seam**: in-cloud, tasks detect an active Spark session (`pyspark` importable + active session) and use Spark I/O; locally they keep the SQL connector exactly as today (zero local behavior change, all 59 tests keep passing untouched). The incremental-load DECISION logic (`days_needing_load`, `parquet_day_counts`) is already pure and reused as-is — on serverless, the Volume is FUSE-mounted at `/Volumes/...` so `pyarrow` metadata reads work unchanged. Bronze writes use Delta `replaceWhere` per business_date (same idempotent day-replacement semantics as `load_day`). Forecast keeps its pandas/Prophet core; only its table I/O goes through the seam. The bundle's entry points stay the same (auto-detection means no bundle changes beyond re-enabling whatever the spike proves).

**Tech Stack:** existing package + `pyspark` (present on serverless, absent locally — detection relies on that), Delta `replaceWhere`, Databricks one-off `jobs submit` spikes.

---

## Scope

- **In scope:** empirical spike (T0); Spark I/O for bronze load (T1) and forecast read/write (T2); bundle/task wiring per spike results incl. dbt placement (T3); green cloud run + freshness (T4); docs/ticket/memory (T5).
- **Out of scope:** extract egress (support ticket, not code); Spark-native rewrite of the local path (stays SQL-connector); dbt-spark session adapter (only if the dbt spike fails do we fall back to *local* dbt as interim, not a new adapter).

**Repo root:** `E:\PyProj\restaurant-forecast-analytics`, branch `cloud-phase2` (continues). CLI prelude as in the Phase-2 plan. Trial workspace facts: catalog `workspace`, warehouse `6d80f9f4dae5c90f`, Volume `/Volumes/workspace/default/raw_orders` (current through business_date 20260705), secret scope `restaurant-forecast` (7 keys), job `restaurant_forecast_nightly` deployed with extract disabled, schedule 09:15 America/Chicago UNPAUSED.

---

## Task 0: Spike — prove the three unknowns on serverless (no repo changes)

One throwaway script, one one-off `jobs submit` (spark_python_task, serverless env), plus a one-off dbt-task run:

- [ ] **(a) FUSE Volume reads:** `pyarrow.parquet.read_metadata('/Volumes/workspace/default/raw_orders/business_date=20260705/orders.parquet').num_rows` → expect 101.
- [ ] **(b) Spark ↔ UC:** `spark.sql("select count(*) from workspace.default.bronze_orders").collect()` and a scratch-table write `spark.sql("create or replace table workspace.default._spike_probe as select 1 as x")` then drop it → both succeed.
- [ ] **(c) dbt task in-cloud:** submit a one-off run with ONLY a `dbt_task` (`dbt build --select stg_orders`, `warehouse_id 6d80f9f4dae5c90f`), `project_directory` pointing at the already-deployed prod bundle files under `/Workspace/Shared/.bundle/restaurant-forecast-analytics/prod/files`; if that path is unavailable, upload the dbt project fresh to a workspace path and point at that. Question answered: does the managed dbt task's warehouse connection route internally, or hit the same egress wall?
- [x] Record all three outcomes verbatim in the report. (a)+(b) green → T1/T2 proceed. (c) decides T3's dbt placement.

**SPIKE RESULTS (2026-07-06, executed):**
- **(a) PASS** — `read_metadata` on the FUSE Volume path returned 101 rows (business_date=20260705).
- **(b) PASS** — active session present; `spark.sql` read bronze (75,659); scratch-table write+drop clean; `spark.read.parquet` on the Volume read 101 rows.
- **(c) PASS-with-config-fix** — the managed dbt task **connected to the warehouse successfully** (adapter registered, project parsed, 8 threads — NO egress wall), then failed on `UC_HIVE_METASTORE_DISABLED_EXCEPTION`: the auto-generated profile has no catalog, so dbt defaulted to the disabled legacy `hive_metastore`. **T3 fix: declare `catalog: workspace` + `schema: default` on the `dbt_task`** in the bundle. dbt STAYS in the cloud job.

## Task 1: Spark-native bronze load (TDD on the seam)

**Files:** Create `load/spark_io.py`, `tests/test_spark_io.py`; Modify `load/run_load.py`

- [ ] `load/spark_io.py`: `active_spark()` → returns an active SparkSession or None (lazy `from pyspark.sql import SparkSession` inside; `SparkSession.getActiveSession()`; ImportError → None). `spark_day_counts(spark, table)` → dict via `spark.sql` GROUP BY (mirrors `bronze_day_counts`). `spark_load_day(spark, table, business_date, parquet_path)` → read `spark.read.parquet(parquet_path)`, **project to the bronze column order** (`df.select(*COLUMNS)` — by-name resolution guards against parquet column-order drift), then idempotently replace that day: `df.write.format("delta").mode("overwrite").option("replaceWhere", f"business_date = {int(business_date)}").saveAsTable(table)` — mirrors `load_day`'s DELETE+INSERT semantics. Returns `df.count()`. **PRIMARY idiom is saveAsTable + replaceWhere**; the implementer smoke-verifies it once against the live runtime (a one-day re-load) before wiring, and adjusts only if the runtime rejects it (document any deviation).
- [ ] `load/run_load.py` `main()`: after parsing args — `spark = active_spark()`; **structural branch**: if spark → Spark path (same `parquet_day_counts(root)` + `days_needing_load(..., window)` decision over the FUSE path, per-day `spark_load_day`, same emit lines, `connect()` NEVER called); else → existing `connect()` path byte-identical.
- [ ] TDD, concretely: FakeSpark exposes `.sql(q)` (recorded, returns a FakeDF whose `.collect()` yields preset day-count Rows) and `.read.parquet(path)` (recorded, returns a FakeDF with `.select(*cols)` returning itself, `.count()` preset, and a `.write` chain recorder capturing format/mode/options/table). Tests: decision logic picks the right days; `spark_load_day` issues exactly one write with the right replaceWhere predicate and projected columns; `active_spark()` returns None locally (real assertion — pyspark is absent from the venv; verify with an import-fails test). NO pyspark in local requirements.
- [ ] Full local suite green (59 + new). Commit `feat: spark-native bronze load path (serverless in-cloud I/O)`.

## Task 2: Forecast I/O through the seam (TDD)

**Files:** Modify `forecast/data.py`, `forecast/run_forecast.py`; Create/extend tests

- [ ] `forecast/data.py`: `load_daily_series_spark(spark, table=None)` → `spark.sql(f"select business_date, net_sales from {table or mart_table()} order by business_date").toPandas()` → `clean_daily_series`. Reuses everything pure.
- [ ] `forecast/run_forecast.py` — **structural seam, detected ONCE**: `spark = active_spark()` at the top of `main()`. When spark is active, `connect()` is NEVER called (neither the line-29 series read nor the line-64 persistence connection): series comes from `load_daily_series_spark(spark)`; persistence goes through new Spark writers. When spark is None, the existing code path is byte-identical.
- [ ] **Spark writers (in `load/spark_io.py`): explicit schemas are load-bearing.** `spark.createDataFrame` on `forecast_rows()` output CANNOT infer types when the baseline wins (band columns all-None → un-inferrable). Define a `StructType` mirroring `_FORECAST_TYPES` — `forecast_date` LongType, `yhat` DoubleType, `yhat_lower`/`yhat_upper` **DoubleType nullable**, `model` StringType, `run_ts` TimestampType — and a metrics StructType mirroring `_METRICS_TYPES` (symmetry). `spark_write_forecast(spark, fc, model, run_ts)`: rows from the existing pure `forecast_rows()`, `createDataFrame(rows, schema)`, `mode("overwrite").saveAsTable(FORECAST_TABLE)` (DELETE-all-equivalent, latest-only). `spark_write_metrics(...)`: rows from `metrics_rows()`, `mode("append")`.
- [ ] **CSV exports**: guard ONLY the I/O — `write_exports(...)` and its print run under `if spark is None`; the pure builders may run or be folded into the guard, but no filesystem write happens in-cloud.
- [ ] TDD with the FakeSpark from Task 1 (extended with a `createDataFrame` recorder verifying the schema's nullable band fields and append-vs-overwrite modes; one test drives the baseline shape — band all-None — through the writer). Local suite green. Commit `feat: spark-native forecast I/O (schema-pinned writes)`.

## Task 3: Wire the job per spike results

- [ ] Bundle: load/forecast tasks unchanged (same entry points — detection does the work). If spike (c) PASSED: dbt task stays. If FAILED: comment dbt task out with an INTERIM note (like extract) and extend the local morning script with `dbt build` + document; ticket covers both.
- [ ] Redeploy prod; validate. Commit `feat: wire cloud job per dbt spike result`.

## Task 4: Green cloud run + freshness

- [ ] `bundle run` → load (Spark) + [dbt] + forecast (Spark) all green. Fetch each task's output tail as proof.
- [ ] Freshness: bronze max business_date **20260705**, count ≈ 75659+691=76350 (691 = 97+101+101+144+147+101 for 6/30..7/05); mart max 20260705; forecast tables re-written with a fresh run_ts.
- [ ] Commit any final wiring — **checkpoint: Spark-native cloud I/O complete.**

## Task 5: Docs + ticket + memory

- [ ] README/bundle comments updated to the as-built (Spark-native in-cloud I/O; SQL connector local; extract interim).
- [ ] Support-ticket draft (docs/databricks-egress-ticket.md): trial workspace, serverless egress allowlist blocks public APIs + own SQL endpoint despite Allow-all policy + payment method; ask for full egress enablement; include probe evidence.
- [ ] Memory: trial-tier egress reality, Spark-native seam design, SQL-scoped job token.
- [ ] Commit `docs: spark-native as-built, egress ticket draft` — plan complete.

---

## Self-review

- Spike-before-code prevents building on unverified serverless behavior ✅. Local behavior byte-identical (seam returns None) ✅. Idempotency semantics preserved (replaceWhere per day = load_day's DELETE+INSERT) ✅. Extract honesty: unchanged, still local, ticket open ✅. dbt decision deferred to evidence ✅.
