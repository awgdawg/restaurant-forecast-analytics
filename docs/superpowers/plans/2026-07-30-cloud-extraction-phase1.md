# Cloud Extraction Phase 1 (PC Retirement) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the daily Toast extraction on GCP Cloud Run (scheduled 08:30 CT) so the pipeline no longer depends on the owner's PC.

**Architecture:** A new `rfa-sync` entry point (extract via the existing `extract_range` + upload via a new `databricks-sdk` Files-API module) runs as a containerized Cloud Run Job in new project `restaurant-forecast-ops`, credentials in Secret Manager, triggered by Cloud Scheduler. Databricks side unchanged. Spec: `docs/superpowers/specs/2026-07-30-cloud-extraction-design.md`.

**Tech Stack:** existing package + `databricks-sdk`, Docker (built remotely via Cloud Build — no local Docker needed), Cloud Run Jobs, Cloud Scheduler, Secret Manager, `gcloud` CLI.

**Repo root:** `E:\PyProj\restaurant-forecast-analytics`. **Branch:** create `cloud-extraction` from `main` at Task 1. Venv: `.\.venv\Scripts\python.exe`; suite baseline **93 passed, 1 skipped**. NEVER echo secret values.

**Workspace facts:** Volume `/Volumes/workspace/default/raw_orders`; DBX host in `.env` as `DBX_HOST` (bare hostname); established env-shim convention: `DBX_HOST`/`DBX_TOKEN` env vars, code prepends `https://`.

---

## Task 0 (USER + agent): GCP project bootstrap

No repo changes. User drives console/auth; agent runs verifiable CLI steps.

- [ ] **Step 1 (user):** Install the Google Cloud CLI: `winget install Google.CloudSDK` (new terminal afterward so `gcloud` is on PATH). Then `gcloud auth login` (browser flow).
- [ ] **Step 2 (user):** Create the project at https://console.cloud.google.com/projectcreate — name/id `restaurant-forecast-ops` (accept the suffixed id GCP generates if the bare id is taken; report the final PROJECT_ID). Link billing: https://console.cloud.google.com/billing/linkedaccount?project=<PROJECT_ID> → select your billing account.
- [ ] **Step 3 (user):** Mint a NEW Databricks PAT for uploads only: workspace → avatar → Settings → Developer → Access tokens → Manage → Generate new token, comment `gcp-toast-sync`, lifetime 180 days. Add to local `.env` as `DBX_SYNC_TOKEN=<value>` (never into chat).
- [ ] **Step 4 (agent):** Configure + enable APIs (expect each command to end `Operation ... finished successfully`):

```bash
gcloud config set project <PROJECT_ID>
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com
```

- [ ] **Step 5 (agent):** Artifact Registry repo + runtime service account:

```bash
gcloud artifacts repositories create rfa --repository-format=docker --location=us-central1
gcloud iam service-accounts create toast-sync --display-name "Toast sync job"
```

- [ ] **Step 6 (agent):** Create the five secrets, piping values from the sourced `.env` (Git Bash; values are not path-like so no MSYS mangling risk; NEVER echo):

```bash
cd /e/PyProj/restaurant-forecast-analytics && set -a && source .env && set +a
printf %s "$TOAST_CLIENT_ID"       | gcloud secrets create toast-client-id       --data-file=-
printf %s "$TOAST_CLIENT_SECRET"   | gcloud secrets create toast-client-secret   --data-file=-
printf %s "$TOAST_RESTAURANT_GUID" | gcloud secrets create toast-restaurant-guid --data-file=-
printf %s "$DBX_HOST"              | gcloud secrets create dbx-host              --data-file=-
printf %s "$DBX_SYNC_TOKEN"        | gcloud secrets create dbx-token             --data-file=-
```

- [ ] **Step 7 (agent):** Grant the runtime SA access to exactly those secrets:

```bash
PROJECT_ID=$(gcloud config get-value project)
SA="toast-sync@${PROJECT_ID}.iam.gserviceaccount.com"
for s in toast-client-id toast-client-secret toast-restaurant-guid dbx-host dbx-token; do
  gcloud secrets add-iam-policy-binding "$s" --member "serviceAccount:$SA" --role roles/secretmanager.secretAccessor
done
```

- [ ] **Step 8 (agent):** Verify: `gcloud secrets list` shows the 5 names; `gcloud artifacts repositories list --location=us-central1` shows `rfa`.

## Task 1: `ingest/to_volume.py` — Files-API partition upload (TDD)

**Files:** Create `ingest/to_volume.py`, `tests/test_to_volume.py`.

- [ ] **Step 1: Failing tests** — `tests/test_to_volume.py`:

```python
"""to_volume: local partition discovery + Files-API upload (fake client)."""

from datetime import date
from pathlib import Path

from ingest.to_volume import local_partitions, upload_partitions


def _make_partition(root: Path, bd: str) -> Path:
    part = root / f"business_date={bd}"
    part.mkdir(parents=True)
    target = part / "orders.parquet"
    target.write_bytes(b"parquet-bytes-" + bd.encode())
    return target


class FakeFiles:
    def __init__(self):
        self.dirs: list[str] = []
        self.uploads: list[tuple[str, bytes, bool]] = []

    def create_directory(self, path):
        self.dirs.append(path)

    def upload(self, path, contents, *, overwrite):
        self.uploads.append((path, contents.read(), overwrite))


class FakeClient:
    def __init__(self):
        self.files = FakeFiles()


def test_local_partitions_returns_only_existing_days(tmp_path):
    _make_partition(tmp_path, "20260728")
    days = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)]
    assert local_partitions(tmp_path, days) == [
        ("20260728", tmp_path / "business_date=20260728" / "orders.parquet")
    ]


def test_upload_partitions_uploads_each_existing_day(tmp_path):
    _make_partition(tmp_path, "20260728")
    _make_partition(tmp_path, "20260729")
    client = FakeClient()
    n = upload_partitions(
        tmp_path,
        [date(2026, 7, 28), date(2026, 7, 29)],
        "/Volumes/ws/default/raw",
        client=client,
    )
    assert n == 2
    assert client.files.dirs == [
        "/Volumes/ws/default/raw/business_date=20260728",
        "/Volumes/ws/default/raw/business_date=20260729",
    ]
    assert client.files.uploads == [
        (
            "/Volumes/ws/default/raw/business_date=20260728/orders.parquet",
            b"parquet-bytes-20260728",
            True,
        ),
        (
            "/Volumes/ws/default/raw/business_date=20260729/orders.parquet",
            b"parquet-bytes-20260729",
            True,
        ),
    ]


def test_upload_partitions_empty_window_never_builds_client(tmp_path):
    # client=None + no partitions must return 0 WITHOUT constructing a real client
    assert upload_partitions(tmp_path, [date(2026, 7, 28)], "/Volumes/x") == 0
```

- [ ] **Step 2: RED** — `.\.venv\Scripts\python.exe -m pytest tests/test_to_volume.py -q` → ImportError (`ingest.to_volume` missing).
- [ ] **Step 3: Implement** `ingest/to_volume.py`:

```python
"""Upload local Parquet partitions to the UC Volume via the Files API.

The workspace's serverless egress wall blocks Toast (proven exhaustively;
see docs/superpowers/specs/2026-07-30-cloud-extraction-design.md), so
extraction runs OUTSIDE Databricks and pushes partitions here. The SDK import
is lazy and the client honors the repo's DBX_* env convention.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

DEFAULT_VOLUME_ROOT = "/Volumes/workspace/default/raw_orders"


def _workspace_client():
    import os

    from databricks.sdk import WorkspaceClient

    if "DBX_HOST" in os.environ:
        return WorkspaceClient(
            host=f"https://{os.environ['DBX_HOST']}",
            token=os.environ["DBX_TOKEN"],
        )
    return WorkspaceClient()


def local_partitions(root: Path, days: list[date]) -> list[tuple[str, Path]]:
    """(yyyymmdd, parquet path) for each day whose partition exists on disk.
    Zero-order days write no partition and are skipped silently."""
    out = []
    for day in days:
        bd = day.strftime("%Y%m%d")
        target = root / f"business_date={bd}" / "orders.parquet"
        if target.exists():
            out.append((bd, target))
    return out


def upload_partitions(
    root: Path,
    days: list[date],
    volume_root: str = DEFAULT_VOLUME_ROOT,
    *,
    client=None,
    log=None,
) -> int:
    """Upload each existing partition's orders.parquet (overwrite, idempotent).
    Parent directories are created explicitly -- the Files API does not
    reliably create them (the CLI single-file cp provably does not)."""
    emit = log or (lambda _m: None)
    parts = local_partitions(root, days)
    if not parts:
        return 0
    client = client or _workspace_client()
    for bd, path in parts:
        part_dir = f"{volume_root}/business_date={bd}"
        client.files.create_directory(part_dir)
        with open(path, "rb") as f:
            client.files.upload(f"{part_dir}/orders.parquet", f, overwrite=True)
        emit(f"uploaded business_date={bd}")
    return len(parts)
```

- [ ] **Step 4: GREEN** — `.\.venv\Scripts\python.exe -m pytest tests/test_to_volume.py -q` → 3 passed. Full suite → 96 passed, 1 skipped.
- [ ] **Step 5: Commit** — `git checkout -b cloud-extraction` then `git add ingest/to_volume.py tests/test_to_volume.py && git commit -m "feat: Files-API partition upload module (cloud extraction seam)"`.

## Task 2: `ingest/sync.py` — extract-then-upload entry point (TDD)

> **AMENDMENT (2026-07-30, controller):** adopt the empty-window failure guard
> from the salvaged fallback workflow: in `--refresh-days` mode, ZERO uploaded
> partitions across the window is a FAILURE (`sys.exit(1)` after an error
> message) — the restaurant closes only Mondays, so a ≥2-day window can never
> be legitimately empty, and the 2026-07 incident was a silent no-op, not a
> red run. In `--today` mode an empty result is legitimate (pre-open, Monday)
> and exits 0. Add tests for both behaviors.

**Files:** Create `ingest/sync.py`, `tests/test_sync.py`.

- [ ] **Step 1: Failing tests** — `tests/test_sync.py`:

```python
"""sync: window computation + orchestration wiring (fakes, no network)."""

from datetime import date

import pytest

from ingest.sync import business_today, sync_window


def test_sync_window_refresh_days_ends_yesterday():
    start, end = sync_window(3, False, date(2026, 7, 30))
    assert (start, end) == (date(2026, 7, 27), date(2026, 7, 29))


def test_sync_window_today_mode_is_single_day():
    start, end = sync_window(0, True, date(2026, 7, 30))
    assert (start, end) == (date(2026, 7, 30), date(2026, 7, 30))


def test_business_today_uses_chicago_clock():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    fixed = datetime(2026, 7, 30, 22, 30, tzinfo=ZoneInfo("America/Chicago"))
    assert business_today(lambda: fixed) == date(2026, 7, 30)


def test_main_wires_extract_then_upload(monkeypatch, tmp_path):
    calls = {}

    monkeypatch.setattr("ingest.sync.load_toast_config", lambda: {"fake": "cfg"})
    monkeypatch.setattr("ingest.sync.ToastClient", lambda cfg: "fake-client")
    monkeypatch.setattr(
        "ingest.sync.business_today", lambda now_fn=None: date(2026, 7, 30)
    )

    def fake_extract(client, start, end, out_dir, *, overwrite, log):
        calls["extract"] = (client, start, end, out_dir, overwrite)
        return 42

    def fake_upload(root, days, volume_root, *, log):
        calls["upload"] = (root, days, volume_root)
        return 2

    monkeypatch.setattr("ingest.sync.extract_range", fake_extract)
    monkeypatch.setattr("ingest.sync.upload_partitions", fake_upload)

    from ingest.sync import main

    main(["--refresh-days", "2", "--out-dir", str(tmp_path), "--volume-root", "/Volumes/x"])

    client, start, end, out_dir, overwrite = calls["extract"]
    assert (start, end) == (date(2026, 7, 28), date(2026, 7, 29))
    assert overwrite is True
    root, days, volume_root = calls["upload"]
    assert days == [date(2026, 7, 28), date(2026, 7, 29)]
    assert volume_root == "/Volumes/x"


def test_main_rejects_zero_refresh_days():
    with pytest.raises(SystemExit):
        from ingest.sync import main

        main(["--refresh-days", "0"])
```

- [ ] **Step 2: RED** — `pytest tests/test_sync.py -q` → ImportError.
- [ ] **Step 3: Implement** `ingest/sync.py`:

```python
"""Extract-then-upload in one process: the cloud-side replacement for
scripts/morning_extract.ps1. Runs anywhere with open egress (Cloud Run).

Modes:
  rfa-sync --refresh-days 3   # daily self-healing window (ends yesterday)
  rfa-sync --today            # intraday: current business date only
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.extract import extract_range
from ingest.to_volume import DEFAULT_VOLUME_ROOT, upload_partitions
from ingest.toast_client import ToastClient

BUSINESS_TZ = ZoneInfo("America/Chicago")


def business_today(now_fn=None) -> date:
    """Current business date. The restaurant closes at 20:00 local, so the
    business date is today's date in America/Chicago -- no midnight rollover."""
    now = (now_fn or (lambda: datetime.now(BUSINESS_TZ)))()
    return now.date()


def sync_window(refresh_days: int, today_mode: bool, today: date) -> tuple[date, date]:
    """Inclusive [start, end] to extract and upload."""
    if today_mode:
        return today, today
    end = today - timedelta(days=1)
    return end - timedelta(days=refresh_days - 1), end


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Extract Toast orders and upload partitions to the UC Volume."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--refresh-days",
        type=int,
        default=0,
        help="re-pull the trailing N days ending yesterday (self-healing window)",
    )
    mode.add_argument(
        "--today", action="store_true", help="pull only the current business date"
    )
    parser.add_argument("--out-dir", default="data/raw/orders")
    parser.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    args = parser.parse_args(argv)
    if not args.today and args.refresh_days < 1:
        parser.error("--refresh-days must be >= 1")

    load_dotenv()
    start, end = sync_window(args.refresh_days, args.today, business_today())
    client = ToastClient(load_toast_config())
    out_dir = Path(args.out_dir)
    print(f"Syncing {start.isoformat()} -> {end.isoformat()}")
    rows = extract_range(client, start, end, out_dir, overwrite=True, log=print)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    uploaded = upload_partitions(out_dir, days, args.volume_root, log=print)
    print(f"Synced {rows} rows across {uploaded} partitions to {args.volume_root}")
```

- [ ] **Step 4: GREEN** — `pytest tests/test_sync.py -q` → 5 passed. Full suite → 101 passed, 1 skipped.
- [ ] **Step 5: Commit** — `git add ingest/sync.py tests/test_sync.py && git commit -m "feat: rfa-sync orchestration (extract window + Volume upload)"`.

## Task 3: Package wiring + local live verify

**Files:** Modify `pyproject.toml`, `requirements.txt`.

- [ ] **Step 1:** `pyproject.toml`: add `"databricks-sdk>=0.20,<1"` to dependencies (below the capped pins, same comment discipline applies) and `rfa-sync = "ingest.sync:main"` to `[project.scripts]`.
- [ ] **Step 2:** `requirements.txt`: add the pinned equivalent matching the venv (`.\.venv\Scripts\python.exe -m pip show databricks-sdk` for the version; it is already installed as a dbt transitive dep).
- [ ] **Step 3:** Reinstall entry points: `.\.venv\Scripts\python.exe -m pip install -e . --no-deps` then verify `.\.venv\Scripts\python.exe -m ingest.sync --help` prints both modes.
- [ ] **Step 4: Local live verify** (PowerShell, from repo root — uses `.env` DBX_* via the shim; safe, idempotent):

```powershell
.\.venv\Scripts\python.exe -m ingest.sync --refresh-days 2
```

Expected: per-day extract lines, `uploaded business_date=...` lines for existing days, final `Synced N rows across M partitions...`. Then verify Volume timestamps via Git Bash:

```bash
cd /e/PyProj/restaurant-forecast-analytics && set -a && source .env && set +a \
  && DBX_CLI="$(command -v databricks || echo "$LOCALAPPDATA/Microsoft/WinGet/Links/databricks.exe")" \
  && export DATABRICKS_HOST="https://$DBX_HOST" DATABRICKS_TOKEN="$DBX_TOKEN" \
  && MSYS_NO_PATHCONV=1 "$DBX_CLI" fs ls -l "dbfs:/Volumes/workspace/default/raw_orders" | tail -5
```

Expected: trailing partitions show today's modification time.
- [ ] **Step 5:** Full suite + `python -m pre_commit run --all-files` green. Commit: `git add pyproject.toml requirements.txt && git commit -m "feat: rfa-sync entry point + databricks-sdk dependency"`.

## Task 4: Container definition

> **AMENDMENT (2026-07-31, as-built):** three additions beyond the blocks
> below. (1) `ENV PYTHONUNBUFFERED=1` — Cloud Logging must see progress lines
> in real time and must not lose buffered output on SIGKILL (task timeout /
> OOM). (2) A tzdata note on the Dockerfile: `ZoneInfo("America/Chicago")` in
> `ingest/sync.py` needs tzdata, which arrives transitively via `pandas>=2.2`;
> if pandas is ever dropped from the image, add an explicit tzdata dependency
> or `sync.py` dies at import. (3) The `.gcloudignore` below was REPLACED by a
> hardened version: gcloud folds in `.gitignore` only when it AUTO-generates
> the file, so the hand-written one made every gitignored artifact
> upload-eligible — `target/` (1.7MB of dbt artifacts) and
> `.databricks/bundle/prod/resources.json`, which carries the user's email
> into a RETAINED GCS source archive. Fix: a leading `#!include:.gitignore`
> directive plus explicit lines. Order is load-bearing — the include comes
> FIRST so the explicit `data/`/`exports/` lines beat `.gitignore`'s
> `!exports/*.csv` negation (last match wins). `.databricks/` must stay
> explicit: it is ignored via a NESTED `.databricks/.gitignore`, which the
> include directive does not read.

**Files:** Create `Dockerfile`, `.gcloudignore`.

- [ ] **Step 1:** `Dockerfile`:

```dockerfile
# Cloud Run Job image for rfa-sync (extraction moved off-workspace; the
# serverless egress wall blocks Toast -- see the 2026-07-30 design spec).
# Installs the full package for one-source-of-truth deps; image weight from
# prophet/cmdstanpy is accepted (job cold-start is seconds either way).
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY ingest/ ingest/
COPY load/ load/
COPY forecast/ forecast/
COPY publish/ publish/
RUN pip install --no-cache-dir .
ENTRYPOINT ["rfa-sync"]
CMD ["--refresh-days", "3"]
```

- [ ] **Step 2:** `.gcloudignore` (Cloud Build uploads the source dir — exclude everything heavy/secret):

```
.git
.venv
data/
exports/
logs/
dist/
docs/
tests/
.env
*.log
```

- [ ] **Step 3:** Sanity: `.\.venv\Scripts\python.exe -c "print(open('Dockerfile').read().count('COPY'))"` → 5. Commit: `git add Dockerfile .gcloudignore && git commit -m "feat: Cloud Run container for rfa-sync"`.

## Task 5 (agent, gcloud): Build, deploy, first cloud execution

- [ ] **Step 1: Build remotely** (no local Docker; ~5-8 min first time):

```bash
cd /e/PyProj/restaurant-forecast-analytics
PROJECT_ID=$(gcloud config get-value project)
gcloud builds submit --tag "us-central1-docker.pkg.dev/${PROJECT_ID}/rfa/toast-sync:v1" .
```

Expected: `STATUS: SUCCESS`.
- [ ] **Step 2: Create the job** (secrets → env vars matching the repo's shim names):

```bash
gcloud run jobs create toast-sync \
  --image "us-central1-docker.pkg.dev/${PROJECT_ID}/rfa/toast-sync:v1" \
  --region us-central1 \
  --service-account "toast-sync@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets "TOAST_CLIENT_ID=toast-client-id:latest,TOAST_CLIENT_SECRET=toast-client-secret:latest,TOAST_RESTAURANT_GUID=toast-restaurant-guid:latest,DBX_HOST=dbx-host:latest,DBX_TOKEN=dbx-token:latest" \
  --max-retries 1 --task-timeout 900 --memory 1Gi
```

- [ ] **Step 3: Execute once, watch it:**

```bash
gcloud run jobs execute toast-sync --region us-central1 --wait
```

Expected: `... completed successfully`. Fetch logs: `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="toast-sync"' --limit 30 --format "value(textPayload)"` → the familiar `Syncing ... / NNNNNNNN: N orders / uploaded business_date=... / Synced ...` lines.
- [ ] **Step 4: Verify on the Databricks side** — Volume partition mtimes updated (same `fs ls -l` check as Task 3 Step 4). Any failure: diagnose from logs (auth → secret values; DNS/egress should NOT occur on Cloud Run — full internet).
- [ ] **Step 5:** No repo changes; record the image tag + job name in the task report.

## Task 6 (agent, gcloud): Schedule + failure alerting

- [ ] **Step 1: Grant Scheduler the right to run the job** (dedicated invoker SA):

```bash
gcloud iam service-accounts create sync-scheduler --display-name "Scheduler invoker"
gcloud run jobs add-iam-policy-binding toast-sync --region us-central1 \
  --member "serviceAccount:sync-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role roles/run.invoker
```

- [ ] **Step 2: Daily schedule (08:30 America/Chicago):**

```bash
gcloud scheduler jobs create http daily-toast-sync \
  --location us-central1 \
  --schedule "30 8 * * *" --time-zone "America/Chicago" \
  --uri "https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/us-central1/jobs/toast-sync:run" \
  --http-method POST \
  --oauth-service-account-email "sync-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"
```

- [ ] **Step 3: Force-run the schedule now to prove the trigger path:** `gcloud scheduler jobs run daily-toast-sync --location us-central1`, then confirm a new successful execution: `gcloud run jobs executions list --job toast-sync --region us-central1 --limit 2`.
- [ ] **Step 4: Failure alert (user-guided console, ~3 min):** console.cloud.google.com → Monitoring → Alerting → Create policy → metric `Cloud Run Job` → `Completed execution count` filtered `result=failed`, threshold > 0, notification channel = user's email (verify the channel when prompted). Agent verifies afterward: `gcloud alpha monitoring policies list --format "value(displayName)"` includes the new policy. (If the alpha command is unavailable, the user confirms the policy exists in the console — acceptable.)
- [ ] **Step 5:** No repo changes.

## Task 7: Parallel-run parity, PC retirement, docs

- [ ] **Step 1 (checkpoint, spans 2-3 mornings):** Both the PC Task Scheduler job (08:45) and Cloud Run (08:30) are live and idempotently overwrite the same partitions. Each morning, verify via `fs ls -l` that trailing partitions carry that morning's timestamps and the 09:15 Databricks run stays green. Confirm at least 2 consecutive clean mornings.
- [ ] **Step 2 (user):** Disable (don't delete) the Windows Task Scheduler entry `restaurant-forecast-morning-extract`. Keep `scripts/morning_extract.ps1` in the repo as the documented manual fallback/backfill tool.
- [ ] **Step 3:** Docs — README cloud section: extraction now runs on Cloud Run (project name, schedule, secret store; no IDs/hostnames per public-repo posture); note the PC path is retired and the ps1 is fallback-only. Update `scripts/morning_extract.ps1` header comment to say it is the manual fallback. Commit: `docs: extraction runs on Cloud Run; PC path retired`.
- [ ] **Step 4:** Memory update (project memory: Phase 1 complete, GCP project facts, new PAT location) — controller does this, not a subagent.
- [ ] **Step 5:** Merge gate: full suite + hooks green → final review of the branch diff → merge `cloud-extraction` → `main` + push (finishing-a-development-branch).

---

## Self-review (spec coverage)

- Spec §rfa-sync modes → Tasks 2 (both modes implemented; `--today` used by Phase 2). ✅
- Spec §container/no-secrets → Task 4 (generic Dockerfile, `.gcloudignore` excludes `.env`). ✅
- Spec §Secret Manager + dedicated PAT → Task 0 Steps 3/6/7. ✅
- Spec §daily schedule 08:30 CT → Task 6. ✅ (Intraday schedule = Phase 2 plan, per spec phasing.)
- Spec §alerting → Task 6 Step 4. ✅
- Spec §parity + retirement + fallback script retained → Task 7. ✅
- Spec success criteria 1-2, 4-5 covered here; criterion 3 (intraday ≤15 min) is Phase 2 by design. ✅
- Types: `sync_window(refresh_days:int, today_mode:bool, today:date)` and `upload_partitions(root, days, volume_root, *, client, log)` consistent across Tasks 1-2 tests and implementations. ✅
