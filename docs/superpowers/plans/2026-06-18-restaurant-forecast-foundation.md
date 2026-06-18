# Restaurant Forecast — Foundation & Toast Ingest Implementation Plan (Plan 1 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `restaurant-forecast-analytics` repo, a tested Toast **auth** client, and a one-shot **schema-discovery** script that captures a real Toast Orders sample — getting us to "we can authenticate, pull raw data, and know the real schema" so Plan 2 can build transforms/forecast/dashboard against reality (no guessed schemas).

**Architecture:** Local Python ingestion (`requests`) + a dbt-databricks project skeleton, mirroring the conventions of the existing `kc-blight-analytics` repo (pinned `requirements.txt`, staging→marts dbt layout, gitleaks pre-commit). Secrets via `.env` / `env_var()` only. Forecasting/transform models are deliberately **out of scope** for this plan (Plan 2).

**Tech Stack:** Python 3.10, `requests`, `python-dotenv`, `pytest` + `responses` (HTTP mocking), `dbt-core` + `dbt-databricks`, pre-commit (gitleaks + ruff), GitHub Actions.

---

## Scope (Plan 1 of 2)

**In scope (fully concrete now):**
- M0 real-world prerequisites (accounts/access) — checklist with verifications
- Repo scaffolding: Python project + venv, requirements, pre-commit, CI
- dbt-databricks project skeleton + connection smoke test
- Toast config loader + auth client (`authenticate`, authenticated `get`) — TDD with mocked HTTP
- Toast schema-discovery spike — capture a real Orders sample to a fixture

**Out of scope (Plan 2, written against the captured sample):** `extract.py` (range pulls → partitioned Parquet), `load_to_delta.py`, dbt staging + `fct_daily_sales` + reconciliation test, baseline + Prophet forecast + backtest, Tableau Public dashboard.

**Repo root for all paths/commands:** `E:\PyProj\restaurant-forecast-analytics` (run commands from here in PowerShell). The repo, `.gitignore`, and `docs/` already exist.

---

## Prerequisites — M0 (real-world; no code)

These gate **Task 6 and Task 7** only. Tasks 1–5 need none of them and can be done immediately.

- [ ] **Toast read-only API access.** In Toast Web, confirm the restaurant is on **RMS Essentials or higher** and that the admin has the **Manage Integrations** permission. Then: Toast Web → **Toast Partner Integrations** → set up **Standard API Access** → generate a **read-only** credential set scoped to the restaurant.
  - **Done when:** you hold a `clientId`, a `clientSecret`, and the **restaurant GUID**.
  - **If blocked** (plan below RMS Essentials): stop and decide on a plan change before Plan 2 — sales data cannot be pulled without this.
- [ ] **Databricks Free Edition workspace.** Sign up at the Databricks Free Edition signup. In the workspace: note the **workspace host URL**; open SQL → the pre-provisioned **Serverless Starter Warehouse** → **Connection details** → copy the **Server hostname** and **HTTP path**; create a **Personal Access Token** (Settings → Developer → Access tokens); create a **catalog** named `restaurant_forecast` (Catalog → Create) and a **schema** `dev`.
  - **Done when:** you have `DBX_HOST`, `DBX_HTTP_PATH`, `DBX_TOKEN`, and the `restaurant_forecast.dev` catalog/schema exists.
- [ ] **Tableau Public account** created (free) at the Tableau Public signup. *(Used in Plan 2; create now so it's ready.)*
  - **Done when:** you can log in to Tableau Public.

---

## Task 1: Python project skeleton + virtual environment

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `ingest/__init__.py` (empty)
- Create: `tests/__init__.py` (empty)
- Create: `README.md`

- [ ] **Step 1: Create the virtual environment (on E:, per project convention)**

Run (PowerShell, from repo root):
```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
```
Expected: prints `Python 3.10.x`, prompt now shows `(.venv)`.

- [ ] **Step 2: Write `requirements.txt`**

```
dbt-core==1.10.4
dbt-databricks~=1.10
requests==2.32.3
python-dotenv==1.0.1
PyYAML==6.0.2
pytest==8.3.3
responses==0.25.3
```

- [ ] **Step 3: Install dependencies**

Run:
```powershell
pip install -r requirements.txt
```
Expected: completes without error; `pip show dbt-databricks` prints a 1.10.x version.

- [ ] **Step 4: Write `.env.example`**

```
# --- Toast Standard API Access (read-only) — from Toast Web > Partner Integrations ---
TOAST_BASE_URL=https://ws-api.toasttab.com
TOAST_CLIENT_ID=
TOAST_CLIENT_SECRET=
TOAST_RESTAURANT_GUID=

# --- Databricks (Free Edition) — workspace host, Serverless Starter Warehouse, PAT ---
DBX_HOST=
DBX_HTTP_PATH=
DBX_TOKEN=
DBX_CATALOG=restaurant_forecast
DBX_SCHEMA=dev
```

- [ ] **Step 5: Create empty package markers and a README stub**

Create `ingest/__init__.py` (empty), `tests/__init__.py` (empty), and `README.md`:
```markdown
# restaurant-forecast-analytics

Daily restaurant sales forecasting from live Toast POS data → Databricks (dbt) → Tableau Public.
See the design spec in `docs/superpowers/specs/` and the plans in `docs/superpowers/plans/`.

## Setup
```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # then fill in credentials
```
```

- [ ] **Step 6: Commit**

```powershell
git add requirements.txt .env.example ingest/__init__.py tests/__init__.py README.md
git commit -m "chore: python project skeleton, requirements, env template"
```

---

## Task 2: Pre-commit (gitleaks + ruff) and CI

**Files:**
- Create: `.pre-commit-config.yaml`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `.pre-commit-config.yaml`** (gitleaks matches the kc-blight convention; ruff adds Python lint/format)

```yaml
repos:
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.18.0
    hooks:
      - id: gitleaks
        name: gitleaks (scan for secrets)
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
      - id: ruff-format
```

- [ ] **Step 2: Install the hooks and run them**

Run:
```powershell
pip install pre-commit
pre-commit run --all-files
```
Expected: gitleaks and ruff hooks install and **pass** (no secrets; no lint errors on the skeleton).

- [ ] **Step 3: Write `.github/workflows/ci.yml`**

```yaml
name: CI
on: [push, pull_request]
jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt pre-commit
      - run: pre-commit run --all-files
      - run: pytest -q
```

- [ ] **Step 4: Commit**

```powershell
git add .pre-commit-config.yaml .github/workflows/ci.yml
git commit -m "ci: pre-commit (gitleaks + ruff) and GitHub Actions lint+test"
```

---

## Task 3: dbt-databricks project skeleton

**Files:**
- Create: `dbt_project.yml`
- Create: `profiles.yml.example`
- Create: `models/staging/.gitkeep`
- Create: `models/marts/.gitkeep`

- [ ] **Step 1: Write `dbt_project.yml`** (mirrors kc-blight's staging→marts layout)

```yaml
name: 'restaurant_forecast'
version: '1.0.0'
config-version: 2

profile: 'restaurant_forecast'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  restaurant_forecast:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

- [ ] **Step 2: Write `profiles.yml.example`** (secrets come from env vars — never hard-coded)

```yaml
restaurant_forecast:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: "{{ env_var('DBX_CATALOG', 'restaurant_forecast') }}"
      schema: "{{ env_var('DBX_SCHEMA', 'dev') }}"
      host: "{{ env_var('DBX_HOST') }}"
      http_path: "{{ env_var('DBX_HTTP_PATH') }}"
      token: "{{ env_var('DBX_TOKEN') }}"
      threads: 4
```

- [ ] **Step 3: Create empty model directories**

Create `models/staging/.gitkeep` (empty) and `models/marts/.gitkeep` (empty).

- [ ] **Step 4: Verify the project parses (offline, no connection needed)**

Run:
```powershell
copy profiles.yml.example profiles.yml
$env:DBT_PROFILES_DIR = (Get-Location).Path
dbt parse
```
Expected: `dbt parse` succeeds (it resolves the project + profile without connecting). `profiles.yml` is gitignored, so it will not be committed.

- [ ] **Step 5: Commit**

```powershell
git add dbt_project.yml profiles.yml.example models/staging/.gitkeep models/marts/.gitkeep
git commit -m "feat: dbt-databricks project skeleton (staging + marts)"
```

---

## Task 4: Toast config loader

**Files:**
- Create: `ingest/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

`tests/test_config.py`:
```python
import pytest
from ingest.config import ToastConfig, load_toast_config


def test_load_reads_env(monkeypatch):
    monkeypatch.setenv("TOAST_CLIENT_ID", "cid")
    monkeypatch.setenv("TOAST_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TOAST_RESTAURANT_GUID", "guid-123")
    monkeypatch.delenv("TOAST_BASE_URL", raising=False)

    cfg = load_toast_config()

    assert isinstance(cfg, ToastConfig)
    assert cfg.client_id == "cid"
    assert cfg.client_secret == "sec"
    assert cfg.restaurant_guid == "guid-123"
    assert cfg.base_url == "https://ws-api.toasttab.com"  # default


def test_load_missing_var_raises(monkeypatch):
    for var in ("TOAST_CLIENT_ID", "TOAST_CLIENT_SECRET", "TOAST_RESTAURANT_GUID"):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(RuntimeError, match="Missing required Toast env var"):
        load_toast_config()
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```powershell
pytest tests/test_config.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'ingest.config'`.

- [ ] **Step 3: Write the minimal implementation**

`ingest/config.py`:
```python
import os
from dataclasses import dataclass

DEFAULT_BASE_URL = "https://ws-api.toasttab.com"


@dataclass(frozen=True)
class ToastConfig:
    base_url: str
    client_id: str
    client_secret: str
    restaurant_guid: str


def load_toast_config() -> ToastConfig:
    """Build a ToastConfig from environment variables.

    Call load_dotenv() in the CLI entrypoint before this if using a .env file.
    """
    try:
        return ToastConfig(
            base_url=os.environ.get("TOAST_BASE_URL", DEFAULT_BASE_URL),
            client_id=os.environ["TOAST_CLIENT_ID"],
            client_secret=os.environ["TOAST_CLIENT_SECRET"],
            restaurant_guid=os.environ["TOAST_RESTAURANT_GUID"],
        )
    except KeyError as exc:
        raise RuntimeError(f"Missing required Toast env var: {exc.args[0]}") from exc
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```powershell
pytest tests/test_config.py -v
```
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```powershell
git add ingest/config.py tests/test_config.py
git commit -m "feat: Toast config loader from environment"
```

---

## Task 5: Toast auth client (`authenticate` + authenticated `get`)

**Files:**
- Create: `ingest/toast_client.py`
- Test: `tests/test_toast_client.py`

> **Schema note:** the auth response path `token.accessToken` and the `Toast-Restaurant-External-ID` header follow Toast's documented contract. They are **validated for real in Task 7**; if the live response differs, adjust the extraction there and update this test.

- [ ] **Step 1: Write the failing test**

`tests/test_toast_client.py`:
```python
import pytest
import responses
from ingest.config import ToastConfig
from ingest.toast_client import ToastClient, ToastAuthError

CONFIG = ToastConfig(
    base_url="https://ws-api.toasttab.com",
    client_id="cid",
    client_secret="sec",
    restaurant_guid="guid-123",
)
LOGIN_URL = "https://ws-api.toasttab.com/authentication/v1/authentication/login"


@responses.activate
def test_authenticate_returns_access_token():
    responses.add(
        responses.POST,
        LOGIN_URL,
        json={"token": {"tokenType": "Bearer", "accessToken": "abc123", "expiresIn": 86400}},
        status=200,
    )
    client = ToastClient(CONFIG)
    assert client.authenticate() == "abc123"


@responses.activate
def test_authenticate_raises_on_http_error():
    responses.add(responses.POST, LOGIN_URL, json={"error": "bad creds"}, status=401)
    client = ToastClient(CONFIG)
    with pytest.raises(ToastAuthError):
        client.authenticate()


@responses.activate
def test_get_sends_bearer_and_restaurant_header():
    responses.add(
        responses.POST,
        LOGIN_URL,
        json={"token": {"accessToken": "abc123"}},
        status=200,
    )
    responses.add(
        responses.GET,
        "https://ws-api.toasttab.com/orders/v2/ordersBulk",
        json=[{"guid": "order-1"}],
        status=200,
    )
    client = ToastClient(CONFIG)
    body = client.get("/orders/v2/ordersBulk", params={"startDate": "x", "endDate": "y"})

    assert body == [{"guid": "order-1"}]
    get_request = responses.calls[1].request
    assert get_request.headers["Authorization"] == "Bearer abc123"
    assert get_request.headers["Toast-Restaurant-External-ID"] == "guid-123"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```powershell
pytest tests/test_toast_client.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'ingest.toast_client'`.

- [ ] **Step 3: Write the minimal implementation**

`ingest/toast_client.py`:
```python
from __future__ import annotations

import requests

from ingest.config import ToastConfig


class ToastAuthError(RuntimeError):
    pass


class ToastClient:
    """Minimal Toast API client: authenticate once, then issue authenticated GETs."""

    def __init__(self, config: ToastConfig, session: requests.Session | None = None) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._token: str | None = None

    def authenticate(self) -> str:
        url = f"{self._config.base_url}/authentication/v1/authentication/login"
        payload = {
            "clientId": self._config.client_id,
            "clientSecret": self._config.client_secret,
            "userAccessType": "TOAST_MACHINE_CLIENT",
        }
        resp = self._session.post(url, json=payload, timeout=30)
        if resp.status_code != 200:
            raise ToastAuthError(f"Auth failed: {resp.status_code} {resp.text}")
        token = resp.json().get("token", {}).get("accessToken")
        if not token:
            raise ToastAuthError("Auth response missing token.accessToken")
        self._token = token
        return token

    def get(self, path: str, params: dict | None = None) -> object:
        if self._token is None:
            self.authenticate()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Toast-Restaurant-External-ID": self._config.restaurant_guid,
        }
        resp = self._session.get(
            f"{self._config.base_url}{path}", headers=headers, params=params, timeout=60
        )
        resp.raise_for_status()
        return resp.json()
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```powershell
pytest tests/test_toast_client.py -v
```
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```powershell
git add ingest/toast_client.py tests/test_toast_client.py
git commit -m "feat: Toast auth client (authenticate + authenticated get)"
```

---

## Task 6: Databricks connection smoke test  *(requires M0: Databricks)*

**Files:** none created — this verifies the dbt connection to the Serverless Starter Warehouse.

- [ ] **Step 1: Fill Databricks values into `.env`**

Copy `.env.example` → `.env` (if not already) and set `DBX_HOST`, `DBX_HTTP_PATH`, `DBX_TOKEN`, `DBX_CATALOG=restaurant_forecast`, `DBX_SCHEMA=dev`. `.env` is gitignored.

- [ ] **Step 2: Load `.env` into the shell and point dbt at the local profile**

Run (PowerShell, from repo root):
```powershell
.\.venv\Scripts\Activate.ps1
Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^\s*#' } | ForEach-Object {
    $k,$v = $_ -split '=',2; Set-Item -Path "Env:$($k.Trim())" -Value $v.Trim()
}
$env:DBT_PROFILES_DIR = (Get-Location).Path
```

- [ ] **Step 3: Run `dbt debug`**

Run:
```powershell
dbt debug
```
Expected: `Connection test: [OK connection ok]` and `All checks passed!`. If it fails on auth, re-check `DBX_HTTP_PATH` (must be the Serverless Starter Warehouse path) and the PAT.

- [ ] **Step 4: Confirm the catalog/schema is reachable**

Run:
```powershell
dbt show --inline "select current_catalog() as c, current_schema() as s"
```
Expected: returns one row; `c` = `restaurant_forecast`. *(No commit — this task changes no tracked files.)*

---

## Task 7: Toast schema-discovery spike  *(requires M0: Toast)*

**Files:**
- Create: `ingest/discover_schema.py`
- Output (gitignored): `data/raw/sample_orders.json`
- Output (committed): `docs/toast-orders-shape.md`

> This is the bridge to Plan 2: it captures the **real** Orders response so Plan 2's parsing/marts are written against fact, not assumption.

- [ ] **Step 1: Write `ingest/discover_schema.py`**

```python
"""One-shot: authenticate to Toast, pull one business date of orders, and
record the real response shape. Requires real credentials in .env.

Usage (PowerShell):
    python -m ingest.discover_schema 2026-06-15
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

from ingest.config import load_toast_config
from ingest.toast_client import ToastClient


def _shape(value: object, prefix: str = "") -> list[str]:
    """Return sorted 'path: type' lines describing the structure of value."""
    lines: list[str] = []
    if isinstance(value, dict):
        for key in sorted(value):
            lines += _shape(value[key], f"{prefix}.{key}" if prefix else key)
    elif isinstance(value, list):
        lines.append(f"{prefix}: list[{len(value)}]")
        if value:
            lines += _shape(value[0], f"{prefix}[]")
    else:
        lines.append(f"{prefix}: {type(value).__name__}")
    return lines


def main(business_date: str) -> None:
    load_dotenv()
    cfg = load_toast_config()
    client = ToastClient(cfg)

    params = {
        "startDate": f"{business_date}T00:00:00.000-0000",
        "endDate": f"{business_date}T23:59:59.999-0000",
    }
    orders = client.get("/orders/v2/ordersBulk", params=params)

    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "sample_orders.json").write_text(json.dumps(orders, indent=2))

    count = len(orders) if isinstance(orders, list) else "n/a (not a list)"
    shape_lines = _shape(orders[0]) if isinstance(orders, list) and orders else ["(no orders returned)"]
    doc = (
        f"# Toast Orders response shape\n\n"
        f"- Source: `GET /orders/v2/ordersBulk` for business date {business_date}\n"
        f"- Orders returned: {count}\n\n"
        f"## Field paths (first order)\n\n```\n" + "\n".join(shape_lines) + "\n```\n"
    )
    Path("docs/toast-orders-shape.md").write_text(doc)
    print(f"Orders returned: {count}. Wrote data/raw/sample_orders.json and docs/toast-orders-shape.md")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m ingest.discover_schema YYYY-MM-DD")
    main(sys.argv[1])
```

- [ ] **Step 2: Run it against a recent OPEN business day**

Run (pick a date the restaurant was open):
```powershell
.\.venv\Scripts\Activate.ps1
python -m ingest.discover_schema 2026-06-15
```
Expected: prints `Orders returned: N` (N > 0), and creates `data/raw/sample_orders.json` + `docs/toast-orders-shape.md`.
- If `401`: re-check credentials / that the credential set is active.
- If `0 orders`: try a different open date.
- If the field paths differ from the auth assumption in Task 5, note it — Plan 2 builds on this real shape.

- [ ] **Step 3: Verify the auth assumption held (or record the correction)**

Open `docs/toast-orders-shape.md`. Confirm it lists real order fields (e.g., a `guid`, opened/closed timestamps, `checks[]`, amounts). This confirms `authenticate()` + `get()` work end-to-end against the live API.

- [ ] **Step 4: Commit the shape doc only** (raw sample stays gitignored)

```powershell
git add ingest/discover_schema.py docs/toast-orders-shape.md
git commit -m "feat: Toast schema-discovery spike + captured Orders shape"
```

---

## Self-Review (completed during authoring)

- **Spec coverage (this plan = M0 + M1-foundation only):** §5 Toast access ✓ (M0 + auth client), §7 tech stack ✓, §8 repo skeleton ✓ (`ingest/`, dbt dirs, CI, pre-commit), §14 secrets (env vars, gitleaks, `.env` gitignored) ✓, §16 M0 ✓ & M1-start ✓ (auth + sample pull). **Deferred to Plan 2 (noted in Scope):** §10 marts, §11 forecast, §12 full extract/idempotency, §13 dbt tests + reconciliation, §16 M2–M4, §18 success criteria 2–4. `extract.py`/`load_to_delta.py` are intentionally not here.
- **Placeholder scan:** none — every code/command step is complete and runnable.
- **Type consistency:** `ToastConfig(base_url, client_id, client_secret, restaurant_guid)` and `ToastClient(config, session=None).authenticate()->str` / `.get(path, params=None)` are used identically in Tasks 4, 5, and 7.
