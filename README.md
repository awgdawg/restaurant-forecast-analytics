# restaurant-forecast-analytics

Daily restaurant sales forecasting from live **Toast** POS data → **Databricks** (dbt) → **Tableau Public**.

Targets the forecasting and Tableau skill gaps. See the design spec in
[`docs/superpowers/specs/`](docs/superpowers/specs/) and the implementation plans in
[`docs/superpowers/plans/`](docs/superpowers/plans/).

## Setup

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # then fill in Toast + Databricks credentials
```

## Layout

- `ingest/` — local Python: Toast API auth + extraction
- `models/` — dbt (staging → marts) on Databricks
- `tests/` — pytest
- `docs/` — spec, plans, captured API shapes
