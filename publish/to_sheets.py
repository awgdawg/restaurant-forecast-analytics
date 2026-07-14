"""Mirror the two serving tables into a Google Sheet, for Tableau Public to sync.

The nightly job's forecast task writes model_metrics and dbt builds the
forecast_vs_actuals view; this publish task copies both into named tabs of a
Google Sheet, which Tableau Public auto-refreshes from (Tableau cannot reach a
Databricks warehouse on the free tier, but it can sync a public Sheet).

Two seams, both mirroring the rest of the repo:
- Reads branch on active_spark() exactly like forecast/run_forecast.py: in-cloud
  the serverless egress allowlist blocks the SQL connector, so table reads go
  through the job's own Spark session and connect() is never called; locally
  there is no pyspark and reads use the databricks-sql connector.
- The Google service-account JSON is resolved via the secrets shim (see
  load/databricks.py / ingest/config.py): env GCP_SA_JSON wins locally,
  otherwise it comes from the 'restaurant-forecast' secret scope in-cloud.
  gspread/googleapis IS reachable from serverless -- only the SQL connector is not.
"""

from __future__ import annotations

import argparse
import json
import numbers
import os
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv

from forecast.data import mart_table, metrics_table
from load.databricks import connect
from load.spark_io import active_spark

SECRET_SCOPE = "restaurant-forecast"
SECRET_KEY = "gcp-sa-json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

FORECAST_TAB = "forecast_vs_actuals"
METRICS_TAB = "model_metrics"


def _cell(value):
    """Coerce one value to a Sheets-safe scalar (str/int/float only).

    None / NaN / NaT -> "" (empty cell); date/datetime/Timestamp -> str();
    numpy/pandas numeric scalars -> native python int/float; bool and everything
    else -> str(). gspread only accepts plain scalars, so numpy types must be
    unwrapped and bool (a subclass of int) is stringified rather than left as-is.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    try:
        if pd.isna(value):  # NaN / NaT
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return str(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        return float(value)
    return str(value)


def frame_to_rows(df: pd.DataFrame) -> list[list]:
    """DataFrame -> list-of-lists for gspread: header row + Sheets-safe cells.

    Row 0 is the column names in order; each subsequent row is one record with
    every cell passed through _cell (str/int/float only, NaN/None -> "").
    """
    rows: list[list] = [[str(c) for c in df.columns]]
    for record in df.itertuples(index=False, name=None):
        rows.append([_cell(v) for v in record])
    return rows


def publish_frames(
    client, sheet_id: str, frames: dict[str, pd.DataFrame]
) -> dict[str, int]:
    """Overwrite each named tab of the sheet with its frame's rows.

    client: an authorized gspread client (open_by_key(sheet_id) -> spreadsheet
    whose worksheet(tab) -> a worksheet exposing clear()/update(values)). Per
    tab: clear() then update() with frame_to_rows output. Returns the data-row
    count written per tab (header excluded).
    """
    spreadsheet = client.open_by_key(sheet_id)
    counts: dict[str, int] = {}
    for tab, frame in frames.items():
        rows = frame_to_rows(frame)
        worksheet = spreadsheet.worksheet(tab)
        worksheet.clear()
        worksheet.update(rows)  # values-first form (gspread >= 6)
        counts[tab] = len(rows) - 1
    return counts


def _workspace_client():
    """Return a databricks.sdk WorkspaceClient (lazy import; ambient job auth).

    Imported lazily so local/CI use never requires databricks-sdk; on serverless
    job compute the SDK is preinstalled and picks up ambient job auth.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _resolve_sa_json() -> str:
    """Resolve the Google service-account JSON, env-wins then secret scope.

    Mirrors the other shims: GCP_SA_JSON in env wins (local runs load it via
    load_dotenv()); otherwise the value is read from the 'restaurant-forecast'
    secret scope (in-cloud, where a serverless wheel task receives no env vars).
    Neither available -> RuntimeError. The returned value is either inline JSON
    or a path to a key file -- the caller discriminates.
    """
    if "GCP_SA_JSON" in os.environ:
        return os.environ["GCP_SA_JSON"]
    try:
        return _workspace_client().dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    except Exception as exc:
        raise RuntimeError(
            "Missing required Google service-account JSON: GCP_SA_JSON not in "
            f"env and the '{SECRET_SCOPE}' secret scope was unreachable ({exc})"
        ) from exc


def _gspread_client():
    """Authorize a gspread client from the service-account JSON.

    The resolved value is inline JSON (starts with '{') or a path to a key file;
    inline -> from_service_account_info, path -> from_service_account_file. Scoped
    to Sheets only.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    raw = _resolve_sa_json()
    if raw.lstrip().startswith("{"):
        creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(raw, scopes=SCOPES)
    return gspread.authorize(creds)


def _read_sql(conn, query: str) -> pd.DataFrame:
    """Run a query through the SQL connector into a DataFrame (local path).

    Column names come from cursor.description because these are SELECT * reads
    whose columns are not known ahead of time.
    """
    cur = conn.cursor()
    try:
        cur.execute(query)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        cur.close()
    return pd.DataFrame(rows, columns=columns)


def main() -> None:
    # load_dotenv() runs BEFORE building the parser so the --sheet-id default can
    # read GSHEET_ID from .env. In-cloud there is no .env (harmless no-op) and the
    # value arrives via the --sheet-id CLI arg the bundle passes instead.
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Publish serving tables to a Google Sheet for Tableau Public."
    )
    parser.add_argument(
        "--sheet-id",
        default=os.environ.get("GSHEET_ID"),
        help="Google Sheet ID (default: env GSHEET_ID)",
    )
    args = parser.parse_args()

    sheet_id = args.sheet_id
    if not sheet_id:
        parser.error("--sheet-id is required (pass --sheet-id or set GSHEET_ID)")

    # The dbt marts view sits beside fct_daily_sales in <schema>_marts; model_metrics
    # is a base-schema source table (metrics_table()).
    forecast_view = mart_table().replace("fct_daily_sales", FORECAST_TAB)
    q_forecast = f"SELECT * FROM {forecast_view} ORDER BY business_date"
    q_metrics = f"SELECT * FROM {metrics_table()} ORDER BY run_ts"

    # Detect the cloud seam once (same as forecast/run_forecast.py). Spark present
    # -> read through it, connect() never called; else -> SQL connector.
    spark = active_spark()
    if spark is not None:
        fva = spark.sql(q_forecast).toPandas()
        metrics = spark.sql(q_metrics).toPandas()
    else:
        conn = connect()
        try:
            fva = _read_sql(conn, q_forecast)
            metrics = _read_sql(conn, q_metrics)
        finally:
            conn.close()

    client = _gspread_client()
    counts = publish_frames(client, sheet_id, {FORECAST_TAB: fva, METRICS_TAB: metrics})
    print(
        f"Published {counts[FORECAST_TAB]} rows to {FORECAST_TAB}, "
        f"{counts[METRICS_TAB]} rows to {METRICS_TAB}"
    )


if __name__ == "__main__":
    main()
