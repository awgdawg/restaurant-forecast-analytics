"""Open a Databricks SQL connection, from env vars or the secret scope.

Env vars win when present (local runs load them via load_dotenv() in the
entrypoint, reusing the DBX_* values proven by `dbt debug`). When DBX_HOST is
absent -- the in-cloud case, where a serverless wheel task has no way to
receive env vars -- the connection values are read from the
'restaurant-forecast' Databricks secret scope instead (a minimal SQL-scoped
job token, not the admin PAT). catalog/schema are not secrets and always come
from env-with-defaults.
"""

from __future__ import annotations

import os

from databricks import sql

SECRET_SCOPE = "restaurant-forecast"
SECRET_KEYS = {
    "host": "dbx-host",
    "http_path": "dbx-http-path",
    "token": "dbx-token",
}


def _workspace_client():
    """Return a databricks.sdk WorkspaceClient.

    Imported lazily so local/CI use never requires databricks-sdk; on serverless
    job compute the SDK is preinstalled and picks up ambient job auth.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _connect_kwargs_from_secrets() -> dict:
    """Read the DBX connection values from the Databricks secret scope.

    Used only when the env vars are absent (i.e. in-cloud, where the bundle
    cannot inject env vars into a serverless wheel task -- the environment/task
    schema has no env-var field, so credentials come from the secret scope
    instead). Any failure to reach the scope surfaces as RuntimeError.
    """
    try:
        secrets = _workspace_client().dbutils.secrets
        return {
            "server_hostname": secrets.get(SECRET_SCOPE, SECRET_KEYS["host"]),
            "http_path": secrets.get(SECRET_SCOPE, SECRET_KEYS["http_path"]),
            "access_token": secrets.get(SECRET_SCOPE, SECRET_KEYS["token"]),
        }
    except Exception as exc:
        raise RuntimeError(
            "Missing required Databricks connection settings: DBX_HOST/"
            "DBX_HTTP_PATH/DBX_TOKEN not in env and the "
            f"'{SECRET_SCOPE}' secret scope was unreachable ({exc})"
        ) from exc


def connect():
    """Open a Databricks SQL connection.

    Discriminator: DBX_HOST in env -> read all three connection values from env
    (local); otherwise read them from the 'restaurant-forecast' secret scope
    (in-cloud). catalog/schema are not secrets and always come from env, with
    'workspace'/'default' defaults.
    """
    if "DBX_HOST" in os.environ:
        try:
            conn_kwargs = {
                "server_hostname": os.environ["DBX_HOST"],
                "http_path": os.environ["DBX_HTTP_PATH"],
                "access_token": os.environ["DBX_TOKEN"],
            }
        except KeyError as exc:
            raise RuntimeError(
                f"Missing required Databricks env var: {exc.args[0]}"
            ) from exc
    else:
        conn_kwargs = _connect_kwargs_from_secrets()

    return sql.connect(
        catalog=os.environ.get("DBX_CATALOG", "workspace"),
        schema=os.environ.get("DBX_SCHEMA", "default"),
        **conn_kwargs,
    )
