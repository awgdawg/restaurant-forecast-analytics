import os
from dataclasses import dataclass

DEFAULT_BASE_URL = "https://ws-api.toasttab.com"

SECRET_SCOPE = "restaurant-forecast"
SECRET_KEYS = {
    "client_id": "toast-client-id",
    "client_secret": "toast-client-secret",
    "restaurant_guid": "toast-restaurant-guid",
}


@dataclass(frozen=True)
class ToastConfig:
    base_url: str
    client_id: str
    client_secret: str
    restaurant_guid: str


def _workspace_client():
    """Return a databricks.sdk WorkspaceClient.

    Imported lazily so local/CI use never requires databricks-sdk; on serverless
    job compute the SDK is preinstalled and picks up ambient job auth.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _load_from_secrets(base_url: str) -> ToastConfig:
    """Read Toast credentials from the Databricks secret scope.

    Used only when the env vars are absent (i.e. in-cloud, where the bundle
    cannot inject env vars into a serverless wheel task -- the environment/task
    schema has no env-var field, so credentials come from the secret scope
    instead). Any failure to reach the scope surfaces as RuntimeError.
    """
    try:
        secrets = _workspace_client().dbutils.secrets
        return ToastConfig(
            base_url=base_url,
            client_id=secrets.get(SECRET_SCOPE, SECRET_KEYS["client_id"]),
            client_secret=secrets.get(SECRET_SCOPE, SECRET_KEYS["client_secret"]),
            restaurant_guid=secrets.get(SECRET_SCOPE, SECRET_KEYS["restaurant_guid"]),
        )
    except Exception as exc:
        raise RuntimeError(
            "Missing required Toast credentials: TOAST_CLIENT_ID/"
            "TOAST_CLIENT_SECRET/TOAST_RESTAURANT_GUID not in env and the "
            f"'{SECRET_SCOPE}' secret scope was unreachable ({exc})"
        ) from exc


def load_toast_config() -> ToastConfig:
    """Build a ToastConfig from environment variables, falling back to secrets.

    Env vars win when present (local runs load them via load_dotenv() in the CLI
    entrypoint). When TOAST_CLIENT_ID is absent -- the in-cloud case, where a
    serverless wheel task has no way to receive env vars -- the three values are
    read from the '{SECRET_SCOPE}' Databricks secret scope instead.
    """
    base_url = os.environ.get("TOAST_BASE_URL", DEFAULT_BASE_URL)
    if "TOAST_CLIENT_ID" not in os.environ:
        return _load_from_secrets(base_url)
    try:
        return ToastConfig(
            base_url=base_url,
            client_id=os.environ["TOAST_CLIENT_ID"],
            client_secret=os.environ["TOAST_CLIENT_SECRET"],
            restaurant_guid=os.environ["TOAST_RESTAURANT_GUID"],
        )
    except KeyError as exc:
        raise RuntimeError(f"Missing required Toast env var: {exc.args[0]}") from exc
