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
