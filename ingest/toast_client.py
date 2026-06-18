from __future__ import annotations

import requests

from ingest.config import ToastConfig


class ToastAuthError(RuntimeError):
    pass


class ToastClient:
    """Minimal Toast API client: authenticate once, then issue authenticated GETs."""

    def __init__(
        self, config: ToastConfig, session: requests.Session | None = None
    ) -> None:
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
