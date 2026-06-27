from __future__ import annotations

import time

import requests

from ingest.config import ToastConfig


class ToastAuthError(RuntimeError):
    pass


# Transient statuses worth retrying: rate limit + the usual gateway/5xx blips.
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class ToastClient:
    """Minimal Toast API client: authenticate once, then issue authenticated GETs.

    Built to survive long backfills: GETs retry transient 429/5xx responses with
    exponential backoff (honoring a Retry-After header), and re-authenticate once
    if the token expires mid-run (401).
    """

    def __init__(
        self,
        config: ToastConfig,
        session: requests.Session | None = None,
        max_retries: int = 5,
        backoff_base: float = 0.5,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._token: str | None = None
        self._max_retries = max_retries
        self._backoff_base = backoff_base

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
        url = f"{self._config.base_url}{path}"
        attempt = 0
        reauthed = False
        while True:
            headers = {
                "Authorization": f"Bearer {self._token}",
                "Toast-Restaurant-External-ID": self._config.restaurant_guid,
            }
            resp = self._session.get(url, headers=headers, params=params, timeout=60)
            if resp.status_code == 401 and not reauthed:
                self.authenticate()  # token likely expired mid-run; refresh once
                reauthed = True
                continue
            if resp.status_code in _RETRY_STATUSES and attempt < self._max_retries:
                time.sleep(self._retry_delay(resp, attempt))
                attempt += 1
                continue
            resp.raise_for_status()
            return resp.json()

    def _retry_delay(self, resp: requests.Response, attempt: int) -> float:
        """Seconds to wait before a retry: server's Retry-After if given, else
        exponential backoff (base, 2x, 4x, ...)."""
        retry_after = resp.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return self._backoff_base * (2**attempt)

    def get_paginated(
        self, path: str, params: dict | None = None, page_size: int = 100
    ) -> list:
        """GET a list endpoint, paging via page/pageSize until a short page."""
        query = dict(params or {})
        query["pageSize"] = page_size
        results: list = []
        page = 1
        while True:
            query["page"] = page
            batch = self.get(path, params=query)
            if not isinstance(batch, list) or not batch:
                break
            results.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return results
