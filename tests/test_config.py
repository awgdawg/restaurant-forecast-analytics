import pytest

import ingest.config as config
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


def test_env_wins_secrets_not_consulted(monkeypatch):
    """When TOAST_CLIENT_ID is present, secrets are never read (no cloud call)."""
    monkeypatch.setenv("TOAST_CLIENT_ID", "cid")
    monkeypatch.setenv("TOAST_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TOAST_RESTAURANT_GUID", "guid-123")

    def _boom():
        raise AssertionError("secrets backend must not be consulted when env is set")

    monkeypatch.setattr(config, "_load_from_secrets", _boom)

    cfg = load_toast_config()

    assert cfg.client_id == "cid"
    assert cfg.client_secret == "sec"
    assert cfg.restaurant_guid == "guid-123"


def test_secrets_consulted_only_when_env_missing(monkeypatch):
    """With TOAST_CLIENT_ID absent, the three values come from the secret scope."""
    for var in ("TOAST_CLIENT_ID", "TOAST_CLIENT_SECRET", "TOAST_RESTAURANT_GUID"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("TOAST_BASE_URL", raising=False)

    fake_secrets = {
        "toast-client-id": "cloud-cid",
        "toast-client-secret": "cloud-sec",
        "toast-restaurant-guid": "cloud-guid",
    }

    class FakeSecrets:
        def get(self, scope, key):
            assert scope == "restaurant-forecast"
            return fake_secrets[key]

    class FakeDbutils:
        secrets = FakeSecrets()

    class FakeWorkspaceClient:
        dbutils = FakeDbutils()

    monkeypatch.setattr(config, "_workspace_client", lambda: FakeWorkspaceClient())

    cfg = load_toast_config()

    assert cfg.client_id == "cloud-cid"
    assert cfg.client_secret == "cloud-sec"
    assert cfg.restaurant_guid == "cloud-guid"
    assert cfg.base_url == "https://ws-api.toasttab.com"  # default


def test_load_missing_env_and_secrets_raises(monkeypatch):
    """Neither env nor a reachable secret backend -> RuntimeError is preserved."""
    for var in ("TOAST_CLIENT_ID", "TOAST_CLIENT_SECRET", "TOAST_RESTAURANT_GUID"):
        monkeypatch.delenv(var, raising=False)

    def _unavailable():
        raise RuntimeError("no secret backend")

    monkeypatch.setattr(config, "_workspace_client", _unavailable)

    with pytest.raises(RuntimeError, match="Missing required Toast credentials"):
        load_toast_config()
