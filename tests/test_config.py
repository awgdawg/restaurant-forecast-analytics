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
