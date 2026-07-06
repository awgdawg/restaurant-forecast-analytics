import pytest

import load.databricks as databricks
from load.databricks import connect


def test_connect_reads_env(monkeypatch):
    """With DBX_HOST present, sql.connect gets the three env values."""
    monkeypatch.setenv("DBX_HOST", "env-host")
    monkeypatch.setenv("DBX_HTTP_PATH", "env-path")
    monkeypatch.setenv("DBX_TOKEN", "env-token")
    monkeypatch.delenv("DBX_CATALOG", raising=False)
    monkeypatch.delenv("DBX_SCHEMA", raising=False)

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "conn"

    monkeypatch.setattr(databricks.sql, "connect", fake_connect)

    assert connect() == "conn"
    assert captured["server_hostname"] == "env-host"
    assert captured["http_path"] == "env-path"
    assert captured["access_token"] == "env-token"
    assert captured["catalog"] == "workspace"  # default
    assert captured["schema"] == "default"  # default


def test_env_wins_secrets_not_consulted(monkeypatch):
    """When DBX_HOST is present, the secrets helper is never consulted."""
    monkeypatch.setenv("DBX_HOST", "env-host")
    monkeypatch.setenv("DBX_HTTP_PATH", "env-path")
    monkeypatch.setenv("DBX_TOKEN", "env-token")

    def fake_connect(**kwargs):
        return "conn"

    monkeypatch.setattr(databricks.sql, "connect", fake_connect)

    def _boom():
        raise AssertionError("secrets backend must not be consulted when env is set")

    monkeypatch.setattr(databricks, "_workspace_client", _boom)

    assert connect() == "conn"


def test_secrets_consulted_only_when_env_missing(monkeypatch):
    """With DBX_HOST absent, the three values come from the secret scope."""
    for var in ("DBX_HOST", "DBX_HTTP_PATH", "DBX_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DBX_CATALOG", "env-catalog")
    monkeypatch.setenv("DBX_SCHEMA", "env-schema")

    fake_secrets = {
        "dbx-host": "cloud-host",
        "dbx-http-path": "cloud-path",
        "dbx-token": "cloud-token",
    }

    class FakeSecrets:
        def get(self, scope, key):
            assert scope == "restaurant-forecast"
            return fake_secrets[key]

    class FakeDbutils:
        secrets = FakeSecrets()

    class FakeWorkspaceClient:
        dbutils = FakeDbutils()

    monkeypatch.setattr(databricks, "_workspace_client", lambda: FakeWorkspaceClient())

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "conn"

    monkeypatch.setattr(databricks.sql, "connect", fake_connect)

    assert connect() == "conn"
    assert captured["server_hostname"] == "cloud-host"
    assert captured["http_path"] == "cloud-path"
    assert captured["access_token"] == "cloud-token"
    # catalog/schema are not secrets -- they still come from env.
    assert captured["catalog"] == "env-catalog"
    assert captured["schema"] == "env-schema"


def test_secrets_catalog_schema_defaults(monkeypatch):
    """With DBX_HOST and DBX_CATALOG/DBX_SCHEMA absent, defaults apply."""
    for var in ("DBX_HOST", "DBX_HTTP_PATH", "DBX_TOKEN", "DBX_CATALOG", "DBX_SCHEMA"):
        monkeypatch.delenv(var, raising=False)

    fake_secrets = {
        "dbx-host": "cloud-host",
        "dbx-http-path": "cloud-path",
        "dbx-token": "cloud-token",
    }

    class FakeSecrets:
        def get(self, scope, key):
            return fake_secrets[key]

    class FakeDbutils:
        secrets = FakeSecrets()

    class FakeWorkspaceClient:
        dbutils = FakeDbutils()

    monkeypatch.setattr(databricks, "_workspace_client", lambda: FakeWorkspaceClient())

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "conn"

    monkeypatch.setattr(databricks.sql, "connect", fake_connect)

    connect()
    assert captured["catalog"] == "workspace"  # default
    assert captured["schema"] == "default"  # default


def test_connect_missing_env_and_secrets_raises(monkeypatch):
    """Neither env nor a reachable secret backend -> actionable RuntimeError."""
    for var in ("DBX_HOST", "DBX_HTTP_PATH", "DBX_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    def _unavailable():
        raise RuntimeError("no secret backend")

    monkeypatch.setattr(databricks, "_workspace_client", _unavailable)

    with pytest.raises(RuntimeError) as excinfo:
        connect()

    msg = str(excinfo.value)
    assert "DBX_HOST" in msg
    assert "restaurant-forecast" in msg
