"""to_volume: local partition discovery + Files-API upload (fake client)."""

from datetime import date
from pathlib import Path

from ingest.to_volume import local_partitions, upload_partitions


def _make_partition(root: Path, bd: str) -> Path:
    part = root / f"business_date={bd}"
    part.mkdir(parents=True)
    target = part / "orders.parquet"
    target.write_bytes(b"parquet-bytes-" + bd.encode())
    return target


class FakeFiles:
    def __init__(self):
        self.dirs: list[str] = []
        self.uploads: list[tuple[str, bytes, bool]] = []

    def create_directory(self, path):
        self.dirs.append(path)

    def upload(self, path, contents, *, overwrite):
        self.uploads.append((path, contents.read(), overwrite))


class FakeClient:
    def __init__(self):
        self.files = FakeFiles()


def test_local_partitions_returns_only_existing_days(tmp_path):
    _make_partition(tmp_path, "20260728")
    days = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)]
    assert local_partitions(tmp_path, days) == [
        ("20260728", tmp_path / "business_date=20260728" / "orders.parquet")
    ]


def test_upload_partitions_uploads_each_existing_day(tmp_path):
    _make_partition(tmp_path, "20260728")
    _make_partition(tmp_path, "20260729")
    client = FakeClient()
    n = upload_partitions(
        tmp_path,
        [date(2026, 7, 28), date(2026, 7, 29)],
        "/Volumes/ws/default/raw",
        client=client,
    )
    assert n == 2
    assert client.files.dirs == [
        "/Volumes/ws/default/raw/business_date=20260728",
        "/Volumes/ws/default/raw/business_date=20260729",
    ]
    assert client.files.uploads == [
        (
            "/Volumes/ws/default/raw/business_date=20260728/orders.parquet",
            b"parquet-bytes-20260728",
            True,
        ),
        (
            "/Volumes/ws/default/raw/business_date=20260729/orders.parquet",
            b"parquet-bytes-20260729",
            True,
        ),
    ]


def _must_not_build_client():
    raise AssertionError("client must not be built for an empty window")


def test_upload_partitions_empty_window_never_builds_client(tmp_path, monkeypatch):
    # client=None + no partitions must return 0 WITHOUT constructing a real
    # client. Patched explicitly so the test fails structurally if the empty
    # check ever moves below client construction -- unpatched it only proved
    # anything on a machine whose env happened to lack Databricks credentials.
    monkeypatch.setattr("ingest.to_volume._workspace_client", _must_not_build_client)
    assert upload_partitions(tmp_path, [date(2026, 7, 28)], "/Volumes/x") == 0
