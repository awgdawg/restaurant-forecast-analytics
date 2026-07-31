"""sync: window computation + orchestration wiring (fakes, no network)."""

from datetime import date

import pytest

from ingest.sync import business_today, sync_window


def _stub_pipeline(monkeypatch, *, rows: int, uploaded: int):
    """Patch every outbound dependency of main() so only the guard is exercised."""
    monkeypatch.setattr("ingest.sync.load_toast_config", lambda: {"fake": "cfg"})
    monkeypatch.setattr("ingest.sync.ToastClient", lambda cfg: "fake-client")
    monkeypatch.setattr(
        "ingest.sync.business_today", lambda now_fn=None: date(2026, 7, 30)
    )
    monkeypatch.setattr(
        "ingest.sync.extract_range",
        lambda client, start, end, out_dir, *, overwrite, log: rows,
    )
    monkeypatch.setattr(
        "ingest.sync.upload_partitions",
        lambda root, days, volume_root, *, log: uploaded,
    )


def test_sync_window_refresh_days_ends_yesterday():
    start, end = sync_window(3, False, date(2026, 7, 30))
    assert (start, end) == (date(2026, 7, 27), date(2026, 7, 29))


def test_sync_window_today_mode_is_single_day():
    start, end = sync_window(0, True, date(2026, 7, 30))
    assert (start, end) == (date(2026, 7, 30), date(2026, 7, 30))


def test_business_today_uses_chicago_clock():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    fixed = datetime(2026, 7, 30, 22, 30, tzinfo=ZoneInfo("America/Chicago"))
    assert business_today(lambda: fixed) == date(2026, 7, 30)


def test_main_wires_extract_then_upload(monkeypatch, tmp_path, capsys):
    calls = {}

    monkeypatch.setattr("ingest.sync.load_toast_config", lambda: {"fake": "cfg"})
    monkeypatch.setattr("ingest.sync.ToastClient", lambda cfg: "fake-client")
    monkeypatch.setattr(
        "ingest.sync.business_today", lambda now_fn=None: date(2026, 7, 30)
    )

    def fake_extract(client, start, end, out_dir, *, overwrite, log):
        calls["extract"] = (client, start, end, out_dir, overwrite)
        return 42

    def fake_upload(root, days, volume_root, *, log):
        calls["upload"] = (root, days, volume_root)
        return 2

    monkeypatch.setattr("ingest.sync.extract_range", fake_extract)
    monkeypatch.setattr("ingest.sync.upload_partitions", fake_upload)

    from ingest.sync import main

    main(
        [
            "--refresh-days",
            "2",
            "--out-dir",
            str(tmp_path),
            "--volume-root",
            "/Volumes/x",
        ]
    )

    client, start, end, out_dir, overwrite = calls["extract"]
    assert (start, end) == (date(2026, 7, 28), date(2026, 7, 29))
    assert overwrite is True
    root, days, volume_root = calls["upload"]
    assert days == [date(2026, 7, 28), date(2026, 7, 29)]
    assert volume_root == "/Volumes/x"
    # Pairs with the guard tests below to pin ordering: the success summary is
    # printed only on the path where the guard did not trip.
    assert "Synced" in capsys.readouterr().out


def test_main_rejects_zero_refresh_days(capsys):
    from ingest.sync import main

    with pytest.raises(SystemExit):
        main(["--refresh-days", "0"])
    # Asserts the MESSAGE, not just the exit: with default=0 argparse would
    # instead emit the generic "one of the arguments ... is required".
    assert "--refresh-days must be >= 1" in capsys.readouterr().err


def test_refresh_mode_zero_partitions_is_a_failure(monkeypatch, tmp_path, capsys):
    # A >=2-day window can never be legitimately empty (Mondays are the only
    # closure), so zero uploads means extraction broke -- fail loudly.
    _stub_pipeline(monkeypatch, rows=1234, uploaded=0)

    from ingest.sync import main

    with pytest.raises(SystemExit) as excinfo:
        main(["--refresh-days", "3", "--out-dir", str(tmp_path)])
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "ERROR" in captured.err
    assert "Synced" not in captured.out


def test_refresh_mode_zero_rows_is_a_failure(monkeypatch, tmp_path, capsys):
    # Warm working dir: total extraction failure still re-uploads stale
    # partitions from earlier runs, so a nonzero upload count is NOT proof of
    # freshness -- rows == 0 must fail the run too.
    _stub_pipeline(monkeypatch, rows=0, uploaded=3)

    from ingest.sync import main

    with pytest.raises(SystemExit) as excinfo:
        main(["--refresh-days", "3", "--out-dir", str(tmp_path)])
    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "rows=0" in captured.err
    assert "Synced" not in captured.out


def test_today_mode_zero_partitions_exits_normally(monkeypatch, tmp_path):
    # Intraday before open (or on a Monday) legitimately has nothing to upload.
    _stub_pipeline(monkeypatch, rows=0, uploaded=0)

    from ingest.sync import main

    assert main(["--today", "--out-dir", str(tmp_path)]) is None
