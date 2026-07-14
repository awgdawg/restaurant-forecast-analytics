"""Tests for the Google Sheets publish bridge (publish/to_sheets.py).

Pure builder tests for frame_to_rows plus a fake-worksheet drive of the publish
routine. No network: the fakes record only the calls the code makes.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from publish.to_sheets import frame_to_rows, publish_frames

# --- frame_to_rows (pure builder) --------------------------------------------


def test_frame_to_rows_header_is_column_names_in_order():
    df = pd.DataFrame({"b": [1], "a": [2], "c": [3]})

    rows = frame_to_rows(df)

    assert rows[0] == ["b", "a", "c"]


def test_frame_to_rows_nan_and_none_become_empty_string():
    df = pd.DataFrame({"x": [1.0, None], "y": ["ok", float("nan")]})

    rows = frame_to_rows(df)

    assert rows[0] == ["x", "y"]
    assert rows[1] == [1.0, "ok"]
    assert rows[2] == ["", ""]  # None (in float col -> NaN) and NaN both empty


def test_frame_to_rows_python_date_is_stringified():
    df = pd.DataFrame({"d": [date(2026, 6, 16)]})

    rows = frame_to_rows(df)

    assert rows[1][0] == str(date(2026, 6, 16))  # "2026-06-16"


def test_frame_to_rows_timestamp_is_stringified():
    df = pd.DataFrame({"ts": pd.to_datetime(["2026-06-16"])})

    rows = frame_to_rows(df)

    assert rows[1][0] == str(pd.Timestamp("2026-06-16"))


def test_frame_to_rows_cells_are_sheets_safe_types_only():
    df = pd.DataFrame(
        {
            "i": pd.Series([1, 2], dtype="int64"),
            "f": [1.5, 2.5],
            "s": ["a", "b"],
            "ts": pd.to_datetime(["2026-06-16", "2026-06-17"]),
            "n": [None, float("nan")],
            "flag": [True, False],
        }
    )

    rows = frame_to_rows(df)

    for row in rows:
        for cell in row:
            # str/int/float only -- and NOT bool (bool is a subclass of int, but
            # gspread's cell values must be plain scalars).
            assert isinstance(cell, (str, int, float))
            assert not isinstance(cell, bool)
    # numpy scalars are coerced to native python int/float
    assert type(rows[1][0]) is int  # int64 -> int
    assert type(rows[1][1]) is float


# --- publish_frames (fake-worksheet drive) -----------------------------------


class FakeWorksheet:
    """Records clear()/update(values) so the test can assert order + payload."""

    def __init__(self):
        self.calls = []
        self.updated = None

    def clear(self):
        self.calls.append("clear")

    def update(self, values):
        self.calls.append("update")
        self.updated = values


class FakeSpreadsheet:
    def __init__(self, worksheets):
        self._worksheets = worksheets  # dict: tab name -> FakeWorksheet

    def worksheet(self, name):
        return self._worksheets[name]


class FakeClient:
    def __init__(self, spreadsheet):
        self._spreadsheet = spreadsheet
        self.opened = []

    def open_by_key(self, sheet_id):
        self.opened.append(sheet_id)
        return self._spreadsheet


def test_publish_frames_clears_then_updates_each_tab_with_frame_rows():
    fva = pd.DataFrame(
        {"business_date": [20260616, 20260617], "net_sales_actual": [100.0, 200.0]}
    )
    metrics = pd.DataFrame({"model": ["prophet"], "wape": [13.4]})
    frames = {"forecast_vs_actuals": fva, "model_metrics": metrics}

    worksheets = {tab: FakeWorksheet() for tab in frames}
    client = FakeClient(FakeSpreadsheet(worksheets))

    counts = publish_frames(client, "SHEET123", frames)

    # opened exactly the sheet we asked for, by key
    assert client.opened == ["SHEET123"]
    # every tab: cleared THEN updated (that order), with EXACTLY frame_to_rows output
    for tab, frame in frames.items():
        ws = worksheets[tab]
        assert ws.calls == ["clear", "update"]
        assert ws.updated == frame_to_rows(frame)
    # returns the data-row count per tab (header excluded)
    assert counts == {"forecast_vs_actuals": 2, "model_metrics": 1}
