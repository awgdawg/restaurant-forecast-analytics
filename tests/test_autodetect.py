from datetime import date, timedelta

from ingest.extract import _confirmed_empty, find_earliest_business_date


def test_confirmed_empty_distinguishes_closed_day_from_pre_start():
    data_days = {date(2025, 1, 10), date(2025, 1, 11), date(2025, 1, 13)}

    def has(d):
        return d in data_days

    # 12th has no orders but the 11th does -> a weekly closed day, not the start
    assert not _confirmed_empty(has, date(2025, 1, 12))
    # well before any data: empty with an empty neighbor -> genuinely pre-start
    assert _confirmed_empty(has, date(2025, 1, 5))


def test_finds_exact_first_day_with_data():
    start = date(2025, 3, 15)
    # closed one day a week, offset so closures are never consecutive nor on the start
    closed = {start + timedelta(days=7 * i + 3) for i in range(80)}

    def has(d):
        return d >= start and d not in closed

    result = find_earliest_business_date(has, date(2026, 6, 27))

    assert result == start


def test_returns_floor_when_data_predates_lookback():
    def has(d):
        return True  # data as far back as we care to look

    today = date(2026, 6, 27)

    result = find_earliest_business_date(has, today, max_lookback_days=900)

    assert result == today - timedelta(days=900)
