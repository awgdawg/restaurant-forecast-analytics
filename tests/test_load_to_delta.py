import pandas as pd

from load.load_to_delta import (
    COLUMNS,
    bronze_ddl,
    days_needing_load,
    insert_sql,
    parquet_day_counts,
    rows_from_df,
)


def test_columns_match_flatten_output():
    expected = {
        "business_date",
        "order_guid",
        "opened_date",
        "closed_date",
        "source",
        "dining_option_guid",
        "num_guests",
        "num_checks",
        "net_amount",
        "total_amount",
        "tax_amount",
        "tip_amount",
        "deferred_amount",
        "voided",
        "deleted",
    }
    assert set(COLUMNS) == expected


def test_bronze_ddl_lists_every_column():
    ddl = bronze_ddl("bronze_orders")
    assert "CREATE TABLE IF NOT EXISTS bronze_orders" in ddl
    assert "USING DELTA" in ddl
    for col in COLUMNS:
        assert col in ddl


def test_insert_sql_has_one_placeholder_per_column():
    sql = insert_sql("bronze_orders")
    assert sql.count("?") == len(COLUMNS)
    assert "INSERT INTO bronze_orders" in sql


def test_rows_from_df_are_tuples_in_column_order():
    df = pd.DataFrame([{c: i for i, c in enumerate(COLUMNS)}])
    rows = rows_from_df(df)
    assert rows == [tuple(range(len(COLUMNS)))]


def test_insert_sql_multi_row_scales_placeholders():
    sql = insert_sql("bronze_orders", 3)
    assert sql.count("?") == 3 * len(COLUMNS)
    assert "INSERT INTO bronze_orders" in sql


def test_days_needing_load_flags_missing_and_mismatched_only():
    parquet = {20260626: 100, 20260627: 179, 20260628: 90, 20260412: 102}
    bronze = {20260626: 100, 20260627: 179, 20260412: 0}  # 4/12 partial, 6/28 missing

    todo = days_needing_load(parquet, bronze)

    assert todo == [20260412, 20260628]  # sorted; matching days skipped


def test_days_needing_load_window_forces_recent_days():
    parquet = {20260626: 100, 20260627: 179, 20260628: 90}
    bronze = dict(parquet)  # counts all match -> nothing stale by count

    assert days_needing_load(parquet, bronze) == []
    # a trailing window still reloads the most recent days: post-close edits
    # (refunds, tip adjustments) can change values without changing counts
    assert days_needing_load(parquet, bronze, window=2) == [20260627, 20260628]
    # window larger than the number of days on disk: loads everything, no error
    assert days_needing_load(parquet, bronze, window=99) == sorted(parquet)


def test_parquet_day_counts_reads_metadata(tmp_path):
    for bd, n in [(20260627, 3), (20260628, 2)]:
        part = tmp_path / f"business_date={bd}"
        part.mkdir(parents=True)
        pd.DataFrame({"order_guid": [f"o{i}" for i in range(n)]}).to_parquet(
            part / "orders.parquet", index=False
        )

    counts = parquet_day_counts(tmp_path)

    assert counts == {20260627: 3, 20260628: 2}
