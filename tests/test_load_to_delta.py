import pandas as pd

from load.load_to_delta import COLUMNS, bronze_ddl, insert_sql, rows_from_df


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
