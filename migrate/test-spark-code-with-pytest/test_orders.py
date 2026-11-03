from datetime import datetime

from main import clean_orders, sample_orders


def rows(df):
    """Collected rows as sorted tuples: order-insensitive and readable in a failure message."""
    return sorted(tuple(row) for row in df.collect())


def test_keeps_the_latest_version_of_each_order(spark):
    result = clean_orders(sample_orders(spark)).filter("order_id = 1")
    assert [row.amount_cents for row in result.collect()] == [1300]


def test_normalizes_emails(spark):
    emails = {row.email for row in clean_orders(sample_orders(spark)).collect()}
    assert emails == {"ana@example.com", "bo@example.com", "di@example.com"}


def test_drops_negative_amounts(spark):
    assert clean_orders(sample_orders(spark)).filter("order_id = 3").count() == 0


def test_whole_result(spark):
    assert rows(clean_orders(sample_orders(spark))) == [
        (1, "ana@example.com", 1300, datetime(2026, 1, 2, 9, 0)),
        (2, "bo@example.com", 725, datetime(2026, 1, 1, 11, 0)),
        (4, "di@example.com", 10, datetime(2026, 1, 4, 8, 30)),
    ]
