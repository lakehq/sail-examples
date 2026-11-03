# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "pytest==9.1.1",
# ]
# ///
"""Unit-test Spark transformation code with pytest, using Sail as the local engine.

`clean_orders` is ordinary Spark DataFrame code. `test_orders.py` tests it against a Sail
session created once per test run by the fixture in `conftest.py`, with no Java and no cluster.
Running this file runs those tests.
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def clean_orders(orders: DataFrame) -> DataFrame:
    """Normalize emails, keep the latest version of each order, and store amounts in cents."""
    latest = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    return (
        orders.withColumn("email", F.lower(F.trim("email")))
        .withColumn("version", F.row_number().over(latest))
        .filter("version = 1")
        .filter(F.col("amount") >= 0)
        .select(
            "order_id",
            "email",
            F.round(F.col("amount") * 100).cast("long").alias("amount_cents"),
            "updated_at",
        )
    )


SAMPLE = [
    (1, "  Ana@Example.com ", 12.5, "2026-01-01 10:00:00"),
    (1, "ana@example.com", 13.0, "2026-01-02 09:00:00"),
    (2, "BO@example.com", 7.25, "2026-01-01 11:00:00"),
    (3, "cy@example.com", -4.0, "2026-01-03 12:00:00"),
    (4, "Di@Example.com", 0.1, "2026-01-04 08:30:00"),
]


def sample_orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        SAMPLE, "order_id long, email string, amount double, updated_at string"
    ).withColumn("updated_at", F.to_timestamp_ntz("updated_at"))


def run(spark: SparkSession) -> DataFrame:
    return clean_orders(sample_orders(spark)).orderBy("order_id")


if __name__ == "__main__":
    import pytest

    here = Path(__file__).parent
    sys.exit(pytest.main(["-q", "-p", "no:cacheprovider", str(here / "test_orders.py")]))
