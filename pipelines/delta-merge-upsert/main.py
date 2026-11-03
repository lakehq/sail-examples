# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Upsert changes into a Delta Lake table with MERGE INTO, then read the version before it.

Sail reads and writes Delta Lake natively, so no Delta package or JVM jars are needed. One
MERGE statement updates changed rows, inserts new ones and deletes rows flagged for removal.
"""

import atexit
import shutil
import tempfile

from pyspark.sql import DataFrame, SparkSession

CUSTOMERS = [
    (1, "Ana", "gold"),
    (2, "Bo", "silver"),
    (3, "Cy", "bronze"),
    (4, "Di", "silver"),
    (5, "Ed", "bronze"),
]
CHANGES = [
    (2, "Bo", "gold", False),  # tier upgrade
    (3, "Cyrus", "bronze", False),  # name change
    (5, "Ed", "bronze", True),  # account closed
    (6, "Fay", "silver", False),  # new customer
]


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def run(spark: SparkSession) -> dict[str, DataFrame]:
    path = temp_dir("customers-")
    spark.createDataFrame(CUSTOMERS, "id long, name string, tier string").write.format("delta").mode(
        "overwrite"
    ).save(path)
    spark.sql("DROP TABLE IF EXISTS customers")  # safe to run again in the same session
    spark.sql(f"CREATE TABLE customers USING delta LOCATION '{path}'")
    spark.createDataFrame(
        CHANGES, "id long, name string, tier string, deleted boolean"
    ).createOrReplaceTempView("changes")

    spark.sql(
        """
        MERGE INTO customers AS c
        USING changes AS u
        ON c.id = u.id
        WHEN MATCHED AND u.deleted THEN DELETE
        WHEN MATCHED THEN UPDATE SET c.name = u.name, c.tier = u.tier
        WHEN NOT MATCHED AND NOT u.deleted THEN INSERT (id, name, tier) VALUES (u.id, u.name, u.tier)
        """
    )

    current = spark.read.format("delta").load(path).orderBy("id")
    before = spark.read.format("delta").option("versionAsOf", 0).load(path).orderBy("id")
    by_tier = spark.sql("SELECT tier, count(*) AS customers FROM customers GROUP BY tier ORDER BY tier")
    return {"current": current, "version_0": before, "by_tier": by_tier}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    print("After MERGE:")
    results["current"].show()
    print("Version 0 (before MERGE):")
    results["version_0"].show()
    spark.stop()
    server.stop()
