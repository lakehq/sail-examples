# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Run Spark SQL and DataFrame code on Sail: no Java, no cluster, no setup."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def run(spark: SparkSession) -> DataFrame:
    # 10,000 synthetic orders, generated in the engine so the example needs no data files.
    orders = spark.range(10_000).select(
        (F.col("id") % 250).alias("customer_id"),
        (F.col("id") % 30 + 1).alias("day"),
        ((F.col("id") * 7919) % 1000 / 10.0).alias("amount"),
    )
    orders.createOrReplaceTempView("orders")

    # Plain Spark SQL.
    per_customer = spark.sql(
        """
        SELECT customer_id,
               count(*)    AS orders,
               round(sum(amount), 2) AS spend,
               max(day)    AS last_day
        FROM orders
        GROUP BY customer_id
        """
    )

    # The DataFrame API and a window function, on the same data.
    ranked = per_customer.withColumn(
        "spend_rank", F.dense_rank().over(Window.orderBy(F.desc("spend"), "customer_id"))
    )
    return ranked.filter(F.col("spend_rank") <= 10).orderBy("spend_rank")


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    run(spark).show()
    spark.stop()
    server.stop()
