# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
# ]
# ///
"""The same per-row calculation in every Python style Spark offers, checked to agree.

score = sqrt(a^2 + b^2) + ln(1 + c), computed with built-in functions, a row UDF, a pandas UDF,
an Arrow UDF and mapInArrow. The README explains when to reach for each one.
"""

import math
from collections.abc import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import arrow_udf, pandas_udf


def inputs(spark: SparkSession) -> DataFrame:
    return spark.range(100_000).select(
        "id",
        ((F.col("id") % 97) / 7.0).alias("a"),
        ((F.col("id") % 89) / 11.0).alias("b"),
        ((F.col("id") % 83) / 3.0).alias("c"),
    )


# 1. Built-in functions: no Python runs per row.
def builtin_score(a, b, c):
    return F.sqrt(a * a + b * b) + F.log1p(c)


# Styles 2 to 4 are plain functions here and become UDFs inside run(): with PySpark 4, wrapping a
# function as a UDF needs an active Spark session, which does not exist yet when a module is imported.


# 2. Row UDF: Python is called once per row.
def row_score(a: float, b: float, c: float) -> float:
    return math.sqrt(a * a + b * b) + math.log1p(c)


# 3. pandas UDF: called once per batch, with pandas Series.
def pandas_score(a: pd.Series, b: pd.Series, c: pd.Series) -> pd.Series:
    return np.sqrt(a * a + b * b) + np.log1p(c)


# 4. Arrow UDF: called once per batch, with Arrow arrays and pyarrow.compute.
def arrow_score(a: pa.Array, b: pa.Array, c: pa.Array) -> pa.Array:
    return pc.add(pc.sqrt(pc.add(pc.multiply(a, a), pc.multiply(b, b))), pc.log1p(c))


# 5. mapInArrow: whole record batches in, whole record batches out. Setup before the loop
#    runs once per partition, which makes it the place to load a model or open a client.
def map_scores(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        a, b, c = (batch.column(name) for name in ("a", "b", "c"))
        score = pc.add(pc.sqrt(pc.add(pc.multiply(a, a), pc.multiply(b, b))), pc.log1p(c))
        yield pa.RecordBatch.from_arrays([batch.column("id"), score], names=["id", "map_in_arrow"])


def run(spark: SparkSession) -> dict[str, DataFrame]:
    data = inputs(spark)
    a, b, c = F.col("a"), F.col("b"), F.col("c")
    row_udf = F.udf(row_score, "double")
    pandas_udf_score = pandas_udf(pandas_score, "double")
    arrow_udf_score = arrow_udf(arrow_score, "double")
    scores = data.select(
        "id",
        builtin_score(a, b, c).alias("builtin"),
        row_udf(a, b, c).alias("row_udf"),
        pandas_udf_score(a, b, c).alias("pandas_udf"),
        arrow_udf_score(a, b, c).alias("arrow_udf"),
    ).join(data.mapInArrow(map_scores, "id long, map_in_arrow double"), "id")

    styles = ["row_udf", "pandas_udf", "arrow_udf", "map_in_arrow"]
    differences = scores.select(*[F.max(F.abs(F.col(s) - F.col("builtin"))).alias(s) for s in styles])
    return {
        "first_rows": scores.orderBy("id").limit(5),
        "max_difference_from_builtin": differences,
        "rows": scores.agg(F.count("*").alias("rows")),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    # Keep the console comparison readable across math libraries and CPU instruction sets.
    print("Scores (rounded to 12 decimal places):")
    first_rows = results["first_rows"]
    first_rows.select(
        "id", *[F.round(F.col(name), 12).alias(name) for name in first_rows.columns if name != "id"]
    ).show()
    print("Maximum absolute difference from built-in <= 1e-12:")
    differences = results["max_difference_from_builtin"]
    differences.select(*[(F.col(name) <= 1e-12).alias(name) for name in differences.columns]).show()
    spark.stop()
    server.stop()
