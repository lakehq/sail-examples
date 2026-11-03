FACT = {
    "id": "udf-setup-per-partition",
    "category": "python-udfs",
    "core": True,
    "text": """To load a model or client once per partition, put the setup before the loop in
        `mapInArrow`, `mapInPandas` or an iterator `pandas_udf`. In an iterator `arrow_udf`, code before the
        loop runs once per batch instead.""",
    "engines": ["sail"],
    "order": 2,
}

import tempfile
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyspark.sql import functions as F
from pyspark.sql.functions import arrow_udf, pandas_udf


def check(spark, engine):
    log_dir = Path(tempfile.mkdtemp())
    log_path = str(log_dir)

    def mark(kind, what):
        with open(f"{log_path}/{kind}-{what}.log", "a") as f:
            f.write("x\n")

    def count(kind, what):
        path = log_dir / f"{kind}-{what}.log"
        return len(path.read_text().splitlines()) if path.exists() else 0

    @arrow_udf("long")
    def arrow_iter(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
        mark("arrow_udf", "setup")
        for array in it:
            mark("arrow_udf", "batch")
            yield array

    @pandas_udf("long")
    def pandas_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
        mark("pandas_udf", "setup")
        for series in it:
            mark("pandas_udf", "batch")
            yield series

    def map_arrow(it):
        mark("mapInArrow", "setup")
        for batch in it:
            mark("mapInArrow", "batch")
            yield batch

    def map_pandas(it):
        mark("mapInPandas", "setup")
        for frame in it:
            mark("mapInPandas", "batch")
            yield frame

    partitions = 2
    base = spark.range(0, 8000).repartition(partitions)
    base.select(arrow_iter("id").alias("y")).agg(F.sum("y")).collect()
    base.select(pandas_iter("id").alias("y")).agg(F.sum("y")).collect()
    base.mapInArrow(map_arrow, "id long").agg(F.sum("id")).collect()
    base.mapInPandas(map_pandas, "id long").agg(F.sum("id")).collect()

    kinds = ("arrow_udf", "pandas_udf", "mapInArrow", "mapInPandas")
    observed = {k: {"setup": count(k, "setup"), "batches": count(k, "batch")} for k in kinds}
    for kind in ("pandas_udf", "mapInArrow", "mapInPandas"):
        assert observed[kind]["setup"] == partitions, (
            f"{kind} setup should run once per partition: {observed}"
        )
        assert observed[kind]["batches"] > partitions, f"{kind} should see several batches: {observed}"
    arrow = observed["arrow_udf"]
    assert arrow["setup"] == arrow["batches"] > partitions, (
        f"arrow_udf setup should run per batch: {observed}"
    )
    return observed
