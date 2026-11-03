FACT = {
    "id": "cache-does-not-keep-results",
    "category": "silent-behavior",
    "core": True,
    "text": """`cache()` and `persist()` are accepted but do not keep results: every action recomputes
        the DataFrame, including any UDF or model call in it. To reuse an expensive result (for example
        model outputs), write it to Parquet or a table and read it back.""",
    "engines": ["sail", "spark"],
}

import tempfile
from pathlib import Path

from pyspark.sql import functions as F


def udf_calls(spark, keep, rows: int) -> int:
    log = Path(tempfile.mkdtemp()) / "calls.log"
    log_path = str(log)

    @F.udf("long")
    def counted(x):
        with open(log_path, "a") as f:
            f.write("x\n")
        return x

    df = keep(spark.range(rows).select(counted("id").alias("v")))
    df.collect()
    df.collect()
    return len(log.read_text().splitlines())


def check(spark, engine):
    rows = 20
    calls = {
        "cache": udf_calls(spark, lambda df: df.cache(), rows),
        "persist": udf_calls(spark, lambda df: df.persist(), rows),
    }
    expected = 2 * rows if engine == "sail" else rows
    assert calls == {"cache": expected, "persist": expected}, (
        f"expected {expected} UDF calls each, got {calls}"
    )
    return {"rows": rows, "udf_calls": calls}
