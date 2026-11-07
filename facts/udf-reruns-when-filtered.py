FACT = {
    "id": "udf-reruns-when-filtered",
    "title": "A filter can make a Python function run again",
    "category": "python-udfs",
    "text": """As in Spark, a deterministic Python UDF can run more than once per row when the query filters on its
        output. Mark UDFs that are expensive or have side effects (model or API calls) with
        `.asNondeterministic()`, which makes them run once per row.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

import tempfile
from pathlib import Path

from pyspark.sql import functions as F


def calls_for(spark, nondeterministic: bool, rows: int) -> int:
    log = Path(tempfile.mkdtemp()) / "calls.log"
    log_path = str(log)

    def body(x):
        with open(log_path, "a") as f:
            f.write("x\n")
        return x

    udf = F.udf(body, "long")
    if nondeterministic:
        udf = udf.asNondeterministic()
    spark.range(rows).select(udf("id").alias("v")).filter("v >= 0").collect()
    return len(log.read_text().splitlines())


def check(spark, engine):
    rows = 40
    deterministic = calls_for(spark, False, rows)
    nondeterministic = calls_for(spark, True, rows)
    assert deterministic > rows, f"expected re-runs for a deterministic UDF, got {deterministic}"
    assert nondeterministic == rows, f"asNondeterministic() should run once per row, got {nondeterministic}"
    return {"rows": rows, "deterministic_calls": deterministic, "nondeterministic_calls": nondeterministic}
