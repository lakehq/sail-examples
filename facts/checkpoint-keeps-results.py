FACT = {
    "id": "checkpoint-keeps-results",
    "title": "Use a checkpoint to reuse a result",
    "category": "configure",
    "text": """To reuse an expensive result without writing a table yourself, set
        `SAIL_EXECUTION__CHECKPOINT__PATH` to a directory before the server starts and call
        `df.checkpoint()`; the returned DataFrame is not recomputed by later actions.""",
    "engines": ["sail"],
    "order": 4,
    "env": {"SAIL_EXECUTION__CHECKPOINT__PATH": "{tmp}"},
}

import tempfile
from pathlib import Path

from pyspark.sql import functions as F


def check(spark, engine):
    log = Path(tempfile.mkdtemp()) / "calls.log"
    log_path = str(log)

    @F.udf("long")
    def counted(x):
        with open(log_path, "a") as f:
            f.write("x\n")
        return x

    rows = 20
    saved = spark.range(rows).select(counted("id").alias("v")).checkpoint()
    after_checkpoint = len(log.read_text().splitlines())
    saved.collect()
    saved.collect()
    total = len(log.read_text().splitlines())
    assert after_checkpoint == rows, f"checkpoint should compute once ({rows}), got {after_checkpoint}"
    assert total == rows, f"later actions recomputed the UDF: {total} calls"
    return {"udf_calls": total, "rows": rows}
