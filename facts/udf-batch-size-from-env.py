FACT = {
    "id": "udf-batch-size-from-env",
    "category": "python-udfs",
    "text": """The number of rows `mapInArrow` and `mapInPandas` receive per batch is capped by the
        `SAIL_EXECUTION__BATCH_SIZE` environment variable, set before the server starts (default 8192 per the
        Sail configuration docs); `spark.sql.execution.arrow.maxRecordsPerBatch` has no effect. Lower it for
        wide rows such as images, documents or embeddings.""",
    "engines": ["sail"],
    "order": 4,
    "env": {"SAIL_EXECUTION__BATCH_SIZE": "256"},
}

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 60_000


def check(spark, engine):
    work = Path(tempfile.mkdtemp())
    pq.write_table(pa.table({"id": pa.array(range(ROWS), pa.int64())}), work / "rows.parquet")
    log_path = str(work / "batches.log")

    def arrow_sizes(it):
        for batch in it:
            with open(log_path, "a") as f:
                f.write(f"arrow {batch.num_rows}\n")
            yield batch

    def pandas_sizes(it):
        for frame in it:
            with open(log_path, "a") as f:
                f.write(f"pandas {len(frame)}\n")
            yield frame

    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")
    source = spark.read.parquet(str(work / "rows.parquet"))
    source.mapInArrow(arrow_sizes, "id long").count()
    source.mapInPandas(pandas_sizes, "id long").count()

    sizes: dict[str, list[int]] = {"arrow": [], "pandas": []}
    for line in Path(log_path).read_text().splitlines():
        kind, n = line.split()
        sizes[kind].append(int(n))
    for kind, values in sizes.items():
        assert sum(values) == ROWS, f"{kind}: rows lost or duplicated ({sum(values)})"
        assert max(values) == 256, f"{kind}: expected batches capped at 256 by the env var, max {max(values)}"
    return {kind: {"max_batch": max(v), "batches": len(v)} for kind, v in sizes.items()}
