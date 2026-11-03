FACT = {
    "id": "mapinarrow-arrow-types",
    "category": "python-udfs",
    "text": """Build `mapInArrow` output batches with an Arrow schema that matches the declared schema
        exactly (int32 for `int`, int64 for `long`), for example `pa.RecordBatch.from_pydict(rows,
        schema=...)`. Sail accepts an int64 column declared as `int`, but Apache Spark fails on it, so code
        that relies on the leniency is not portable.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

import pyarrow as pa


def as_int64(batches):
    for batch in batches:
        # Python ints become int64 in Arrow, while the declared column type is `int` (int32).
        yield pa.RecordBatch.from_pydict({"n": [v * 2 for v in batch.column("id").to_pylist()]})


def check(spark, engine):
    try:
        rows = spark.range(5).mapInArrow(as_int64, "n int").collect()
    except Exception as error:  # noqa: BLE001
        assert engine == "spark", f"Sail rejected an int64 batch declared as int: {str(error)[:300]}"
        assert "getInt" in str(error) or "UNSUPPORTED_CALL" in str(error), (
            f"other failure: {str(error)[:300]}"
        )
        return {"spark": "rejected"}
    assert engine == "sail", "Spark now accepts int64 declared as int; update this fact"
    values = sorted(r.n for r in rows)
    assert values == [0, 2, 4, 6, 8], f"unexpected values: {values}"
    return {"sail": "accepted", "values": values}
