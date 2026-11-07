FACT = {
    "id": "streaming-limits",
    "title": "Check streaming support before porting a job",
    "category": "missing-features",
    "text": """Structured Streaming support is partial: `writeStream.foreachBatch()` fails and the
        `memory` sink is not available, while both work on Apache Spark. Check the Sail streaming docs before
        porting a streaming job.""",
    "engines": ["sail", "spark"],
    "order": 4,
}


def attempt(start) -> str | None:
    try:
        start().stop()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    foreach = attempt(lambda: stream.writeStream.foreachBatch(lambda df, epoch: None).start())
    memory = attempt(lambda: stream.writeStream.format("memory").queryName("fact_memory_sink").start())
    if engine == "spark":
        assert foreach is None and memory is None, f"failed on Spark: {foreach or memory}"
        return {"spark": "both started"}
    assert foreach is not None, "foreachBatch now works on Sail; update this fact"
    assert "Python UDF output type" in foreach or "foreach" in foreach.lower(), f"other failure: {foreach}"
    assert memory is not None, "the memory sink now works on Sail; update this fact"
    assert "memory" in memory.lower(), f"other failure: {memory}"
    return {"foreachBatch": foreach[:120], "memory_sink": memory[:120]}
