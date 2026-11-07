# /// script
# dependencies = ["fastavro>=1.9"]
# ///
FACT = {
    "id": "file-formats",
    "title": "Choose a supported file format",
    "category": "data-sources",
    "core": True,
    "text": """Parquet, CSV, JSON, text and Delta Lake read and write with the standard `spark.read`
        and `df.write` calls and need no extra packages. Avro can be read but not written. ORC (read or
        write) and the `image` source are not available.""",
    "engines": ["sail"],
    "order": 2,
}

import tempfile
from pathlib import Path

import fastavro
import pyarrow as pa
from pyarrow import orc


def attempt(action) -> str | None:
    try:
        action()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    work = Path(tempfile.mkdtemp())
    df = spark.range(5).selectExpr("id", "cast(id AS string) AS s")
    for fmt in ("parquet", "csv", "json", "delta"):
        path = str(work / fmt)
        df.write.format(fmt).option("header", "true").save(path)
        got = spark.read.format(fmt).option("header", "true").load(path).count()
        assert got == 5, f"{fmt}: wrote 5 rows, read {got}"
    df.select("s").write.text(str(work / "text"))
    assert spark.read.text(str(work / "text")).count() == 5, "text round trip failed"

    (work / "avro-in").mkdir()
    schema = {"type": "record", "name": "r", "fields": [{"name": "id", "type": "long"}]}
    with open(work / "avro-in" / "part.avro", "wb") as f:
        fastavro.writer(f, schema, [{"id": 1}, {"id": 2}, {"id": 3}])
    avro_rows = spark.read.format("avro").load(str(work / "avro-in")).count()
    assert avro_rows == 3, f"Avro read returned {avro_rows} rows, expected 3"

    (work / "orc-in").mkdir()
    orc.write_table(pa.table({"id": [1, 2]}), str(work / "orc-in" / "part.orc"))
    failures = {
        "avro write": attempt(lambda: df.write.format("avro").save(str(work / "avro-out"))),
        "orc read": attempt(lambda: spark.read.orc(str(work / "orc-in")).collect()),
        "orc write": attempt(lambda: df.write.orc(str(work / "orc-out"))),
        "image read": attempt(lambda: spark.read.format("image").load(str(work / "parquet")).collect()),
    }
    now_working = [name for name, error in failures.items() if error is None]
    assert not now_working, f"now available: {now_working}; update this fact"
    # Each error must be about the feature itself; the exact wording may change between releases.
    expected = {"avro write": "not implemented", "orc read": "orc", "orc write": "orc", "image read": "image"}
    wrong = {k: failures[k] for k, text in expected.items() if text not in failures[k].lower()}
    assert not wrong, f"failed for other reasons: {wrong}"
    return {
        "round_trip": ["parquet", "csv", "json", "delta", "text"],
        "avro_read_rows": avro_rows,
        "unavailable": failures,
    }
