FACT = {
    "id": "iceberg-support",
    "title": "Read and update Iceberg tables directly",
    "category": "data-sources",
    "text": """Sail reads and writes Apache Iceberg tables with no extra package: `format("iceberg")` writes
        to a path (including `partitionBy`, append and overwrite), `CREATE TABLE ... USING iceberg LOCATION`,
        `UPDATE`, `DELETE`, `MERGE INTO`, and time travel to a snapshot with `option("snapshot-id", id)` or
        `VERSION AS OF id`. The metadata tables `t.snapshots` and `t.history` list the snapshot ids. In a
        table created with `PARTITIONED BY`, Sail lists the partition columns last; select columns by name.""",
    "engines": ["sail"],
    "order": 2,
}

import json
import tempfile
from pathlib import Path


def snapshot_ids(table: Path) -> list[int]:
    newest = max((table / "metadata").glob("*.metadata.json"), key=lambda p: int(p.name.split(".")[0][1:]))
    return [s["snapshot-id"] for s in json.loads(newest.read_text()).get("snapshots", [])]


def check(spark, engine):
    table = Path(tempfile.mkdtemp()) / "t"
    path = str(table)
    rows = spark.createDataFrame([(1, "a", "m1"), (2, "b", "m2")], "id int, v string, month string")
    rows.write.format("iceberg").partitionBy("month").save(path)
    assert (table / "data" / "month=m1").is_dir(), "partitionBy did not write partition folders"
    rows.limit(1).write.format("iceberg").mode("append").save(path)
    spark.sql(f"CREATE TABLE iceberg_fact USING iceberg LOCATION '{path}'")
    spark.sql("UPDATE iceberg_fact SET v = 'z' WHERE id = 2")
    spark.sql("DELETE FROM iceberg_fact WHERE v = 'a'")
    spark.sql(
        "MERGE INTO iceberg_fact t USING (SELECT 3 AS id, 'c' AS v, 'm3' AS month) s "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    current = sorted(tuple(r) for r in spark.table("iceberg_fact").collect())
    assert current == [(2, "z", "m2"), (3, "c", "m3")], f"table is {current}"

    ids = snapshot_ids(table)
    assert len(ids) >= 2, f"expected a snapshot per write, found {ids}"
    first = spark.read.format("iceberg").option("snapshot-id", str(ids[0])).load(path).count()
    first_sql = spark.sql(f"SELECT count(*) FROM iceberg_fact VERSION AS OF {ids[0]}").first()[0]
    assert first == first_sql == 2, f"snapshot {ids[0]} has {first} and {first_sql} rows"

    listed = {r.snapshot_id for r in spark.sql("SELECT snapshot_id FROM iceberg_fact.snapshots").collect()}
    assert listed == set(ids), f"t.snapshots lists {sorted(listed)}, the metadata file {sorted(ids)}"
    history = {r.snapshot_id for r in spark.sql("SELECT snapshot_id FROM iceberg_fact.history").collect()}
    assert history and history <= set(ids), f"t.history lists {sorted(history)}"

    rows.limit(1).write.format("iceberg").mode("overwrite").save(path)
    assert spark.read.format("iceberg").load(path).count() == 1, "overwrite did not replace the rows"

    declared = Path(tempfile.mkdtemp()) / "declared"
    spark.sql(
        "CREATE TABLE iceberg_declared (month INT, day DATE, v STRING) USING iceberg "
        f"PARTITIONED BY (month) LOCATION '{declared}'"
    )
    columns = spark.table("iceberg_declared").columns
    assert columns == ["day", "v", "month"], f"declared columns now read back as {columns}; update this fact"
    return {"snapshots": len(ids), "history": len(history), "first_snapshot_rows": first}
