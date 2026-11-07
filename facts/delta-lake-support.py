FACT = {
    "id": "delta-lake-support",
    "title": "Read and update Delta Lake tables directly",
    "category": "data-sources",
    "text": """Sail reads and writes Delta Lake with no extra package: `format("delta")` writes
        (`partitionBy`, append, `replaceWhere`), `CREATE TABLE ... USING delta`, `UPDATE`, `DELETE`,
        `MERGE INTO`, time travel (`versionAsOf`, `VERSION AS OF`), `ALTER TABLE ... ADD COLUMNS`,
        `DESCRIBE HISTORY`, `OPTIMIZE` and `VACUUM`. A `mergeSchema` append of computed (nullable) values
        also adds a column to a table created with `CREATE TABLE t (...) USING delta`. `RESTORE` is not
        supported; read the old version with `versionAsOf` instead. Apache Spark needs `delta-spark` with its
        session extension and catalog (per the Delta Lake docs).""",
    "engines": ["sail", "spark"],
    "spark_extras": ["delta"],
    "order": 1,
    "timeout": 180,
}

import tempfile
from pathlib import Path

# Maintenance commands both engines run. ADD COLUMNS goes last so the others see the original schema.
SUPPORTED = {
    "describe_history": "DESCRIBE HISTORY delta_fact",
    "optimize": "OPTIMIZE delta_fact",
    "vacuum": "VACUUM delta_fact RETAIN 168 HOURS",
    "alter_add_columns": "ALTER TABLE delta_fact ADD COLUMNS (note STRING)",
}
RESTORE = "RESTORE TABLE delta_fact TO VERSION AS OF 0"


def error_for(spark, sql: str) -> str | None:
    try:
        spark.sql(sql).collect()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    path = str(Path(tempfile.mkdtemp()) / "t")
    rows = spark.createDataFrame([(1, "a", "m1"), (2, "b", "m2")], "id int, v string, month string")
    rows.write.format("delta").partitionBy("month").save(path)
    assert (Path(path) / "month=m1").is_dir(), "partitionBy did not write partition folders"
    rows.limit(1).selectExpr("3 AS id", "'c' AS v", "'m3' AS month").write.format("delta").mode(
        "append"
    ).save(path)
    replacement = spark.createDataFrame([(9, "z", "m1")], "id int, v string, month string")
    replacement.write.format("delta").mode("overwrite").option("replaceWhere", "month = 'm1'").save(path)
    spark.sql(f"CREATE TABLE delta_fact USING delta LOCATION '{path}'")
    spark.sql("UPDATE delta_fact SET v = 'B' WHERE id = 2")
    spark.sql("DELETE FROM delta_fact WHERE id = 3")
    spark.sql(
        "MERGE INTO delta_fact t USING (SELECT 4 AS id, 'd' AS v, 'm4' AS month) s "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    current = sorted(tuple(r) for r in spark.table("delta_fact").select("id", "v", "month").collect())
    assert current == [(2, "B", "m2"), (4, "d", "m4"), (9, "z", "m1")], f"table is {current}"
    # Row counts after: create, append, replaceWhere, UPDATE, DELETE, MERGE.
    versions = [spark.read.format("delta").option("versionAsOf", v).load(path).count() for v in range(6)]
    assert versions == [2, 3, 3, 3, 2, 3], f"versions {versions}"
    sql_v0 = spark.sql(f"SELECT count(*) FROM delta.`{path}` VERSION AS OF 0").first()[0]
    assert sql_v0 == 2, f"VERSION AS OF 0 gave {sql_v0}"

    # Adding a column: a declared table, then a mergeSchema append whose new column is computed.
    declared = str(Path(tempfile.mkdtemp()) / "d")
    spark.sql(f"CREATE TABLE delta_declared (k INT, v DOUBLE) USING delta LOCATION '{declared}'")
    nullable = [field.nullable for field in spark.table("delta_declared").schema.fields]
    assert nullable == [True, True], f"declared columns have nullable {nullable}"
    spark.sql("SELECT 1 AS k, sum(1.0D) AS v").write.format("delta").mode("append").save(declared)
    added = spark.sql("SELECT 2 AS k, sum(2.0D) AS v, sum(0.5D) AS x")
    added.write.format("delta").mode("append").option("mergeSchema", "true").save(declared)
    evolved = sorted(tuple(r) for r in spark.read.format("delta").load(declared).collect())
    assert evolved == [(1, 1.0, None), (2, 2.0, 0.5)], f"schema evolution gave {evolved}"

    errors = {name: error_for(spark, sql) for name, sql in SUPPORTED.items()}
    assert not any(errors.values()), f"{engine} rejected a maintenance command: {errors}"
    history = {r.version for r in spark.sql("DESCRIBE HISTORY delta_fact").collect()}
    assert set(range(6)) <= history, f"DESCRIBE HISTORY lists versions {sorted(history)}"
    columns = spark.table("delta_fact").columns
    assert columns == ["id", "v", "month", "note"], f"ADD COLUMNS left columns {columns}"

    restore = error_for(spark, RESTORE)
    if engine == "spark":
        assert restore is None, f"Spark with delta-spark rejected RESTORE: {restore[:200]}"
        return {"versions": versions, "commands": "all supported"}
    assert restore is not None, "RESTORE now works on Sail; update this fact"
    assert "RESTORE" in restore, f"RESTORE failed for another reason: {restore[:200]}"
    return {"versions": versions, "history_versions": len(history), "unsupported": ["restore"]}
