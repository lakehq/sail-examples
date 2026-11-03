FACT = {
    "id": "partition-overwrite",
    "category": "silent-behavior",
    "core": True,
    "text": """Sail does not support dynamic partition overwrite for Parquet: with `partitionOverwriteMode`
        set to `dynamic` (in the session or as a writer option), `df.write.mode("overwrite").partitionBy(c)`
        still replaces every partition of the path, and `INSERT OVERWRITE` replaces the whole table, where
        Apache Spark replaces only the partitions the data contains. Delta Lake tables follow the setting
        on both engines. `INSERT OVERWRITE t PARTITION (c = value)` is not supported. To replace one Parquet
        partition, overwrite its own directory (`path/c=value`, without the partition column), or write
        Delta Lake.""",
    "engines": ["sail", "spark"],
    "spark_extras": ["delta"],
    "order": 1,
}

import tempfile


def rows_by_partition(spark, path: str, fmt: str = "parquet") -> list[tuple[int, int]]:
    reader = spark.read.format(fmt)
    # Parquet partition values would otherwise be inferred; Delta rejects a read schema.
    frame = (reader.schema("v int, m int") if fmt == "parquet" else reader).load(path)
    return sorted(tuple(r) for r in frame.groupBy("m").count().collect())


def dynamic_overwrites(spark, fmt: str) -> tuple[list, list]:
    """Partition row counts after an overwrite of partition m=1, by session setting and by writer option."""
    both = spark.createDataFrame([(1, 1), (2, 1), (3, 2)], "v int, m int")
    new_m1 = spark.createDataFrame([(9, 1)], "v int, m int")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    path = tempfile.mkdtemp()
    both.write.format(fmt).mode("overwrite").partitionBy("m").save(path)
    new_m1.write.format(fmt).mode("overwrite").partitionBy("m").save(path)
    session = rows_by_partition(spark, path, fmt)
    # The writer option alone, with the session setting back at its default (static).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    option_path = tempfile.mkdtemp()
    both.write.format(fmt).mode("overwrite").partitionBy("m").save(option_path)
    writer = (
        new_m1.write.format(fmt)
        .mode("overwrite")
        .partitionBy("m")
        .option("partitionOverwriteMode", "dynamic")
    )
    writer.save(option_path)
    return session, rows_by_partition(spark, option_path, fmt)


def insert_overwrite(spark, name: str, fmt: str) -> list[tuple[int, int]]:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(
        f"CREATE TABLE {name} (v INT, m INT) USING {fmt} PARTITIONED BY (m) LOCATION '{tempfile.mkdtemp()}'"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1, 1), (2, 2)")
    spark.sql(f"INSERT OVERWRITE {name} SELECT 9, 1")
    return sorted(tuple(r) for r in spark.table(name).select("v", "m").collect())


def check(spark, engine):
    try:
        parquet_session, parquet_option = dynamic_overwrites(spark, "parquet")
        assert parquet_option == parquet_session, "the writer option and the session setting differ"
        delta_session, delta_option = dynamic_overwrites(spark, "delta")

        # Overwriting the partition's own directory keeps the others on both engines.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        path2 = tempfile.mkdtemp()
        spark.createDataFrame([(1, 1), (2, 1), (3, 2)], "v int, m int").write.mode("overwrite").partitionBy(
            "m"
        ).parquet(path2)
        spark.createDataFrame([(9,)], "v int").write.mode("overwrite").parquet(f"{path2}/m=1")
        assert rows_by_partition(spark, path2) == [(1, 1), (2, 1)], (
            f"directory overwrite: {rows_by_partition(spark, path2)}"
        )

        parquet_insert = insert_overwrite(spark, "overwrite_fact", "parquet")
        delta_insert = insert_overwrite(spark, "overwrite_delta_fact", "delta")
        try:
            spark.sql("INSERT OVERWRITE overwrite_fact PARTITION (m = 2) SELECT 7")
            static_partition = "supported"
        except Exception as error:  # noqa: BLE001
            static_partition = " ".join(str(error).split())
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    # Spark keeps partition m=2 (one row); a full replace leaves only the new m=1 row.
    replaced_one = [(1, 1), (2, 1)]
    assert delta_session == delta_option == replaced_one, (
        f"Delta dynamic overwrite: {delta_session}, {delta_option}"
    )
    assert delta_insert == [(2, 2), (9, 1)], f"Delta INSERT OVERWRITE gave {delta_insert}"
    observed = {
        "parquet_dynamic": parquet_session,
        "parquet_insert_overwrite": parquet_insert,
        "delta_dynamic": delta_session,
        "partition_clause": static_partition[:60],
    }
    if engine == "spark":
        assert parquet_session == replaced_one, f"Spark dynamic overwrite gave {parquet_session}"
        assert parquet_insert == [(2, 2), (9, 1)], f"Spark INSERT OVERWRITE gave {parquet_insert}"
        assert static_partition == "supported", static_partition
        return observed
    assert parquet_session == [(1, 1)], (
        f"Sail dynamic overwrite now gives {parquet_session}: update this fact"
    )
    assert parquet_insert == [(9, 1)], f"Sail INSERT OVERWRITE now gives {parquet_insert}: update this fact"
    assert "partition" in static_partition.lower(), (
        f"INSERT OVERWRITE ... PARTITION: {static_partition[:200]}"
    )
    return observed
