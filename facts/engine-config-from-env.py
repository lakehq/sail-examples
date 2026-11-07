FACT = {
    "id": "engine-config-from-env",
    "title": "Set engine options through the environment",
    "category": "configure",
    "core": True,
    "text": """Set Sail engine options as `SAIL_<SECTION>__<KEY>` environment variables before the server
        starts, for example `SAIL_EXECUTION__DEFAULT_PARALLELISM=8`. `spark.conf.set` changes only the Spark
        SQL session options Sail maps, such as `spark.sql.session.timeZone`; `spark.sql.shuffle.partitions`
        has no effect.""",
    "engines": ["sail"],
    "order": 1,
    "env": {"SAIL_EXECUTION__DEFAULT_PARALLELISM": "3"},
}

import re


def check(spark, engine):
    spark.conf.set("spark.sql.shuffle.partitions", "7")
    spark.range(100).selectExpr("id % 10 AS k").createOrReplaceTempView("t")
    plan = "\n".join(r[0] for r in spark.sql("EXPLAIN SELECT k, count(*) FROM t GROUP BY k").collect())
    partitions = {int(n) for n in re.findall(r"Hash\(\[[^\]]*\], (\d+)\)", plan)}
    assert partitions == {3}, (
        f"expected hash partitioning into 3 (from the env var), got {partitions}:\n{plan}"
    )

    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    zone = spark.sql("SELECT current_timezone()").collect()[0][0]
    assert zone == "Asia/Tokyo", f"session time zone was not applied: {zone}"
    return {"hash_partitions": sorted(partitions), "session_time_zone": zone}
