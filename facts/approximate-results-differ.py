FACT = {
    "id": "approximate-results-differ",
    "title": "Approximate results and random samples can differ",
    "category": "silent-behavior",
    "text": """Approximate and random results can differ from Apache Spark's: `percentile_approx`,
        `approx_count_distinct`, the percentiles from `df.summary()`, and the rows chosen by
        `sample(fraction, seed)` and `randomSplit(weights, seed)`. With the same seed, `sample` and
        `randomSplit` pick the same rows again on Sail, but not the rows Spark picks. When results must match, use
        `percentile`, `median`, `count(DISTINCT ...)`, and a deterministic filter on a hash of a key
        (`hash` and `xxhash64` return the same values on both).""",
    "engines": ["sail", "spark"],
    "order": 4,
}

from pyspark.sql import functions as F

# Results of the same calls on Apache Spark 4.2 (deterministic for this data).
SPARK = {
    "percentile_approx": 49.0,
    "approx_count_distinct": 9854,
    "summary_50": "49.0",
    "sample_rows": 114,
    "split_rows": [814, 186],
}


def check(spark, engine):
    # Four explicit partitions, so the data's layout does not depend on the host's core count.
    df = spark.range(0, 1000, 1, 4).select(
        (F.col("id") % 100).cast("double").alias("x"), F.col("id").alias("k")
    )
    row = df.select(
        F.percentile_approx("x", 0.5).alias("approx"),
        F.expr("percentile(x, 0.5)").alias("exact"),
        F.median("x").alias("median"),
    ).first()
    big = spark.range(0, 10000).select((F.col("id") * 7919 % 100003).alias("v"))
    approx_distinct = big.select(F.approx_count_distinct("v")).first()[0]
    exact_distinct = big.select(F.countDistinct("v")).first()[0]
    summary_50 = {r.summary: r.x for r in df.select("x").summary("50%").collect()}["50%"]
    samples = [sorted(r.k for r in df.sample(fraction=0.1, seed=7).collect()) for _ in range(2)]
    splits = [
        [sorted(r.k for r in part.collect()) for part in df.randomSplit([0.8, 0.2], seed=7)] for _ in range(2)
    ]

    hashes = spark.sql("SELECT hash(42), hash('a'), xxhash64('a'), pmod(hash('key-1'), 10)").first()
    assert tuple(hashes) == (29417773, 1485273170, -8582455328737087284, 5), (
        f"hash values on {engine}: {hashes}"
    )
    # The exact forms agree on both engines.
    assert (row.exact, row.median, exact_distinct) == (49.5, 49.5, 10000), (row, exact_distinct)
    assert samples[0] == samples[1], f"sample(seed) did not pick the same rows twice on {engine}"
    assert splits[0] == splits[1], f"randomSplit(seed) did not pick the same rows twice on {engine}"
    observed = {
        "percentile_approx": row.approx,
        "approx_count_distinct": approx_distinct,
        "summary_50": summary_50,
        "sample_rows": len(samples[0]),
        "split_rows": [len(part) for part in splits[0]],
    }
    if engine == "spark":
        assert observed == SPARK, f"Spark's approximations changed: {observed}"
        return observed
    same = sorted(k for k in SPARK if observed[k] == SPARK[k])
    assert not same, f"Sail now matches Spark for {same}: {observed}; update this fact"
    return observed
