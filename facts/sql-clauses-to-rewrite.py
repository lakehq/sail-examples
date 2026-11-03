FACT = {
    "id": "sql-clauses-to-rewrite",
    "category": "missing-features",
    "text": """A few Spark SQL clauses need another form on Sail: filter on `row_number()` in a subquery
        instead of `QUALIFY`; use `df.groupBy(...).pivot(col, values)` instead of the SQL `PIVOT` clause;
        number sessions with `lag` and a running `sum` instead of `session_window`. `CACHE TABLE` and
        `UNCACHE TABLE` are not supported; save the result to Parquet and read it back instead.""",
    "engines": ["sail", "spark"],
    "order": 4,
}

from pyspark.sql import functions as F

UNSUPPORTED = {
    "qualify": "SELECT a FROM VALUES (1), (2) AS t(a) QUALIFY row_number() OVER (ORDER BY a) = 1",
    "pivot_clause": "SELECT * FROM (SELECT 1 k, 'a' c) PIVOT (count(*) FOR c IN ('a', 'b'))",
    "session_window": "SELECT count(*) FROM VALUES (TIMESTAMP'2024-01-01 00:00:00') AS t(ts) "
    "GROUP BY session_window(ts, '10 minutes')",
    "cache_table": "CACHE TABLE clause_view",
    "uncache_table": "UNCACHE TABLE IF EXISTS clause_view",
}
# The error each clause gives on Sail: a parse error at the clause, or an unsupported plan node.
EVIDENCE = {
    "qualify": "found QUALIFY",
    "pivot_clause": "expected '('",
    "session_window": "function: session_window",
    "cache_table": "PlanNode::CacheTable",
    "uncache_table": "PlanNode::UncacheTable",
}


def outcome(spark, sql: str) -> str | None:
    try:
        spark.sql(sql).collect()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW clause_view AS SELECT 1 AS a")
    errors = {name: outcome(spark, sql) for name, sql in UNSUPPORTED.items()}

    # The rewrites work on both engines.
    first = spark.sql(
        "SELECT a FROM (SELECT a, row_number() OVER (ORDER BY a) rn FROM VALUES (1), (2) AS t(a)) WHERE rn = 1"
    ).collect()
    assert [r.a for r in first] == [1], f"row_number filter gave {first}"
    pivoted = spark.createDataFrame([(1, "a")], "k int, c string").groupBy("k").pivot("c", ["a", "b"]).count()
    assert [tuple(r) for r in pivoted.collect()] == [(1, 1, None)], "DataFrame pivot changed"
    assert F is not None

    if engine == "spark":
        assert not any(errors.values()), f"Spark rejected a clause: {errors}"
        return {"spark": "all supported"}
    for name, error in errors.items():
        assert error is not None, f"{name} now works on Sail; update this fact"
        assert EVIDENCE[name] in error, f"{name} failed for another reason: {error[:200]}"
    return {"unsupported_on_sail": sorted(errors)}
