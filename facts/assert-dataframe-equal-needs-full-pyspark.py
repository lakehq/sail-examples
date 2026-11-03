FACT = {
    "id": "assert-dataframe-equal-needs-full-pyspark",
    "category": "run-and-connect",
    "text": """With the lightweight `pyspark-client` package, `pyspark.testing.assertDataFrameEqual`
        fails before comparing anything (it imports pandas-on-Spark and looks for a local Spark
        installation). In tests against Sail, compare collected rows instead, for example
        `sorted(map(tuple, df.collect())) == expected`.""",
    "engines": ["sail"],
    "order": 3,
}


def check(spark, engine):
    try:
        from pyspark.testing import assertDataFrameEqual

        assertDataFrameEqual(spark.range(3), spark.range(3))
    except BaseException as error:  # noqa: BLE001 - it may also end in SystemExit
        message = f"{type(error).__name__}: {error}"
        expected = ("_builtin_table", "SPARK_HOME", "spark home", "find_spark_home", "pyspark")
        assert any(token.lower() in message.lower() for token in expected), f"other failure: {message[:300]}"
        return {"error": message[:160]}
    raise AssertionError("assertDataFrameEqual now works with pyspark-client; update this fact")
