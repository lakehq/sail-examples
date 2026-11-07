FACT = {
    "id": "no-spark-context",
    "title": "Use DataFrames and SQL instead of RDDs",
    "category": "missing-features",
    "core": True,
    "text": """`spark.sparkContext` and the RDD API are not available, as on any Spark Connect server.
        Use the DataFrame and SQL APIs instead, for example `spark.createDataFrame([], schema)` rather
        than `sparkContext.emptyRDD()`.""",
    "engines": ["sail", "spark"],
    "order": 2,
}


def check(spark, engine):
    try:
        spark.sparkContext  # noqa: B018
    except Exception as error:  # noqa: BLE001
        assert "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(error), f"other failure: {error}"
        return {"error": type(error).__name__}
    raise AssertionError("spark.sparkContext is available; update this fact")
