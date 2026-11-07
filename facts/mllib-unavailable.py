FACT = {
    "id": "mllib-unavailable",
    "title": "Use Python model libraries instead of MLlib",
    "category": "missing-features",
    "core": True,
    "text": """`pyspark.ml` (MLlib) is not available, although Spark Connect 4.x supports it. Train and
        score models with Python libraries (scikit-learn, PyTorch, ONNX Runtime) inside `mapInArrow` or a
        pandas UDF, or outside the engine.""",
    "engines": ["sail", "spark"],
    "order": 1,
}


def check(spark, engine):
    from pyspark.ml.feature import VectorAssembler

    df = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0)], ["a", "b"])
    try:
        rows = VectorAssembler(inputCols=["a", "b"], outputCol="features").transform(df).collect()
    except Exception as error:  # noqa: BLE001
        assert engine == "sail", f"pyspark.ml failed on Spark: {error}"
        assert "ML relation" in str(error) or "ml command" in str(error).lower(), f"other failure: {error}"
        return {"error": type(error).__name__, "message": str(error)[:200]}
    assert engine == "spark", "pyspark.ml now works on Sail; update this fact"
    return {"rows": len(rows)}
