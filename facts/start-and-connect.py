FACT = {
    "id": "start-and-connect",
    "category": "run-and-connect",
    "core": True,
    "text": """Connect to Sail with a normal PySpark Spark Connect session. In a script, start Sail
        in-process with `pysail.spark.SparkConnectServer()`, call `start()`, read the port from
        `server.listening_address`, and connect with `SparkSession.builder.remote(f"sc://localhost:{port}")`.
        As a long-running service, run `sail spark server` (default `127.0.0.1:50051`, per the Sail docs).
        `SELECT version()` returns the Sail version; `spark.version` returns the PySpark client's version.""",
    "engines": ["sail", "spark"],
    "order": 1,
}


def check(spark, engine):
    import pyspark

    engine_version = spark.sql("SELECT version()").collect()[0][0]
    assert spark.version == pyspark.__version__, (
        f"spark.version {spark.version} != client {pyspark.__version__}"
    )
    if engine == "sail":
        import pysail

        assert engine_version == pysail.__version__, f"version() returned {engine_version!r}"
    else:
        assert engine_version.startswith(pyspark.__version__), f"Spark version() returned {engine_version!r}"
    return {"version()": engine_version, "spark.version": spark.version}
