FACT = {
    "id": "udfs-share-one-interpreter",
    "category": "python-udfs",
    "core": True,
    "text": """In local mode (the default), Python UDFs run inside the Sail server process, in one Python
        interpreter shared by all tasks, while Apache Spark runs them in separate Python worker processes.
        Prefer built-in functions, and for CPU-heavy Python work prefer libraries that release the GIL
        (NumPy, PyArrow compute, ONNX Runtime, tokenizers) so tasks can overlap.""",
    "engines": ["sail", "spark"],
    "order": 1,
}

import os

from pyspark.sql import functions as F


def check(spark, engine):
    @F.udf("long")
    def pid(_):
        import os

        return os.getpid()

    pids = {r.pid for r in spark.range(0, 4096, 1, 32).select(pid("id").alias("pid")).distinct().collect()}
    if engine == "sail":
        # The harness starts Sail in-process, so the server process is this process.
        assert pids == {os.getpid()}, f"expected UDFs in the server process {os.getpid()}, saw {pids}"
    else:
        assert os.getpid() not in pids, "Spark ran a Python UDF in the client process"
    return {"distinct_udf_pids": len(pids), "in_server_process": pids == {os.getpid()}}
