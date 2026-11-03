FACT = {
    "id": "udfs-need-a-session",
    "category": "python-udfs",
    "text": """Create UDFs after the Spark session exists. With PySpark 4 clients, `@udf`,
        `@pandas_udf` and `@arrow_udf` applied at import time, before any session, fail with
        `NO_ACTIVE_OR_DEFAULT_SESSION` (or `SESSION_OR_CONTEXT_NOT_EXISTS`). Keep plain functions at
        module level and wrap them, for example `F.udf(fn, "double")`, once the session is up.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

import subprocess
import sys

PROBE = """
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
def f(a: pd.Series) -> pd.Series:
    return a
for make in (lambda: F.udf("double")(lambda x: x), lambda: pandas_udf("double")(f)):
    try:
        make()
        print("CREATED")
    except Exception as error:
        print("FAILED", " ".join(str(error).split())[:120])
"""


def check(spark, engine):
    # A fresh interpreter in this engine's environment, with no session in it.
    probe = [sys.executable, "-c", PROBE]
    out = subprocess.run(probe, capture_output=True, text=True, timeout=120, check=False).stdout
    lines = [line for line in out.splitlines() if line.startswith(("CREATED", "FAILED"))]
    assert len(lines) == 2, f"probe did not report: {out[-300:]}"
    assert all(line.startswith("FAILED") for line in lines), f"a UDF was created without a session: {lines}"
    assert all("SESSION" in line for line in lines), f"failed for another reason: {lines}"
    return {"without_session": [line[:80] for line in lines]}
