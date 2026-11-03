# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Route support tickets with a model from plain SQL, using Sail's built-in Jev functions.

`jev_choice` asks a TypeSafe Jev model to pick one of several labels for each row. The query
runs on distinct texts only, saves the answers, and joins them back. By default the example
runs against the local TypeSafe stand-in that ships in pysail for Sail's own tests, so it needs
no account and sends nothing off the machine. Set JEV_LIVE=1 and TYPESAFE_API_KEY to call the
real service instead (row values are then sent to TypeSafe).
"""

import atexit
import contextlib
import os
import shutil
import tempfile

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession

TEMPLATES = [
    "I was charged twice for {p}, please refund me",
    "{p} crashes with an error when I open a report",
    "How do I export data from {p}?",
    "The {p} invoice amount is wrong",
    "{p} shows error 500 after the update",
    "How to invite a teammate to {p}",
]
PRODUCTS = ["Starter plan", "Team plan", "Business plan", "Enterprise plan"]
ROWS = 2_000
TEAMS = {
    "billing": "Payments, charges, refunds and invoices",
    "bug": "Crashes, errors and broken features",
    "how-to": "Questions about how to use the product",
}


def sql_string(value: str) -> str:
    """A SQL string literal, with single quotes escaped."""
    return "'" + value.replace("'", "''") + "'"


def ticket_text(i: int) -> str:
    return TEMPLATES[i % len(TEMPLATES)].replace("{p}", PRODUCTS[(i // len(TEMPLATES)) % len(PRODUCTS)])


def stand_in_route(text: str) -> str:
    """The stand-in model's rule. The real Jev model reads the descriptions in TEAMS instead."""
    lowered = text.lower()
    if "refund" in lowered or "charged" in lowered or "invoice" in lowered:
        return "billing"
    if "crash" in lowered or "error" in lowered:
        return "bug"
    return "how-to"


def answer_with_rule(response: dict, body: dict) -> dict:
    """Shape the stand-in's answer: choose a team by rule, with a fixed confidence."""
    route = stand_in_route(str(body["state"]))
    for answer in response["answers"].values():
        if answer["type"] == "choice":
            names = list(answer["probabilities"])
            answer["choice"] = route
            answer["confidence"] = 0.9
            answer["probabilities"] = {n: 0.9 if n == route else 0.1 / (len(names) - 1) for n in names}
    return response


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


@contextlib.contextmanager
def jev_service():
    """Yield a request counter: the local stand-in by default, the real service with JEV_LIVE=1."""
    if os.environ.get("JEV_LIVE") == "1":
        yield None
        return
    # JevMock is the TypeSafe stand-in that pysail ships for Sail's own tests, not a stable API.
    from pysail.testing.jev import JevMock

    saved = {k: os.environ.get(k) for k in ("TYPESAFE_BASE_URL", "TYPESAFE_API_KEY")}
    with JevMock() as mock:
        mock.transform = answer_with_rule
        # Set before the first Jev query, so the in-process Sail server sends requests to the stand-in.
        os.environ["TYPESAFE_BASE_URL"] = mock.url
        os.environ["TYPESAFE_API_KEY"] = "local-stand-in"
        try:
            yield mock
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def run(spark: SparkSession) -> dict[str, DataFrame]:
    spark.createDataFrame(
        [(i, ticket_text(i)) for i in range(ROWS)], "ticket_id long, text string"
    ).createOrReplaceTempView("tickets")
    criteria = ", ".join(
        f"{sql_string(name)}, {sql_string(description)}" for name, description in TEAMS.items()
    )

    with jev_service() as service:
        answers = spark.sql(
            f"""
            SELECT text, answer.choice AS team, answer.confidence AS confidence
            FROM (
              SELECT text, jev_choice(text, 'Which team should handle this support ticket?', map({criteria})) AS answer
              FROM (SELECT DISTINCT text FROM tickets)
            )
            """
        )
        # Save the answers once: every later query reads them instead of calling the model again.
        answers_path = temp_dir("routes-")
        answers.write.mode("overwrite").parquet(answers_path)
        requests = service.request_count if service is not None else None

    spark.read.parquet(answers_path).createOrReplaceTempView("routes")
    summary = spark.sql(
        "SELECT team, count(*) AS tickets FROM tickets JOIN routes USING (text) GROUP BY team ORDER BY team"
    )
    stats = spark.createDataFrame(
        [(ROWS, spark.table("routes").count(), requests)], "tickets long, distinct_texts long, requests long"
    )
    return {"routes": spark.sql("SELECT * FROM routes ORDER BY text"), "summary": summary, "stats": stats}


def reference() -> dict[str, pa.Table]:
    """The expected results, computed without Sail from the same data and the stand-in's rule."""
    texts = sorted({ticket_text(i) for i in range(ROWS)})
    routes = [{"text": t, "team": stand_in_route(t), "confidence": 0.9} for t in texts]
    counts: dict[str, int] = {}
    for i in range(ROWS):
        team = stand_in_route(ticket_text(i))
        counts[team] = counts.get(team, 0) + 1
    return {
        "routes": pa.Table.from_pylist(
            routes, pa.schema([("text", pa.string()), ("team", pa.string()), ("confidence", pa.float64())])
        ),
        "summary": pa.Table.from_pylist(
            [{"team": k, "tickets": v} for k, v in sorted(counts.items())],
            pa.schema([("team", pa.string()), ("tickets", pa.int64())]),
        ),
        "stats": pa.Table.from_pylist(
            [{"tickets": ROWS, "distinct_texts": len(texts), "requests": len(texts)}],
            pa.schema([("tickets", pa.int64()), ("distinct_texts", pa.int64()), ("requests", pa.int64())]),
        ),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["summary"].show()
    results["stats"].show()
    spark.stop()
    server.stop()
