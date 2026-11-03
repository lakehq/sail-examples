# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Label every row of a table with a model while calling the model once per distinct value.

The pattern: reduce to distinct inputs, ask the model concurrently inside `mapInArrow`,
save the answers, then join them back onto the full table. A small local service stands in
for the model so the example runs offline; replace `ask_model` with your model client.
"""

import atexit
import json
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TEMPLATES = [
    "I was charged twice for {p}, please refund me",
    "{p} crashes with an error when I open a report",
    "How do I export data from {p}?",
    "Can I get a refund for my {p} subscription?",
    "{p} shows error 500 after the update",
    "How to invite a teammate to {p}",
    "The {p} invoice amount is wrong",
    "{p} is slow but works",
]
PRODUCTS = [
    "Starter plan",
    "Team plan",
    "Business plan",
    "Enterprise plan",
    "Education plan",
    "Nonprofit plan",
]
# At most ASK_TASKS * REQUESTS_PER_TASK requests are in flight at once. Size the product to
# the model API's concurrency or rate limit.
ASK_TASKS = 4
REQUESTS_PER_TASK = 8
ATTEMPTS = 4


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def label_for(text: str) -> str:
    """The stand-in model's rule. A real model would be called instead."""
    lowered = text.lower()
    if "refund" in lowered or "charged" in lowered or "invoice" in lowered:
        return "billing"
    if "crash" in lowered or "error" in lowered:
        return "bug"
    if lowered.startswith("how"):
        return "how-to"
    return "other"


class StandInModel:
    """A local HTTP service that labels text after a short delay, and counts its calls."""

    def __init__(self, latency_s: float = 0.02):
        self.calls = 0
        lock = threading.Lock()
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                text = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["text"]
                time.sleep(latency_s)
                with lock:
                    service.calls += 1
                body = json.dumps({"label": label_for(text)}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/label"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


def make_ask(url: str):
    """Build the mapInArrow function; `url` is captured so the function can run anywhere."""

    def ask_model(text: str) -> str:
        request = urllib.request.Request(
            url, data=json.dumps({"text": text}).encode(), headers={"Content-Type": "application/json"}
        )
        for attempt in range(ATTEMPTS):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)["label"]
            except urllib.error.HTTPError as error:
                # Retry rate limits and server errors with backoff; anything else is a real error.
                if (error.code != 429 and error.code < 500) or attempt == ATTEMPTS - 1:
                    raise
            except urllib.error.URLError:
                if attempt == ATTEMPTS - 1:
                    raise
            time.sleep(0.5 * 2**attempt)
        raise RuntimeError("unreachable")

    def ask(batches):
        with ThreadPoolExecutor(max_workers=REQUESTS_PER_TASK) as pool:
            for batch in batches:
                texts = batch.column("text").to_pylist()
                labels = list(pool.map(ask_model, texts))
                yield pa.RecordBatch.from_pydict({"text": texts, "label": labels})

    return ask


def run(spark: SparkSession) -> dict[str, DataFrame]:
    templates = F.array(*[F.lit(t) for t in TEMPLATES])
    products = F.array(*[F.lit(p) for p in PRODUCTS])
    tickets = spark.range(20_000).select(
        F.col("id").alias("ticket_id"),
        F.replace(
            F.element_at(templates, (F.col("id") % len(TEMPLATES) + 1).cast("int")),
            F.lit("{p}"),
            # The product index does not share a cycle with the template index: 8 x 6 = 48 texts.
            F.element_at(
                products, ((F.col("id") / len(TEMPLATES)).cast("long") % len(PRODUCTS) + 1).cast("int")
            ),
        ).alias("text"),
    )

    model = StandInModel()
    try:
        # 1. Reduce: the model sees each distinct text once, not every row.
        distinct_texts = tickets.select("text").distinct().repartition(ASK_TASKS)
        # 2. Ask: ASK_TASKS tasks, each with REQUESTS_PER_TASK requests in flight.
        answers = distinct_texts.mapInArrow(make_ask(model.url), "text string, label string")
        # 3. Save: write the answers once, so later steps read them instead of calling the model.
        answers_path = temp_dir("answers-")
        answers.write.mode("overwrite").parquet(answers_path)
        saved = spark.read.parquet(answers_path)
        # 4. Integrate: join the answers back onto every ticket.
        labeled = tickets.join(saved, "text")
        summary_rows = labeled.groupBy("label").agg(F.count("*").alias("tickets")).orderBy("label").collect()
        calls = model.calls
    finally:
        model.stop()

    stats = spark.createDataFrame(
        [(tickets.count(), saved.count(), calls)], "tickets long, distinct_texts long, model_calls long"
    )
    return {
        "summary": spark.createDataFrame(summary_rows, "label string, tickets long"),
        "labels": saved.orderBy("text"),
        "stats": stats,
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
