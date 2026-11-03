# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Read a paginated REST API as a table with a Python data source.

`TicketsDataSource` uses the Spark Python Data Source API: `partitions()` asks the API how many
pages there are, and `read()` fetches one page per partition, so pages load in parallel. A
small local service stands in for the API so the example runs offline.
"""

import atexit
import json
import shutil
import tempfile
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

PAGE_SIZE = 50
TOTAL_TICKETS = 487
STATUSES = ["open", "pending", "solved", "closed"]


def ticket(i: int) -> dict:
    return {
        "id": i,
        "subject": f"Ticket {i}",
        "status": STATUSES[(i * 7) % len(STATUSES)],
        "priority": 1 + (i * 13) % 4,
    }


class StandInApi:
    """GET /tickets?page=N returns one page of tickets and the page count."""

    def __init__(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                page = int(parse_qs(urlparse(self.path).query).get("page", ["1"])[0])
                start = (page - 1) * PAGE_SIZE
                items = [ticket(i) for i in range(start, min(start + PAGE_SIZE, TOTAL_TICKETS))]
                pages = -(-TOTAL_TICKETS // PAGE_SIZE)
                body = json.dumps({"page": page, "pages": pages, "items": items}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/tickets"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def get_page(url: str, page: int) -> dict:
    with urllib.request.urlopen(f"{url}?page={page}", timeout=30) as response:
        return json.load(response)


class PagePartition(InputPartition):
    def __init__(self, page: int):
        super().__init__(page)
        self.page = page


class TicketsReader(DataSourceReader):
    def __init__(self, url: str):
        self.url = url

    def partitions(self):
        pages = get_page(self.url, 1)["pages"]
        return [PagePartition(page) for page in range(1, pages + 1)]

    def read(self, partition):
        for item in get_page(self.url, partition.page)["items"]:
            yield (item["id"], item["subject"], item["status"], item["priority"])


class TicketsDataSource(DataSource):
    @classmethod
    def name(cls):
        return "tickets_api"

    def schema(self):
        return "id long, subject string, status string, priority int"

    def reader(self, schema):
        return TicketsReader(self.options["url"])


def run(spark: SparkSession) -> dict[str, DataFrame]:
    api = StandInApi()
    try:
        spark.dataSource.register(TicketsDataSource)
        tickets = spark.read.format("tickets_api").option("url", api.url).load()
        # Read the API once: save the rows, then run every query on the saved copy.
        saved_path = temp_dir("tickets-")
        tickets.write.mode("overwrite").parquet(saved_path)
    finally:
        api.stop()

    saved = spark.read.parquet(saved_path)
    by_status = saved.groupBy("status").agg(
        F.count("*").alias("tickets"), F.round(F.avg("priority"), 3).alias("avg_priority")
    )
    totals = saved.agg(F.count("*").alias("tickets"), F.countDistinct("id").alias("distinct_ids"))
    return {
        "by_status": by_status.orderBy("status"),
        "first_tickets": saved.orderBy("id").limit(5),
        "totals": totals,
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["totals"].show()
    results["by_status"].show()
    spark.stop()
    server.stop()
