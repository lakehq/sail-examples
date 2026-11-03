FACT = {
    "id": "python-data-source-partitions-concurrent",
    "category": "data-sources",
    "text": """A Python data source's partitions are read as separate tasks, so `read()` calls for
        different partitions run at the same time. Return one partition per page, file or key range to
        fetch an API or a set of files concurrently.""",
    "engines": ["sail", "spark"],
    "order": 2,
}

import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

PARTITIONS = 8


class SlowService:
    """Answers every request after 200 ms and records the most requests in flight at once."""

    def __init__(self):
        self.active = self.peak = 0
        lock = threading.Lock()
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                with lock:
                    service.active += 1
                    service.peak = max(service.peak, service.active)
                time.sleep(0.2)
                with lock:
                    service.active -= 1
                body = json.dumps({"value": 1}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


class SlowReader(DataSourceReader):
    def __init__(self, url):
        self.url = url

    def partitions(self):
        return [InputPartition(i) for i in range(PARTITIONS)]

    def read(self, partition):
        with urllib.request.urlopen(self.url, timeout=30) as response:
            yield (partition.value, json.load(response)["value"])


class SlowSource(DataSource):
    @classmethod
    def name(cls):
        return "fact_slow_source"

    def schema(self):
        return "part int, value int"

    def reader(self, schema):
        return SlowReader(self.options["url"])


def check(spark, engine):
    service = SlowService()
    try:
        spark.dataSource.register(SlowSource)
        rows = spark.read.format("fact_slow_source").option("url", service.url).load().collect()
    finally:
        service.server.shutdown()
        service.server.server_close()
    assert len(rows) == PARTITIONS, f"expected {PARTITIONS} rows, got {len(rows)}"
    assert service.peak > 1, f"partitions were read one at a time (peak {service.peak})"
    return {"partitions": PARTITIONS, "peak_concurrent_reads": service.peak}
