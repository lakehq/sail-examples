# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""An Apache Iceberg table on Sail: declare, append, update, delete, merge, and read old snapshots.

The table holds daily taxi trips and fares per pickup borough for January and February 2024,
built from real NYC green taxi records. Every change creates an Iceberg snapshot, and each
snapshot stays readable.
"""

import atexit
import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA = Path(__file__).resolve().parent / ".data"
SOURCE = "https://d37ci6vzurychx.cloudfront.net"
FILES = {
    # name: SHA-256 of the file this example was written against
    "green_tripdata_2024-01.parquet": "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    "green_tripdata_2024-02.parquet": "cb7eb405ee7e795c9fd6b8ad1b291f46f4e0a1cd60a57067b0aaca2420e8bc5d",
    "taxi_zone_lookup.csv": "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
}
# A late correction for one day and borough: (day, borough, trips, fares).
CORRECTION = ("2024-02-29", "Manhattan", 1500, 20000.0)


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def download(name: str) -> Path:
    """Reuse or download a file only when its SHA-256 matches the pinned input."""
    expected = FILES[name]
    folder = "misc" if name.endswith(".csv") else "trip-data"
    url = f"{SOURCE}/{folder}/{name}"
    path = DATA / name
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with (
                urllib.request.urlopen(url, timeout=120) as response,
                tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as out,
            ):
                temporary = Path(out.name)
                while chunk := response.read(1024 * 1024):
                    out.write(chunk)
            with temporary.open("rb") as downloaded:
                if hashlib.file_digest(downloaded, "sha256").hexdigest() != expected:
                    raise ValueError(f"SHA-256 mismatch: {name}")
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    with path.open("rb") as cached:
        if hashlib.file_digest(cached, "sha256").hexdigest() != expected:
            raise ValueError(f"SHA-256 mismatch: {name}")
    return path


def snapshots(table_path: str) -> list[dict]:
    """The table's snapshots, oldest first, from its newest metadata file."""
    metadata = Path(table_path) / "metadata"
    newest = max(metadata.glob("*.metadata.json"), key=lambda p: int(p.name.split(".")[0].lstrip("v")))
    return json.loads(newest.read_text())["snapshots"]


def run(spark: SparkSession) -> dict[str, DataFrame]:
    spark.read.schema("LocationID int, Borough string, Zone string, service_zone string").csv(
        str(download("taxi_zone_lookup.csv")), header=True
    ).createOrReplaceTempView("zones")
    path = temp_dir("iceberg-") + "/daily_trips"

    spark.sql(f"""
        CREATE TABLE daily_trips (month INT, day DATE, borough STRING, trips BIGINT, fares DOUBLE)
        USING iceberg PARTITIONED BY (month) LOCATION '{path}'
    """)
    # Snapshots 1 and 2: insert January, then February.
    for month in (1, 2):
        spark.read.parquet(str(download(f"green_tripdata_2024-{month:02d}.parquet"))).createOrReplaceTempView(
            "raw"
        )
        spark.sql(f"""
            INSERT INTO daily_trips
            SELECT {month}, date(t.lpep_pickup_datetime), z.Borough, count(*), sum(t.fare_amount)
            FROM raw t JOIN zones z ON t.PULocationID = z.LocationID
            WHERE year(t.lpep_pickup_datetime) = 2024 AND month(t.lpep_pickup_datetime) = {month}
            GROUP BY 2, 3
        """)
    # Snapshot 3: relabel a value. Snapshot 4: remove rows. Snapshot 5: merge a late correction.
    spark.sql("UPDATE daily_trips SET borough = 'Outside NYC' WHERE borough = 'N/A'")
    spark.sql("DELETE FROM daily_trips WHERE borough = 'Unknown'")
    day, borough, trips, fares = CORRECTION
    spark.sql(f"""
        MERGE INTO daily_trips t
        USING (SELECT DATE'{day}' AS day, '{borough}' AS borough, {trips}L AS trips, {fares}D AS fares) c
        ON t.day = c.day AND t.borough = c.borough
        WHEN MATCHED THEN UPDATE SET trips = c.trips, fares = c.fares
        WHEN NOT MATCHED THEN INSERT (month, day, borough, trips, fares)
            VALUES (month(c.day), c.day, c.borough, c.trips, c.fares)
    """)

    # Time travel: read every snapshot by id, and compare with the row count Iceberg recorded for it.
    history = [
        (
            number,
            spark.read.format("iceberg").option("snapshot-id", str(s["snapshot-id"])).load(path).count(),
            int(s["summary"]["total-records"]),
        )
        for number, s in enumerate(snapshots(path), start=1)
    ]
    current = spark.table("daily_trips")
    return {
        "snapshots": spark.createDataFrame(history, "snapshot int, rows_read long, rows_recorded long"),
        "by_borough": current.groupBy("borough")
        .agg({"trips": "sum", "day": "count"})
        .withColumnsRenamed({"sum(trips)": "trips", "count(day)": "days"})
        .orderBy("borough"),
        "february_29": current.where(f"day = DATE'{day}'")
        .select("month", "borough", "trips", "fares")
        .orderBy("borough"),
    }


def reference() -> dict[str, pa.Table]:
    """The same results computed with pyarrow alone, from the same files."""
    import pyarrow.compute as pc
    import pyarrow.csv as pv
    import pyarrow.parquet as pq

    zones = pv.read_csv(download("taxi_zone_lookup.csv")).select(["LocationID", "Borough"])
    zones = zones.cast(pa.schema([("LocationID", pa.int32()), ("Borough", pa.string())]))
    rows = {}
    for month in (1, 2):
        raw = pq.read_table(
            download(f"green_tripdata_2024-{month:02d}.parquet"),
            columns=["lpep_pickup_datetime", "PULocationID", "fare_amount"],
        )
        ts = raw["lpep_pickup_datetime"]
        raw = raw.filter(pc.and_(pc.equal(pc.year(ts), 2024), pc.equal(pc.month(ts), month)))
        raw = raw.append_column("day", pc.cast(raw["lpep_pickup_datetime"], pa.date32()))
        joined = raw.join(zones, keys="PULocationID", right_keys="LocationID", join_type="inner")
        grouped = joined.group_by(["day", "Borough"]).aggregate([([], "count_all"), ("fare_amount", "sum")])
        for day, borough, trips, fares in zip(
            *(grouped[c].to_pylist() for c in ("day", "Borough", "count_all", "fare_amount_sum"))
        ):
            rows[(day, borough)] = [month, trips, fares]
    counts = [sum(1 for v in rows.values() if v[0] == 1), len(rows)]
    rows = {(d, "Outside NYC" if b == "N/A" else b): v for (d, b), v in rows.items()}
    counts.append(len(rows))
    rows = {k: v for k, v in rows.items() if k[1] != "Unknown"}
    counts.append(len(rows))
    import datetime as dt

    day, borough, trips, fares = CORRECTION
    key = (dt.date.fromisoformat(day), borough)
    rows[key] = [key[0].month, trips, fares]
    counts.append(len(rows))

    by_borough: dict[str, list[int]] = {}
    for (_, b), (_, t, _) in rows.items():
        by_borough.setdefault(b, [0, 0])
        by_borough[b][0] += t
        by_borough[b][1] += 1
    feb29 = sorted((b, m, t, f) for (d, b), (m, t, f) in rows.items() if d == key[0])
    return {
        "snapshots": pa.table(
            {
                "snapshot": pa.array(range(1, 6), pa.int32()),
                "rows_read": pa.array(counts, pa.int64()),
                "rows_recorded": pa.array(counts, pa.int64()),
            }
        ),
        "by_borough": pa.table(
            {
                "borough": sorted(by_borough),
                "trips": pa.array([by_borough[b][0] for b in sorted(by_borough)], pa.int64()),
                "days": pa.array([by_borough[b][1] for b in sorted(by_borough)], pa.int64()),
            }
        ),
        "february_29": pa.table(
            {
                "month": pa.array([r[1] for r in feb29], pa.int32()),
                "borough": [r[0] for r in feb29],
                "trips": pa.array([r[2] for r in feb29], pa.int64()),
                "fares": pa.array([r[3] for r in feb29], pa.float64()),
            }
        ),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["february_29"] = results["february_29"].withColumn("fares", F.round("fares", 2))
    for name, frame in results.items():
        print(name)
        frame.show(truncate=False)
    spark.stop()
    server.stop()
