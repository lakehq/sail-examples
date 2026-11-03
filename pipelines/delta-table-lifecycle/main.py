# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""The life of a Delta Lake table: append, evolve the schema, update, delete, merge, rebuild, time travel.

The table holds daily taxi trips per pickup borough, built from real NYC green taxi records for
January to March 2024. The example reads each of the eight table versions it creates.
"""

import atexit
import hashlib
import os
import shutil
import tempfile
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

DATA = Path(__file__).resolve().parent / ".data"
SOURCE = "https://d37ci6vzurychx.cloudfront.net"
FILES = {
    # name: (url, SHA-256 of the file this example was written against)
    "green_tripdata_2024-01.parquet": "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    "green_tripdata_2024-02.parquet": "cb7eb405ee7e795c9fd6b8ad1b291f46f4e0a1cd60a57067b0aaca2420e8bc5d",
    "green_tripdata_2024-03.parquet": "43dedd66dd556acfc38da679a174b6adc32e01dd6dccad14bdfdf1521a14abed",
    "taxi_zone_lookup.csv": "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
}


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


def daily_by_borough(spark: SparkSession, month: int, with_tips: bool) -> DataFrame:
    """One month of trips summarized per day and pickup borough."""
    trips = spark.read.parquet(str(download(f"green_tripdata_2024-{month:02d}.parquet")))
    trips.createOrReplaceTempView("month_trips")
    tips = ", round(sum(t.tip_amount), 2) AS tips" if with_tips else ""
    return spark.sql(f"""
        SELECT {month} AS month, date(t.lpep_pickup_datetime) AS day, z.Borough AS borough,
               count(*) AS trips, round(sum(t.fare_amount), 2) AS fares{tips}
        FROM month_trips t JOIN zones z ON t.PULocationID = z.LocationID
        WHERE month(t.lpep_pickup_datetime) = {month} AND year(t.lpep_pickup_datetime) = 2024
        GROUP BY 2, 3
    """)


def run(spark: SparkSession) -> dict[str, DataFrame]:
    spark.read.schema("LocationID int, Borough string, Zone string, service_zone string").csv(
        str(download("taxi_zone_lookup.csv")), header=True
    ).createOrReplaceTempView("zones")
    path = temp_dir("delta-") + "/daily"

    # Version 0: declare the table: its columns, their types, and the partition column.
    spark.sql(f"""
        CREATE TABLE daily_trips (month INT, day DATE, borough STRING, trips BIGINT, fares DOUBLE)
        USING delta PARTITIONED BY (month) LOCATION '{path}'
    """)
    # Versions 1 and 2: append January, then February.
    for month in (1, 2):
        daily_by_borough(spark, month, with_tips=False).write.format("delta").mode("append").save(path)
    # Version 3: March arrives with a new column. mergeSchema adds it; older rows read it as NULL.
    march = daily_by_borough(spark, 3, with_tips=True)
    march.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
    # Version 4: rebuild January with the tips column, replacing only that month's rows.
    january = daily_by_borough(spark, 1, with_tips=True)
    january.write.format("delta").mode("overwrite").option("replaceWhere", "month = 1").save(path)

    # Version 5: relabel a value in place.
    spark.sql("UPDATE daily_trips SET borough = 'Outside NYC' WHERE borough = 'N/A'")
    # Version 6: remove rows that should not be reported.
    spark.sql("DELETE FROM daily_trips WHERE borough = 'Unknown'")
    # Version 7: late corrections for one day: update the matching row, insert the missing one.
    corrections = spark.createDataFrame(
        [
            (3, "2024-03-31", "Manhattan", 1000, 15000.0, 1500.0),
            (3, "2024-03-31", "Staten Island", 1, 20.0, 0.0),
        ],
        "month int, day string, borough string, trips long, fares double, tips double",
    ).selectExpr("month", "CAST(day AS DATE) AS day", "borough", "trips", "fares", "tips")
    corrections.createOrReplaceTempView("corrections")
    spark.sql("""
        MERGE INTO daily_trips t USING corrections c
        ON t.day = c.day AND t.borough = c.borough
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # Time travel: every version is still readable.
    versions = spark.createDataFrame(
        [(v, spark.read.format("delta").option("versionAsOf", v).load(path).count()) for v in range(8)],
        "version int, rows long",
    )
    current = spark.read.format("delta").load(path)
    summary = (
        current.groupBy("month")
        .agg({"trips": "sum", "tips": "count"})
        .withColumnsRenamed({"sum(trips)": "trips", "count(tips)": "rows_with_tips"})
    )
    march_31 = spark.sql(f"""
        SELECT 'version 3' AS version, borough, trips, fares FROM delta.`{path}` VERSION AS OF 3
        WHERE day = DATE'2024-03-31'
        UNION ALL
        SELECT 'current', borough, trips, fares FROM daily_trips WHERE day = DATE'2024-03-31'
    """)
    return {
        "versions": versions.orderBy("version"),
        "by_month": summary.orderBy("month"),
        "march_31": march_31.orderBy("version", "borough"),
        "boroughs": current.select("borough").distinct().orderBy("borough"),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    for name, frame in results.items():
        print(name)
        frame.show(truncate=False)
    spark.stop()
    server.stop()
