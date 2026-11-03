# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Query Parquet and CSV files with SQL: no tables to load, no cluster, no Java.

The data is real: every NYC green taxi trip from January 2024 (one Parquet file) and the taxi
zone lookup table (one CSV file), both published by the NYC Taxi and Limousine Commission.
"""

import hashlib
import os
import tempfile
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

DATA = Path(__file__).resolve().parent / ".data"
SOURCE = "https://d37ci6vzurychx.cloudfront.net"
FILES = {
    # name: (url, SHA-256 of the file this example was written against)
    "green_tripdata_2024-01.parquet": (
        f"{SOURCE}/trip-data/green_tripdata_2024-01.parquet",
        "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    ),
    "taxi_zone_lookup.csv": (
        f"{SOURCE}/misc/taxi_zone_lookup.csv",
        "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
    ),
}


def download(name: str) -> Path:
    """Reuse or download a file only when its SHA-256 matches the pinned input."""
    url, expected = FILES[name]
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


def run(spark: SparkSession) -> dict[str, DataFrame]:
    trips_file, zones_file = download("green_tripdata_2024-01.parquet"), download("taxi_zone_lookup.csv")

    # 1. Query a file directly: name the format and the path in the FROM clause.
    overview = spark.sql(f"""
        SELECT count(*) AS trips,
               min(lpep_pickup_datetime) AS first_pickup,
               max(lpep_pickup_datetime) AS last_pickup,
               median(trip_distance) AS median_miles,
               max(trip_distance) AS max_miles
        FROM parquet.`{trips_file}`
    """)

    # 2. Give files a name with temporary views. The CSV view declares its column types.
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW trips USING parquet OPTIONS (path '{trips_file}')")
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW zones
            (LocationID INT, Borough STRING, Zone STRING, service_zone STRING)
        USING csv OPTIONS (path '{zones_file}', header 'true')
    """)
    schema = spark.sql("DESCRIBE TABLE trips")

    # 3. Join, group and sort, as in any SQL database.
    top_zones = spark.sql("""
        SELECT z.Zone AS pickup_zone, z.Borough AS borough, count(*) AS trips
        FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Zone, z.Borough
        ORDER BY trips DESC, pickup_zone
        LIMIT 10
    """)
    by_hour = spark.sql("""
        SELECT hour(lpep_pickup_datetime) AS hour, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
        FROM trips
        GROUP BY 1
        ORDER BY 1
    """)

    # 4. Decode the payment codes (from the TLC data dictionary) and compare tips. The data records
    #    card tips only, so the tip share is the average tip as a fraction of the fare.
    payments = spark.sql("""
        SELECT CASE payment_type
                   WHEN 0 THEN 'Flex Fare trip' WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
                   WHEN 3 THEN 'No charge' WHEN 4 THEN 'Dispute' WHEN 5 THEN 'Unknown'
                   WHEN 6 THEN 'Voided trip' ELSE 'Not recorded'
               END AS payment,
               count(*) AS trips,
               round(avg(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END), 3) AS tip_share
        FROM trips
        GROUP BY 1
        ORDER BY trips DESC, payment
    """)
    return {
        "overview": overview,
        "schema": schema,
        "top_zones": top_zones,
        "by_hour": by_hour,
        "payments": payments,
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    for name in ("overview", "top_zones", "payments"):
        results[name].show(truncate=False)
    spark.stop()
    server.stop()
