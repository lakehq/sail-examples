# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Explore a year of real taxi trips with SQL: clean, trend, compare and rank.

The data is every NYC green taxi trip record from 2024 (twelve monthly Parquet files) and the
taxi zone lookup, published by the NYC Taxi and Limousine Commission. The questions are the
ones an analyst asks first; each answer is one SQL query.
"""

import hashlib
import os
import tempfile
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

DATA = Path(__file__).resolve().parent / ".data"
SOURCE = "https://d37ci6vzurychx.cloudfront.net"
# SHA-256 of each file this example was written against.
MONTHS = {
    "01": "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    "02": "cb7eb405ee7e795c9fd6b8ad1b291f46f4e0a1cd60a57067b0aaca2420e8bc5d",
    "03": "43dedd66dd556acfc38da679a174b6adc32e01dd6dccad14bdfdf1521a14abed",
    "04": "af174977258a02f8f44c37534abc9a8b4eaa5ec6d2f9c2bc49782327628b608c",
    "05": "aa8b1965ed36f0bda1b342930d5e6f5a89f8fe56d0c6c6d77d9c156b408aa0ea",
    "06": "47e7c37dd39069c474364aee3d76c9ddc887a3f714e9bb550b5866fe0b89b9cd",
    "07": "ce0d7591316050bea21da49e383eb733411e98ae17e114afcc7fbe4b36b116f2",
    "08": "733210fb27cbe8cf8b0ad9f5d6fff03dfafcc8ff7c2beb1c1b707825d72f0376",
    "09": "8f6aa8f3e8a2040fc161c7fbcb60e5418238b27e7e95998363572a4c977b69ce",
    "10": "6e737b7c4edac91f865aa210f7110d0118a9992b3a96720936e090277c24edaa",
    "11": "fde255b43d4158124921b7c085d9abf15de5c5a9128c2ce0fb4e2372bc1e0d4a",
    "12": "1d708f58d29b0795a54ca11a36db6df61262986d122985f0fb52a56acc064bac",
}
FILES = {
    f"trips/green_tripdata_2024-{m}.parquet": (f"{SOURCE}/trip-data/green_tripdata_2024-{m}.parquet", sha)
    for m, sha in MONTHS.items()
}
FILES["taxi_zone_lookup.csv"] = (
    f"{SOURCE}/misc/taxi_zone_lookup.csv",
    "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
)


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
    for name in FILES:
        download(name)

    # A folder of Parquet files reads as one table.
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW raw_trips USING parquet OPTIONS (path '{DATA / 'trips'}')")
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW zones
            (LocationID INT, Borough STRING, Zone STRING, service_zone STRING)
        USING csv OPTIONS (path '{DATA / "taxi_zone_lookup.csv"}', header 'true')
    """)

    # 1. Keep plausible trips: picked up in 2024, 1 to 180 minutes, up to 100 miles, no negative fare.
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW trips AS
        SELECT *, timestampdiff(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) AS minutes
        FROM raw_trips
        WHERE year(lpep_pickup_datetime) = 2024
          AND timestampdiff(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) BETWEEN 1 AND 180
          AND trip_distance BETWEEN 0 AND 100
          AND fare_amount >= 0
    """)
    cleaning = spark.sql("""
        SELECT (SELECT count(*) FROM raw_trips) AS records,
               (SELECT count(*) FROM trips) AS kept,
               (SELECT count(*) FROM raw_trips) - (SELECT count(*) FROM trips) AS dropped
    """)

    # 2. Trend: trips and fares per month, with the change from the month before.
    monthly = spark.sql("""
        SELECT month, trips, fares,
               round((trips - lag(trips) OVER (ORDER BY month)) / lag(trips) OVER (ORDER BY month), 4)
                   AS change_from_previous_month
        FROM (SELECT month(lpep_pickup_datetime) AS month, count(*) AS trips, round(sum(fare_amount), 2) AS fares
              FROM trips GROUP BY 1)
        ORDER BY month
    """)

    # 3. When: the five busiest hours of the week. dayofweek() numbers Sunday 1 to Saturday 7.
    busiest_hours = spark.sql("""
        SELECT date_format(lpep_pickup_datetime, 'EEEE') AS day, dayofweek(lpep_pickup_datetime) AS day_number,
               hour(lpep_pickup_datetime) AS hour, count(*) AS trips
        FROM trips
        GROUP BY 1, 2, 3
        ORDER BY trips DESC, day_number, hour
        LIMIT 5
    """)

    # 4. Where: exact medians and percentiles per pickup borough.
    boroughs = spark.sql("""
        SELECT z.Borough AS borough, count(*) AS trips,
               median(t.fare_amount) AS median_fare,
               round(percentile(t.trip_distance, 0.9), 2) AS p90_miles,
               round(avg(CASE WHEN t.payment_type = 1 THEN 1.0D ELSE 0.0D END), 4) AS card_share
        FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Borough
        ORDER BY trips DESC, borough
    """)

    # 5. Rank: the top two pickup zones within each borough, with a window function.
    top_zones = spark.sql("""
        SELECT borough, zone, trips, rank
        FROM (SELECT z.Borough AS borough, z.Zone AS zone, count(*) AS trips,
                     rank() OVER (PARTITION BY z.Borough ORDER BY count(*) DESC) AS rank
              FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
              GROUP BY z.Borough, z.Zone)
        WHERE rank <= 2
        ORDER BY borough, rank, zone
    """)
    return {
        "cleaning": cleaning,
        "monthly": monthly,
        "busiest_hours": busiest_hours,
        "boroughs": boroughs,
        "top_zones": top_zones,
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
        frame.show(12, truncate=False)
    spark.stop()
    server.stop()
