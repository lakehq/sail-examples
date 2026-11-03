# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Use Sail as the SQL engine inside a Python app: pandas in, parameterized SQL, pandas out.

The app keeps its data in pandas DataFrames (here, real NYC green taxi trips from January 2024).
It registers them as views, answers questions with parameterized SQL, and gets pandas or Arrow
results back for the rest of the program.
"""

import datetime as dt
import hashlib
import os
import tempfile
import urllib.request
from pathlib import Path

import pandas as pd
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

# The app's query. `:name` markers are parameters: values are passed separately, never pasted into SQL.
ZONE_REPORT = """
    SELECT z.Zone AS zone, count(*) AS trips, median(t.fare_amount) AS median_fare
    FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
    WHERE z.Borough = :borough AND t.pickup_date BETWEEN :first_day AND :last_day
    GROUP BY z.Zone
    ORDER BY trips DESC, zone
    LIMIT :top
"""


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


def load_app_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """The app's own data, as pandas DataFrames."""
    columns = ["lpep_pickup_datetime", "PULocationID", "fare_amount", "payment_type"]
    trips = pd.read_parquet(download("green_tripdata_2024-01.parquet"), columns=columns)
    trips["pickup_date"] = trips.pop("lpep_pickup_datetime").dt.date
    # The column has missing values, so pandas reads it as float64; Int64 is pandas' nullable integer.
    trips["payment_type"] = trips["payment_type"].astype("Int64")
    # Keep the lookup's literal "N/A" labels as text: by default pandas reads them as missing values.
    zones = pd.read_csv(download("taxi_zone_lookup.csv"), keep_default_na=False)
    return trips, zones


def zone_report(spark: SparkSession, borough: str, first_day: dt.date, last_day: dt.date, top: int = 5):
    """The busiest pickup zones in a borough between two dates, as a pandas DataFrame."""
    args = {"borough": borough, "first_day": first_day, "last_day": last_day, "top": top}
    return spark.sql(ZONE_REPORT, args=args).toPandas()


def run(spark: SparkSession) -> dict[str, DataFrame]:
    trips, zones = load_app_data()
    # pandas in: each DataFrame becomes a view that SQL can query.
    spark.createDataFrame(trips).createOrReplaceTempView("trips")
    spark.createDataFrame(zones).createOrReplaceTempView("zones")

    # pandas out: the app calls its report function with plain Python values.
    first_week = zone_report(spark, "Manhattan", dt.date(2024, 1, 1), dt.date(2024, 1, 7))
    whole_month = zone_report(spark, "Queens", dt.date(2024, 1, 1), dt.date(2024, 1, 31), top=3)
    # A parameter is only ever a value, so input crafted to change the query matches nothing.
    crafted = zone_report(spark, "Queens' OR '1'='1", dt.date(2024, 1, 1), dt.date(2024, 1, 31))

    # Arrow out: hand columnar data to any Arrow-aware library without converting row by row.
    daily = spark.sql("""
        SELECT pickup_date, count(*) AS trips, count_if(payment_type = 1) AS card_trips
        FROM trips WHERE pickup_date BETWEEN DATE'2024-01-01' AND DATE'2024-01-31'
        GROUP BY pickup_date ORDER BY pickup_date
    """).toArrow()

    # The harness compares DataFrames, so the app's results go back in for checking.
    return {
        "manhattan_first_week": spark.createDataFrame(first_week),
        "queens_january": spark.createDataFrame(whole_month),
        "crafted_input_rows": spark.createDataFrame([(len(crafted),)], "rows long"),
        "daily": spark.createDataFrame(daily),
        "trip_types": spark.createDataFrame(spark.table("trips").dtypes, "column string, type string"),
        "zone_labels": spark.sql(
            "SELECT Borough AS borough, count(*) AS zones FROM zones "
            "WHERE Borough IN ('N/A', 'Unknown') GROUP BY Borough ORDER BY Borough"
        ),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    run(spark)
    print("Busiest Manhattan pickup zones, January 1 to 7, 2024:")
    print(zone_report(spark, "Manhattan", dt.date(2024, 1, 1), dt.date(2024, 1, 7)).to_string(index=False))
    spark.stop()
    server.stop()
