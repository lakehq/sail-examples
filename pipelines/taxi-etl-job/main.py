# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""A batch ETL job: raw monthly files to validated, partitioned Parquet to reporting tables.

The input is a year of real NYC green taxi trip records (twelve monthly Parquet files). The job
loads each month into a fixed schema, rejects invalid rows with a reason, removes duplicates,
writes clean rows and original rejected records to monthly partitions, and builds two reporting
tables. Re-running a month replaces only that month's clean and reject directories.
"""

import atexit
import hashlib
import os
import shutil
import tempfile
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA = Path(__file__).resolve().parent / ".data"
SOURCE = "https://d37ci6vzurychx.cloudfront.net"
# SHA-256 of each file this example was written against.
MONTHS = {
    1: "1512a953bed564ac68dcb622827424dfb4f68ad93c2a4572e50ef6ab0b09c1ad",
    2: "cb7eb405ee7e795c9fd6b8ad1b291f46f4e0a1cd60a57067b0aaca2420e8bc5d",
    3: "43dedd66dd556acfc38da679a174b6adc32e01dd6dccad14bdfdf1521a14abed",
    4: "af174977258a02f8f44c37534abc9a8b4eaa5ec6d2f9c2bc49782327628b608c",
    5: "aa8b1965ed36f0bda1b342930d5e6f5a89f8fe56d0c6c6d77d9c156b408aa0ea",
    6: "47e7c37dd39069c474364aee3d76c9ddc887a3f714e9bb550b5866fe0b89b9cd",
    7: "ce0d7591316050bea21da49e383eb733411e98ae17e114afcc7fbe4b36b116f2",
    8: "733210fb27cbe8cf8b0ad9f5d6fff03dfafcc8ff7c2beb1c1b707825d72f0376",
    9: "8f6aa8f3e8a2040fc161c7fbcb60e5418238b27e7e95998363572a4c977b69ce",
    10: "6e737b7c4edac91f865aa210f7110d0118a9992b3a96720936e090277c24edaa",
    11: "fde255b43d4158124921b7c085d9abf15de5c5a9128c2ce0fb4e2372bc1e0d4a",
    12: "1d708f58d29b0795a54ca11a36db6df61262986d122985f0fb52a56acc064bac",
}
ZONES = (
    f"{SOURCE}/misc/taxi_zone_lookup.csv",
    "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed",
)

# The contract for clean trips: column names and types, independent of the raw file's naming.
CLEAN_COLUMNS = [
    "CAST(VendorID AS INT) AS vendor_id",
    "lpep_pickup_datetime AS pickup_ts",
    "lpep_dropoff_datetime AS dropoff_ts",
    "CAST(PULocationID AS INT) AS pickup_zone_id",
    "CAST(DOLocationID AS INT) AS dropoff_zone_id",
    "CAST(passenger_count AS INT) AS passengers",
    "trip_distance AS miles",
    "fare_amount AS fare",
    "tip_amount AS tip",
    "total_amount AS total",
    "CAST(payment_type AS INT) AS payment_type",
    "CAST(trip_type AS INT) AS trip_type",
]
CLEAN_SCHEMA = (
    "vendor_id int, pickup_ts timestamp_ntz, dropoff_ts timestamp_ntz, pickup_zone_id int, "
    "dropoff_zone_id int, passengers int, miles double, fare double, tip double, total double, "
    "payment_type int, trip_type int, minutes bigint, pickup_date date, month int"
)
# Each rule, in order: the first one a row fails becomes its reject reason.
RULES = [
    (
        "missing time, distance or amount",
        "pickup_ts IS NULL OR dropoff_ts IS NULL OR miles IS NULL OR fare IS NULL OR total IS NULL",
    ),
    ("pickup outside the file's month", "year(pickup_ts) <> 2024 OR month(pickup_ts) <> month"),
    ("duration not 1 to 180 minutes", "minutes NOT BETWEEN 1 AND 180"),
    ("distance not 0 to 100 miles", "miles NOT BETWEEN 0 AND 100"),
    ("negative amount", "fare < 0 OR total < 0"),
    ("unknown pickup or dropoff zone", "pickup_known IS NULL OR dropoff_known IS NULL"),
]


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def download(name: str, url: str, expected: str) -> Path:
    """Reuse or download a file only when its SHA-256 matches the pinned input."""
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


def validate_trips(
    raw: DataFrame, zones: DataFrame, month: int, source_file: str, source_sha256: str
) -> tuple[DataFrame, DataFrame]:
    """Normalize valid trips; retain original source values and provenance for each reject."""
    if zones.groupBy("LocationID").count().where("count > 1").take(1):
        raise ValueError("duplicate zone IDs would multiply source records")
    # These pinned monthly inputs share one raw schema. Capture it before casts or projections.
    original = raw.withColumn("source_record", F.struct(*[F.col(name) for name in raw.columns]))
    # The month comes from the file being loaded, not from the rows: the first rule checks pickups against it.
    typed = original.selectExpr(*CLEAN_COLUMNS, "source_record").withColumn("month", F.lit(month))
    typed = typed.withColumn("minutes", F.expr("timestampdiff(MINUTE, pickup_ts, dropoff_ts)"))
    typed = typed.withColumn("pickup_date", F.to_date("pickup_ts"))

    zones = zones.select("LocationID")
    checked = typed.join(
        zones.withColumnRenamed("LocationID", "pickup_known"),
        F.col("pickup_zone_id") == F.col("pickup_known"),
        "left",
    ).join(
        zones.withColumnRenamed("LocationID", "dropoff_known"),
        F.col("dropoff_zone_id") == F.col("dropoff_known"),
        "left",
    )
    reason = F.lit(None).cast("string")
    for label, condition in reversed(RULES):
        # A condition that evaluates to NULL counts as failed, so no row passes a rule it was not checked against.
        reason = F.when(F.coalesce(F.expr(condition), F.lit(True)), label).otherwise(reason)
    checked = checked.withColumn("reject_reason", reason).drop("pickup_known", "dropoff_known")

    # Deduplicate only the clean schema. Original unused source fields are not clean-row identity.
    valid = (
        checked.where("reject_reason IS NULL")
        .selectExpr(*[column.split()[0] for column in CLEAN_SCHEMA.split(", ")])
        .dropDuplicates()
    )
    # Do not deduplicate rejected records: identical source occurrences still count separately.
    rejected = checked.where("reject_reason IS NOT NULL").select(
        "month",
        "reject_reason",
        "source_record",
        F.lit(source_file).alias("source_file"),
        F.lit(source_sha256).alias("source_sha256"),
    )
    return valid, rejected


def load_month(spark: SparkSession, month: int) -> tuple[DataFrame, DataFrame]:
    """Read a verified source file and attach its relative filename and pinned digest."""
    name = f"trips/green_tripdata_2024-{month:02d}.parquet"
    raw = spark.read.parquet(str(download(name, f"{SOURCE}/trip-data/{name[6:]}", MONTHS[month])))
    return validate_trips(raw, spark.table("zones"), month, name, MONTHS[month])


def write_month(spark: SparkSession, month: int, clean_path: str, reject_path: str):
    """Replace this month's two directories, including an empty reject partition."""
    valid, rejected = load_month(spark, month)
    valid.drop("month").write.mode("overwrite").parquet(f"{clean_path}/month={month}")
    rejected.drop("month").write.mode("overwrite").parquet(f"{reject_path}/month={month}")
    return rejected.schema


def fingerprint(table: DataFrame) -> tuple[int, int]:
    """The row count and an order-independent checksum of every row: the sum of the rows' hashes."""
    row = table.agg(F.count("*"), F.sum(F.hash(*table.columns).cast("long"))).first()
    return row[0], row[1]


def run(spark: SparkSession) -> dict[str, DataFrame]:
    zones_path = download("taxi_zone_lookup.csv", *ZONES)
    spark.read.schema("LocationID int, Borough string, Zone string, service_zone string").csv(
        str(zones_path), header=True
    ).createOrReplaceTempView("zones")

    clean_path, reject_path, gold_path = (
        temp_dir("clean-trips-"),
        temp_dir("rejected-trips-"),
        temp_dir("reports-"),
    )

    raw_rows = 0
    for month in MONTHS:
        reject_schema = write_month(spark, month, clean_path, reject_path)
        raw_rows += spark.read.parquet(str(DATA / f"trips/green_tripdata_2024-{month:02d}.parquet")).count()

    # Read the clean table back with its declared schema: the contract, including the month's type.
    clean = spark.read.schema(CLEAN_SCHEMA).parquet(clean_path)
    clean_rows_first_run, checksum_first_run = fingerprint(clean)

    # Run March again: the job is idempotent, so the table must not change.
    write_month(spark, 3, clean_path, reject_path)
    clean = spark.read.schema(CLEAN_SCHEMA).parquet(clean_path)
    clean_rows_after_rerun, checksum_after_rerun = fingerprint(clean)

    rejected = spark.read.schema(reject_schema).parquet(reject_path)
    quality = rejected.groupBy("reject_reason").count().withColumnRenamed("count", "rows")
    rejected_rows = rejected.count()
    totals = spark.createDataFrame(
        [
            (
                raw_rows,
                rejected_rows,
                raw_rows - rejected_rows - clean_rows_first_run,
                clean_rows_first_run,
                clean_rows_after_rerun,
                checksum_after_rerun == checksum_first_run,
            )
        ],
        "raw_rows long, rejected_rows long, duplicates_removed long, clean_rows long, "
        "clean_rows_after_rerun long, same_rows_after_rerun boolean",
    )

    # Reporting tables, written as files for a dashboard or a downstream job to read.
    clean.createOrReplaceTempView("clean_trips")
    daily = spark.sql("""
        SELECT pickup_date, count(*) AS trips, round(sum(fare), 2) AS fares,
               round(avg(miles), 3) AS avg_miles,
               round(avg(CASE WHEN payment_type = 1 THEN 1.0D ELSE 0.0D END), 4) AS card_share
        FROM clean_trips GROUP BY pickup_date
    """)
    borough_month = spark.sql("""
        SELECT z.Borough AS borough, t.month, count(*) AS trips, round(avg(t.minutes), 2) AS avg_minutes
        FROM clean_trips t JOIN zones z ON t.pickup_zone_id = z.LocationID
        GROUP BY z.Borough, t.month
    """)
    daily.write.mode("overwrite").parquet(f"{gold_path}/daily")
    borough_month.write.mode("overwrite").parquet(f"{gold_path}/borough_month")

    return {
        "totals": totals,
        "rejected": rejected.orderBy("month", "reject_reason", "source_file", F.to_json("source_record")),
        "quality": quality.orderBy(F.desc("rows"), "reject_reason"),
        "clean_by_month": clean.groupBy("month").count().orderBy("month"),
        "daily": spark.read.parquet(f"{gold_path}/daily").orderBy("pickup_date"),
        "borough_month": spark.read.parquet(f"{gold_path}/borough_month").orderBy("borough", "month"),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    for name in ("totals", "quality", "clean_by_month"):
        results[name].show(12, truncate=False)
    results["daily"].show(5)
    spark.stop()
    server.stop()
