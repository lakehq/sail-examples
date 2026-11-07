FACT = {
    "id": "date-trunc-timestamp-ntz",
    "title": "Choose an explicit type when truncating timestamps",
    "category": "silent-behavior",
    "text": """`date_trunc(unit, ts)` on a `TIMESTAMP_NTZ` column (the type Parquet timestamps without a
        time zone are read as) returns `TIMESTAMP_NTZ` on Sail but `TIMESTAMP` on Apache Spark. So the
        column type in files written from it differs, and collected values can differ when the client's
        local time zone is not the session time zone. Cast the result
        (`CAST(date_trunc('MONTH', ts) AS TIMESTAMP_NTZ)`) or truncate a date (`trunc(date(ts), 'MM')`),
        which give the same type and value on both.""",
    "engines": ["sail", "spark"],
    "order": 5,
    "env": {"TZ": "America/Los_Angeles"},
}

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

SQL = """SELECT typeof(date_trunc('MONTH', ts)) AS raw_type,
                typeof(CAST(date_trunc('MONTH', ts) AS TIMESTAMP_NTZ)) AS cast_type,
                CAST(CAST(date_trunc('MONTH', ts) AS TIMESTAMP_NTZ) AS STRING) AS cast_value,
                typeof(trunc(date(ts), 'MM')) AS date_type,
                CAST(trunc(date(ts), 'MM') AS STRING) AS date_value
         FROM VALUES (TIMESTAMP_NTZ'2024-03-17 21:45:00') AS t(ts)"""
VALUE = "TIMESTAMP_NTZ'2024-03-17 21:45:00'"


def check(spark, engine):
    rows = {}
    for zone in ("UTC", "America/Los_Angeles"):
        spark.conf.set("spark.sql.session.timeZone", zone)
        rows[zone] = spark.sql(SQL).first().asDict()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for zone, row in rows.items():
        assert row["cast_type"] == "timestamp_ntz", f"{zone}: {row}"
        assert row["cast_value"] == "2024-03-01 00:00:00", f"{zone}: {row}"
        assert (row["date_type"], row["date_value"]) == ("date", "2024-03-01"), f"{zone}: {row}"
    raw = {zone: row["raw_type"] for zone, row in rows.items()}
    expected = "timestamp" if engine == "spark" else "timestamp_ntz"
    assert set(raw.values()) == {expected}, f"date_trunc on TIMESTAMP_NTZ returned {raw} on {engine}"

    truncated = spark.sql(f"SELECT date_trunc('MONTH', {VALUE}) AS m")
    # The session is UTC and the client process runs in Los Angeles (FACT env).
    collected = truncated.first().m
    expected_collected = datetime(2024, 2, 29, 16, 0) if engine == "spark" else datetime(2024, 3, 1, 0, 0)
    assert collected == expected_collected, f"collected {collected!r} on {engine}"
    directory = Path(tempfile.mkdtemp()) / "truncated"
    truncated.write.parquet(str(directory))
    file = pq.ParquetFile(next(directory.glob("*.parquet")))
    written = (str(file.schema_arrow.field("m").type), file.metadata.schema.column(0).physical_type)
    expected_written = ("timestamp[ns]", "INT96") if engine == "spark" else ("timestamp[us]", "INT64")
    assert written == expected_written, f"{engine} wrote {written}"
    return {"raw_type": expected, "collected": str(collected), "parquet": written}
