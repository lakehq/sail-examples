FACT = {
    "id": "collect-timestamps-local-time",
    "title": "Collected timestamps use the client’s time zone",
    "category": "run-and-connect",
    "text": """`collect()` returns `TIMESTAMP` values as Python datetimes in the client process's local
        time zone, whatever `spark.sql.session.timeZone` is; `TIMESTAMP_NTZ` values come back unchanged.
        Store wall-clock values as `TIMESTAMP_NTZ` (for example `to_timestamp_ntz`), or compare instants,
        so tests do not depend on the machine they run on.""",
    "engines": ["sail", "spark"],
    "order": 4,
    "env": {"TZ": "America/Los_Angeles"},
}

from datetime import datetime


def check(spark, engine):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    row = spark.sql(
        "SELECT to_timestamp('2026-01-02 09:00:00') AS ts, to_timestamp_ntz('2026-01-02 09:00:00') AS ntz"
    ).collect()[0]
    # 09:00 UTC is 01:00 in Los Angeles in January (UTC-8).
    assert row.ts == datetime(2026, 1, 2, 1, 0), f"TIMESTAMP came back as {row.ts!r}"
    assert row.ntz == datetime(2026, 1, 2, 9, 0), f"TIMESTAMP_NTZ came back as {row.ntz!r}"
    return {"timestamp": str(row.ts), "timestamp_ntz": str(row.ntz)}
