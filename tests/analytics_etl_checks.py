"""Adversarial fixtures with literal expected results, run in each engine environment."""

import datetime as dt
import hashlib
import importlib.util
import json
import os
import tempfile
from collections import Counter
from pathlib import Path

from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
LAUNCH = dt.datetime(2024, 1, 1)
EVENT_SCHEMA = "user_id long, ts timestamp_ntz, event string"


def example(directory):
    spec = importlib.util.spec_from_file_location("tabular_example", ROOT / directory / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def analyze(module, spark, rows, *, weeks=3, steps=None):
    if hasattr(module, "analyze_events"):
        return module.analyze_events(
            spark.createDataFrame(rows, EVENT_SCHEMA),
            launch=LAUNCH,
            weeks=weeks,
            steps=steps or module.STEPS,
        )
    # Allows the same fixture to demonstrate the original behavior before the repair.
    module.make_events = lambda: rows
    module.WEEKS = weeks
    return module.run(spark)


def funnel_check(spark):
    module = example("analytics/sessions-funnels-retention")
    rows = []
    streams = {
        0: [(0, "purchase"), (1, "page_view"), (2, "product_view"), (3, "add_to_cart"), (4, "checkout")],
        1: [(0, "page_view"), (0, "product_view"), (1, "add_to_cart"), (2, "checkout"), (3, "purchase")],
        2: [(0, "page_view"), (1, "product_view"), (2, "add_to_cart"), (33, "checkout"), (34, "purchase")],
        3: [
            (0, "page_view"),
            (1, "page_view"),
            (2, "product_view"),
            (3, "product_view"),
            (4, "add_to_cart"),
            (5, "checkout"),
            (6, "purchase"),
            (7, "purchase"),
        ],
    }
    for user, events in streams.items():
        rows.extend((user, LAUNCH + dt.timedelta(minutes=minute), event) for minute, event in events)
    result = analyze(module, spark, rows)
    funnel = result["funnel"].collect()
    assert [row.sessions for row in funnel] == [4, 3, 3, 2, 1], funnel
    assert [row.share_of_previous_step for row in funnel] == [None, 0.75, 1.0, 0.6667, 0.5]
    enriched = spark.createDataFrame(rows, EVENT_SCHEMA).withColumn("session_number", F.lit(99))
    assert [row.sessions for row in module.analyze_events(enriched)["funnel"].collect()] == [4, 3, 3, 2, 1]

    # Identical timestamp peers share a session; an exact 30-minute gap stays in it.
    end = LAUNCH + dt.timedelta(minutes=30)
    peers = [(0, LAUNCH, "page_view")] * 2 + [(0, end, "product_view")]
    peers.append((0, end + dt.timedelta(minutes=30, microseconds=1), "add_to_cart"))
    peer_results = analyze(module, spark, peers)
    peer_summary = peer_results["sessions"].first()
    assert peer_summary.sessions == 2 and peer_summary.avg_events_per_session == 2.0, peer_summary
    assert [row.sessions for row in peer_results["funnel"].collect()] == [1, 1, 0, 0, 0]

    repeated = [(0, LAUNCH + dt.timedelta(seconds=i), event) for i, event in enumerate(["A", "A", "B", "A"])]
    result = analyze(module, spark, repeated, steps=["A", "B", "A"])
    assert [row.sessions for row in result["funnel"].collect()] == [1, 1, 1]
    result = analyze(module, spark, [(0, LAUNCH, "other")])
    assert all(row.sessions == 0 and row.share_of_previous_step is None for row in result["funnel"].collect())
    return {"ordered_sessions": 5, "timestamp_peer_rows": 4, "repeated_stage_names": True}


def retention_check(spark):
    module = example("analytics/sessions-funnels-retention")
    rows = [(0, LAUNCH, "page_view")] * 2 + [(1, LAUNCH + dt.timedelta(weeks=1), "page_view")]
    rows += [
        (9, LAUNCH - dt.timedelta(microseconds=1), "page_view"),
        (10, LAUNCH + dt.timedelta(weeks=3), "page_view"),
    ]
    result = analyze(module, spark, rows)
    matrix = [row.asDict() for row in result["matrix"].collect()]
    assert matrix == [
        {"cohort_week": 0, "cohort_users": 1, "0": 1.0, "1": 0.0, "2": 0.0},
        {"cohort_week": 1, "cohort_users": 1, "0": 1.0, "1": 0.0, "2": None},
    ], matrix
    long = result["retention"].collect()
    assert [(r.cohort_week, r.weeks_later, r.active_users) for r in long] == [
        (0, 0, 1),
        (0, 1, 0),
        (0, 2, 0),
        (1, 0, 1),
        (1, 1, 0),
    ], long
    empty = analyze(module, spark, [])
    assert empty["sessions"].first().sessions == 0
    assert empty["retention"].count() == empty["matrix"].count() == 0
    return {"observable_cells": 5, "future_null_cells": 1, "empty_input": True}


def deferred_check(spark):
    module = example("analytics/sessions-funnels-retention")
    rows = [
        (0, LAUNCH, "page_view"),
        (0, LAUNCH + dt.timedelta(minutes=1), "product_view"),
        (1, LAUNCH + dt.timedelta(weeks=1), "page_view"),
    ]
    first = analyze(module, spark, rows)
    second = analyze(
        module,
        spark,
        [(100, LAUNCH + dt.timedelta(seconds=i), event) for i, event in enumerate(module.STEPS)],
        weeks=1,
    )
    summary = first["sessions"].first().asDict()
    assert summary == {
        "sessions": 2,
        "users": 2,
        "avg_events_per_session": 1.5,
        "median_session_minutes": 0.5,
    }, summary
    assert [row.sessions for row in first["funnel"].collect()] == [2, 1, 0, 0, 0]
    assert [(row.cohort_week, row.weeks_later, row.active_users) for row in first["retention"].collect()] == [
        (0, 0, 1),
        (0, 1, 0),
        (0, 2, 0),
        (1, 0, 1),
        (1, 1, 0),
    ]
    assert [row.asDict() for row in first["matrix"].collect()] == [
        {"cohort_week": 0, "cohort_users": 1, "0": 1.0, "1": 0.0, "2": 0.0},
        {"cohort_week": 1, "cohort_users": 1, "0": 1.0, "1": 0.0, "2": None},
    ]
    assert second["sessions"].first().sessions == 1
    assert [row.sessions for row in second["funnel"].collect()] == [1] * 5
    return {"deferred_analyses_are_isolated": True}


def payloads(rows):
    return Counter(json.dumps(row, sort_keys=True, default=str) for row in rows)


def rejects_check(spark):
    import pyarrow as pa
    import pyarrow.parquet as pq

    module = example("pipelines/taxi-etl-job")
    with tempfile.TemporaryDirectory(prefix="etl-contract-") as temporary:
        folder = Path(temporary)
        module.DATA = folder
        module.MONTHS = {}
        files = {}

        def write_input(month, rows):
            name = f"trips/green_tripdata_2024-{month:02d}.parquet"
            path = folder / name
            path.parent.mkdir(exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), path)
            module.MONTHS[month] = hashlib.sha256(path.read_bytes()).hexdigest()
            files[name] = path
            return path

        def row(month=1, **changes):
            return {
                "VendorID": 1,
                "lpep_pickup_datetime": dt.datetime(2024, month, 2, 10),
                "lpep_dropoff_datetime": dt.datetime(2024, month, 2, 10, 10),
                "PULocationID": 1,
                "DOLocationID": 2,
                "passenger_count": 1,
                "trip_distance": 1.2,
                "fare_amount": 5.0,
                "tip_amount": 0.0,
                "total_amount": 5.0,
                "payment_type": 1,
                "trip_type": 1,
                "unused_source_value": "retain this original value",
                **changes,
            }

        missing = row(lpep_pickup_datetime=None)
        wrong_month = row(lpep_pickup_datetime=dt.datetime(2024, 2, 2, 10), fare_amount=-5.0)
        unknown = row(PULocationID=999)
        write_input(
            1,
            [
                row(),
                row(unused_source_value="different original-only value"),
                missing,
                missing,
                wrong_month,
                unknown,
            ],
        )
        module.download = lambda name, *args: files[name]
        zones = spark.createDataFrame([(1,), (2,)], "LocationID int")
        zones.createOrReplaceTempView("zones")
        valid, rejected = module.load_month(spark, 1)
        assert {"source_record", "source_file", "source_sha256", "reject_reason"} <= set(rejected.columns), (
            rejected.columns
        )
        rejected_rows = [r.asDict(recursive=True) for r in rejected.collect()]
        assert valid.count() == 1 and len(rejected_rows) == 4
        assert payloads([r["source_record"] for r in rejected_rows]) == payloads(
            [missing, missing, wrong_month, unknown]
        )
        assert Counter(r["reject_reason"] for r in rejected_rows) == {
            "missing time, distance or amount": 2,
            "pickup outside the file's month": 1,
            "unknown pickup or dropoff zone": 1,
        }
        assert all(r["source_file"] == "trips/green_tripdata_2024-01.parquet" for r in rejected_rows)
        assert all(r["source_sha256"] == module.MONTHS[1] for r in rejected_rows)
        assert all(r["month"] == 1 for r in rejected_rows)

        clean_path, reject_path = str(folder / "clean"), str(folder / "rejects")
        module.write_month(spark, 1, clean_path, reject_path)
        persisted = spark.read.schema(rejected.schema).parquet(reject_path)
        assert payloads([r.asDict(recursive=True) for r in persisted.collect()]) == payloads(rejected_rows)
        write_input(3, [row(3), row(3, PULocationID=999)])
        module.write_month(spark, 3, clean_path, reject_path)
        before = [
            r.asDict(recursive=True)
            for r in spark.read.schema(rejected.schema).parquet(reject_path).where("month = 3").collect()
        ]
        write_input(1, [row()])
        module.write_month(spark, 1, clean_path, reject_path)
        after = [
            r.asDict(recursive=True)
            for r in spark.read.schema(rejected.schema).parquet(reject_path).collect()
        ]
        assert payloads(after) == payloads(before), (
            "a reject-free rerun must remove stale rejects only in its month"
        )
        assert spark.read.schema(module.CLEAN_SCHEMA).parquet(clean_path).count() == 2
        spark.createDataFrame([(1,), (1,), (2,)], "LocationID int").createOrReplaceTempView("zones")
        try:
            module.load_month(spark, 1)
        except ValueError as error:
            assert "duplicate" in str(error).lower() and "zone" in str(error).lower()
        else:
            raise AssertionError("duplicate zone IDs must fail before multiplying source records")
    return {
        "raw_rows": 6,
        "clean_rows": 1,
        "rejected_occurrences": 4,
        "duplicates_removed": 1,
        "empty_reject_rerun": True,
        "duplicate_lookup_rejected": True,
    }


def check(spark, engine):
    case = os.environ["SAIL_EXAMPLES_TABULAR_CASE"]
    return {
        "engine": engine,
        "case": case,
        "observed": {
            "funnel": funnel_check,
            "retention": retention_check,
            "rejects": rejects_check,
            "deferred": deferred_check,
        }[case](spark),
    }
