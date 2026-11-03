"""Independent boundary, revision, leakage and deferred-plan controls on both engines."""

import datetime as dt
import importlib.util
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "pipelines/build-point-in-time-features/main.py"
    spec = importlib.util.spec_from_file_location("point_in_time_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def collect(outputs):
    return {name: [row.asDict() for row in frame.collect()] for name, frame in outputs.items()}


def check(spark, engine):
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    module = load_example()
    data = module.demo_inputs()
    # Do not collect the first plan until another invocation has built different inputs.
    deferred = module.build_features(spark, **data)
    old_data = dict(data)
    decision = dt.datetime(2026, 1, 5)
    old_data["requests"] = [row for row in data["requests"] if row["decision_at"] == decision]
    old_data["orders"] = [row for row in data["orders"] if row["available_at"] <= decision]
    old_data["products"] = [row for row in data["products"] if row["available_at"] <= decision]
    old_outputs = module.build_features(spark, **old_data)
    actual = collect(deferred)
    assert actual == module.python_reference(**data), f"{engine}: default differs from independent oracle"
    old_features = [row.asDict() for row in old_outputs["features"].collect()]
    assert old_features == [row for row in actual["features"] if row["decision_at"] == decision]
    assert actual["features"][0]["units"] == 12
    assert actual["naive_features"][0]["units"] == 25

    boundary = decision - dt.timedelta(days=1)

    def time(hours):
        return decision + dt.timedelta(hours=hours)

    requests = [
        {
            "request_id": "boundary",
            "product_id": "tea",
            "history_start": boundary,
            "decision_at": decision,
            "horizon_end": time(24),
        }
    ]
    orders = []
    for identity, revision, event, available, units, product in [
        ("left", 1, boundary, boundary, 2, "tea"),
        ("before-left", 1, boundary - dt.timedelta(microseconds=1), boundary, 100, "tea"),
        ("just-before", 1, decision - dt.timedelta(microseconds=1), decision, 3, "tea"),
        ("at-decision", 1, decision, decision, 5, "tea"),
        ("at-end", 1, time(24), time(24), 100, "tea"),
        ("cancelled", 1, time(-3), time(-3), 50, "tea"),
        ("cancelled", 2, time(-3), time(-1), 0, "tea"),
        ("changed-product", 1, time(-4), time(-4), 60, "tea"),
        ("changed-product", 2, time(-4), time(-2), 60, "coffee"),
        ("moved-in", 1, time(-25), time(-25), 7, "tea"),
        ("moved-in", 2, time(-5), time(-1), 7, "tea"),
        ("revision-tie", 2, time(-7), time(-1), 11, "tea"),
        ("revision-tie", 1, time(-7), time(-1), 200, "tea"),
        ("late", 1, time(-2), time(1), 1000, "tea"),
    ]:
        orders.append(
            {
                "order_id": identity,
                "revision": revision,
                "product_id": product,
                "event_at": event,
                "available_at": available,
                "units": units,
            }
        )
    products = [
        {
            "interval_id": "old",
            "revision": 1,
            "product_id": "tea",
            "valid_from": time(-48),
            "valid_to": decision,
            "available_at": time(-48),
            "category": "old",
            "lead_days": 99,
        },
        {
            "interval_id": "new",
            "revision": 1,
            "product_id": "tea",
            "valid_from": decision,
            "valid_to": time(48),
            "available_at": decision,
            "category": "new",
            "lead_days": 1,
        },
    ]
    coverage = [{"product_id": "tea", "observed_through": time(24), "available_at": time(25)}]
    bundle = {
        "requests": requests,
        "orders": orders,
        "products": products,
        "coverage": coverage,
        "evaluation_at": time(25),
    }
    result = collect(module.build_features(spark, **bundle))
    assert result == module.python_reference(**bundle)
    feature = result["features"][0]
    assert (feature["units"], feature["order_count"], feature["category"]) == (23, 4, "new")
    assert feature["last_order_at"] == decision - dt.timedelta(microseconds=1)
    assert result["outcomes"][0]["target_units"] == 5
    assert result["outcomes"][0]["outcome_status"] == "observed"

    # A known interval correction moving it away must not resurrect its older match.
    products.append(dict(products[1], revision=2, valid_from=time(1), available_at=decision))
    moved = collect(module.build_features(spark, **bundle))
    assert moved["features"] == []
    assert moved["rejected"][0]["reason"] == "missing_product_interval"

    # Empty history with proven coverage is observed zero; lack of coverage is unknown.
    zero_bundle = dict(bundle, orders=[], products=products[:2])
    observed_zero = collect(module.build_features(spark, **zero_bundle))
    assert observed_zero["features"][0]["units"] == 0
    assert observed_zero["outcomes"][0]["target_units"] == 0
    unavailable = module.build_features(spark, **dict(zero_bundle, coverage=[]))
    assert unavailable["outcomes"].collect()[0].target_units is None

    empty = collect(module.build_features(spark, [], [], [], [], decision))
    assert empty == {name: [] for name in ("features", "naive_features", "outcomes", "rejected")}
    return {
        "default_requests": len(data["requests"]),
        "boundary_units": feature["units"],
        "late_append_preserved": len(old_features),
        "empty_outputs": len(empty),
    }
