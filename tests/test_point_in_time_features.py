"""Point-in-time features retain only information known at each decision."""

import copy
import datetime as dt
from pathlib import Path

import pytest
from harness import run
from point_in_time_checks import load_example

pytestmark = pytest.mark.examples("build-point-in-time-features")


def test_reference_exposes_late_knowledge_and_unavailable_outcomes():
    module = load_example()
    result = module.python_reference(**module.demo_inputs())
    features = {row["request_id"]: row for row in result["features"]}
    naive = {row["request_id"]: row for row in result["naive_features"]}
    assert (features["early"]["units"], features["early"]["order_count"]) == (12, 3)
    assert naive["early"]["units"] == 25
    assert (features["later"]["units"], features["later"]["lead_days"]) == (16, 2)
    assert features["zero"]["units"] == 0 and features["zero"]["last_order_at"] is None
    outcomes = {row["request_id"]: row for row in result["outcomes"]}
    assert outcomes["early"]["target_units"] == 7
    assert outcomes["later"]["target_units"] is None
    assert outcomes["zero"]["outcome_status"] == "unavailable"
    assert [(row["request_id"], row["reason"]) for row in result["rejected"]] == [
        ("missing", "missing_product_interval"),
        ("overlap", "ambiguous_product_intervals"),
    ]


@pytest.mark.parametrize(
    "table,index,field,value,reason",
    [
        ("orders", 0, "units", -1, "invalid units"),
        ("orders", 0, "revision", True, "invalid revision"),
        ("orders", 0, "available_at", dt.datetime(2026, 1, 1), "order available before event"),
        ("products", 0, "valid_to", dt.datetime(2026, 1, 1), "invalid product interval"),
        ("products", 0, "lead_days", None, "invalid lead_days"),
        ("requests", 0, "horizon_end", dt.datetime(2026, 1, 5), "invalid request interval"),
        ("requests", 0, "product_id", " ", "invalid product_id"),
        ("coverage", 0, "observed_through", dt.datetime(2026, 1, 10), "coverage available before"),
    ],
)
def test_invalid_records_fail_before_query(table, index, field, value, reason):
    module = load_example()
    data = module.demo_inputs()
    data[table][index][field] = value
    with pytest.raises(ValueError, match=reason):
        module.build_features(None, **data)


@pytest.mark.parametrize("table", ["orders", "products"])
def test_same_revision_conflicts_fail_instead_of_choosing_a_tie(table):
    module = load_example()
    data = module.demo_inputs()
    conflicting = copy.deepcopy(data[table][0])
    conflicting["units" if table == "orders" else "lead_days"] += 1
    data[table].append(conflicting)
    with pytest.raises(ValueError, match="conflicting source revision"):
        module.build_features(None, **data)


def test_exact_duplicates_collapse_but_duplicate_requests_fail():
    module = load_example()
    data = module.demo_inputs()
    assert len(module.normalize_inputs(**data)["orders"]) == len(data["orders"]) - 1
    data["requests"].append(dict(data["requests"][0]))
    with pytest.raises(ValueError, match="duplicate request_id"):
        module.build_features(None, **data)


def test_utc_json_values_and_empty_lists_are_explicit():
    module = load_example()
    data = module.normalize_inputs([], [], [], [], "2026-01-09T00:00:00Z")
    assert data["evaluation_at"] == dt.datetime(2026, 1, 9)
    assert module.python_reference(**data) == {
        name: [] for name in ("features", "naive_features", "outcomes", "rejected")
    }
    for invalid in ("2026-01-09T00:00:00", "2026-01-09T00:00:00+01:00", None):
        with pytest.raises(ValueError):
            module.normalize_inputs([], [], [], [], invalid)


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_point_in_time_queries_match_independent_controls(engine, tmp_path):
    meta = run(
        engine, "fact", Path(__file__).with_name("point_in_time_checks.py"), tmp_path / engine, [], 240
    )
    assert meta["status"] == "ok", f"{engine}: {meta}"
