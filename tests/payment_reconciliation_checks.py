"""Independent ledger controls, evaluated in each engine's environment."""

import datetime as dt
import importlib.util
import os
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def example():
    spec = importlib.util.spec_from_file_location(
        "payments_example", ROOT / "analytics/reconcile-orders-payments/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def frames(module, spark, snapshots):
    return [
        spark.createDataFrame(rows, module.INPUT_SCHEMAS[source])
        for source, rows in zip(module.INPUT_SCHEMAS, snapshots, strict=True)
    ]


def collected(result):
    return {name: [row.asDict() for row in frame.collect()] for name, frame in result.items()}


def compare(module, spark, snapshots):
    outputs = module.reconcile(*frames(module, spark, snapshots))
    for name, frame in outputs.items():
        expected_columns = [field.split()[0] for field in module.OUTPUT_SCHEMAS[name].split(",")]
        assert frame.columns == expected_columns, f"{name} output column order differs: {frame.columns}"
    actual = collected(outputs)
    expected = module.reference_rows(*snapshots)
    assert actual == expected, f"engine result differs from the independent ledger: {actual} != {expected}"
    for row in actual["accounting"]:
        assert row["total_minor"] == row["attributed_minor"] + row["unmatched_minor"], row
    return actual


def literal_default(result):
    """Amounts computed from the fixture specification, not from engine expressions."""
    assert [
        (r["order_id"], r["captured_minor"], r["refunded_minor"], r["balance_minor"], r["status"])
        for r in result["orders"]
    ] == [
        ("over", 500, 600, 600, "over_refunded"),
        ("paid", 1000, 0, 0, "paid"),
        ("partial", 300, 0, 700, "under_captured"),
        ("refund", 1000, 200, 200, "partially_refunded"),
        ("waiting", 0, 0, 700, "unpaid"),
        ("yen", 1000, 0, 0, "paid"),
    ]
    assert [
        (
            r["source"],
            r["currency"],
            r["records"],
            r["deliveries"],
            r["total_minor"],
            r["attributed_minor"],
            r["unmatched_minor"],
        )
        for r in result["accounting"]
    ] == [
        ("capture", "EUR", 1, 1, 50, 0, 50),
        ("capture", "JPY", 1, 1, 1000, 1000, 0),
        ("capture", "USD", 6, 7, 2880, 2800, 80),
        ("refund", "EUR", 1, 1, 5, 0, 5),
        ("refund", "USD", 3, 4, 825, 800, 25),
    ]
    assert Counter(r["reason"] for r in result["exceptions"]) == {
        "conflicting_id": 2,
        "not_yet_received": 1,
        "currency_mismatch": 2,
        "missing_order": 1,
        "missing_capture": 1,
        "over_refunded": 1,
    }


def adversarial(module):
    """Conflicting parents, cutoff boundaries, invalid values and zero/three-decimal currency units."""
    posted = module.CUTOFF - dt.timedelta(hours=2)
    received = module.CUTOFF - dt.timedelta(hours=1)

    def row(key, value, parent=None, currency="KWD", **changes):
        source_id = "order_id" if parent is None else "capture_id" if key.startswith("c") else "refund_id"
        value = {
            source_id: key,
            "currency": currency,
            "amount_minor": value,
            "posted_at": posted,
            "received_at": received,
        }
        if parent is not None:
            value["order_id" if source_id == "capture_id" else "capture_id"] = parent
        return value | changes

    orders = [row("ok", 1500), row("conflicted", 10), row("conflicted", 11), row("zero", 0)]
    captures = [
        row("c-ok", 1500, "ok"),
        row("c-parent", 10, "conflicted"),
        row("c-conflict", 10, "ok"),
        row("c-conflict", 10, "absent"),
        row("c-boundary", 9, "ok", posted_at=module.CUTOFF),
        row("c-last", 7, "ok", posted_at=module.CUTOFF - dt.timedelta(microseconds=1)),
        row("c-negative", -1, "ok"),
        row("c-null", None, "ok"),
        row("c-huge", module.MAX_AMOUNT + 1, "ok"),
        row("c-time", 4, "ok", received_at=None),
        row("c-id ", 1, "ok"),
        row("c-tab\t", 1, "ok"),
        row("c-unicode-é", 1, "ok"),
        row("c-nbsp\u00a0", 1, "ok"),
        row("c-leading", 1, "ok", capture_id="\u3000c-leading"),
        row("c-currency", 1, "ok", currency="usd"),
    ]
    # Different receipt times are deliveries of one payload, not conflicting transactions.
    captures.append(row("c-ok", 1500, "ok", received_at=received + dt.timedelta(seconds=1)))
    # A contradictory delivery received exactly at cutoff cannot poison earlier eligible state.
    captures.append(row("c-ok", 999, "ok", received_at=module.CUTOFF))
    refunds = [
        row("r-ok", 200, "c-ok"),
        row("r-again", 300, "c-ok"),
        row("r-parent", 2, "c-parent"),
        row("r-conflict", 3, "c-conflict"),
        row("r-early", 5, "c-ok", posted_at=posted - dt.timedelta(microseconds=1)),
    ]
    return orders, captures, refunds


def contract_check(spark):
    module = example()
    snapshots = module.fixture()
    original = compare(module, spark, snapshots)
    literal_default(original)
    reordered = compare(module, spark, [list(reversed(rows)) for rows in snapshots])
    assert reordered == original, "delivery ordering changed reconciliation"
    replayed = compare(module, spark, [rows + rows for rows in snapshots])
    assert replayed["orders"] == original["orders"], "replay changed business state"
    for name in ("ledger", "exceptions", "accounting"):
        assert [{k: v for k, v in r.items() if k != "deliveries"} for r in replayed[name]] == [
            {k: v for k, v in r.items() if k != "deliveries"} for r in original[name]
        ], f"replay changed logical {name}"
        assert [r["deliveries"] for r in replayed[name]] == [2 * r["deliveries"] for r in original[name]], (
            name
        )
    return {"reorder": True, "replay_preserves_money": True, "delivery_counts_double": True}


def invalid_check(spark):
    module = example()
    snapshots = adversarial(module)
    actual = compare(module, spark, snapshots)
    assert len(actual["orders"]) == 1, "conflicting/invalid orders entered the parent dimension"
    order = actual["orders"][0]
    assert (
        order["captured_minor"],
        order["refunded_minor"],
        order["capture_count"],
        order["refund_count"],
    ) == (1507, 500, 2, 2), order
    assert Counter(r["reason"] for r in actual["exceptions"]) == {
        "conflicting_id": 4,
        "invalid_amount": 4,
        "invalid_timestamp": 1,
        "invalid_id": 5,
        "invalid_currency": 1,
        "not_yet_received": 1,
        "not_yet_posted": 1,
        "missing_order": 1,
        "unmatched_capture_order": 1,
        "missing_capture": 1,
        "refund_before_capture": 1,
    }, actual["exceptions"]
    invalid = next(r for r in actual["exceptions"] if r["record_id"] == "c-negative")
    assert (invalid["parent_id"], invalid["amount_minor"], invalid["posted_at"]) == (
        "ok",
        -1,
        snapshots[1][0]["posted_at"],
    ), invalid
    inputs = frames(module, spark, ([], [], []))
    for field, value in [("extra", 1), ("amount_minor", 1.0)]:
        from pyspark.sql import functions as F

        changed = inputs[0].withColumn(field, F.lit(value))
        try:
            module.reconcile(changed, *inputs[1:])
        except ValueError as error:
            assert "schema" in str(error), error
        else:
            raise AssertionError(f"invalid schema {field} was accepted")
    try:
        module.reconcile(*inputs, cutoff=module.CUTOFF.replace(tzinfo=dt.UTC))
    except ValueError as error:
        assert "cutoff" in str(error), error
    else:
        raise AssertionError("timezone-aware cutoff was accepted")
    # Test the real bound cheaply without constructing an oversized default fixture.
    module.MAX_ROWS = 2
    try:
        module.reconcile(*frames(module, spark, (snapshots[0], [], [])))
    except ValueError as error:
        assert "row input bound" in str(error), error
    else:
        raise AssertionError("source row bound was not enforced before joins")
    return {"quarantined_parent_ids": True, "exact_cutoff_microsecond": True, "reject_payload": True}


def empty_skew_check(spark):
    module = example()
    # A deferred result must still read its own DataFrames after a second invocation.
    deferred = module.run(spark)
    empty_frames = module.reconcile(*frames(module, spark, ([], [], [])))
    empty = collected(empty_frames)
    assert all(not rows for rows in empty.values()), empty
    for name, frame in empty_frames.items():
        expected = {
            field.split()[0]: field.split()[1].lower() for field in module.OUTPUT_SCHEMAS[name].split(",")
        }
        expected = {key: "bigint" if kind == "long" else kind for key, kind in expected.items()}
        actual = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
        assert actual == expected, f"empty {name} lost its declared schema: {actual}"
    assert collected(deferred) == module.reference_rows(*module.fixture()), (
        "later invocation replaced deferred input"
    )
    stamp = module.CUTOFF - dt.timedelta(hours=1)
    base = {"currency": "JPY", "posted_at": stamp, "received_at": stamp}
    n = 1001
    orders = [base | {"order_id": "skew", "amount_minor": n}]
    captures = [base | {"capture_id": f"c-{i:04}", "order_id": "skew", "amount_minor": 1} for i in range(n)]
    refunds = [
        base | {"refund_id": f"r-{i:04}", "capture_id": f"c-{i:04}", "amount_minor": 1}
        for i in range(0, n, 2)
    ]
    captures += [dict(captures[0])] * 101
    refunds += [dict(refunds[0])] * 103
    actual = compare(module, spark, (orders, captures, refunds))
    row = actual["orders"][0]
    assert (
        row["captured_minor"],
        row["refunded_minor"],
        row["net_minor"],
        row["capture_count"],
        row["refund_count"],
    ) == (1001, 501, 500, 1001, 501), row
    assert not actual["exceptions"], actual["exceptions"]
    return {"typed_empty_tables": 4, "skew_unique_captures": n, "skew_duplicate_deliveries": 204}


def check(spark, engine):
    case = os.environ["SAIL_EXAMPLES_PAYMENTS_CASE"]
    return {"contract": contract_check, "invalid": invalid_check, "empty_skew": empty_skew_check}[case](spark)
