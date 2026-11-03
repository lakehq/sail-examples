# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Reconcile observed orders, captures and refunds without multiplying money."""

import datetime as dt
import re
from collections import defaultdict

CUTOFF = dt.datetime(2026, 1, 2)
MAX_ROWS = 20_000
MAX_AMOUNT = 10**12
COMMON = "currency STRING, amount_minor LONG, posted_at TIMESTAMP_NTZ, received_at TIMESTAMP_NTZ"
INPUT_SCHEMAS = {
    "order": "order_id STRING, " + COMMON,
    "capture": "capture_id STRING, order_id STRING, " + COMMON,
    "refund": "refund_id STRING, capture_id STRING, " + COMMON,
}
FIELDS = ["source", "record_id", "parent_id", "currency", "amount_minor", "posted_at", "received_at"]
BASE_SCHEMA = "source STRING, record_id STRING, parent_id STRING, " + COMMON
OUTPUT_SCHEMAS = {
    "orders": "order_id STRING, currency STRING, expected_minor LONG, captured_minor LONG, refunded_minor LONG, net_minor LONG, balance_minor LONG, capture_count LONG, refund_count LONG, status STRING",
    "ledger": BASE_SCHEMA + ", deliveries LONG, status STRING",
    "exceptions": BASE_SCHEMA + ", deliveries LONG, reason STRING",
    "accounting": "source STRING, currency STRING, records LONG, deliveries LONG, total_minor LONG, attributed_minor LONG, unmatched_minor LONG",
}


def fixture():
    """Small source snapshots, including deliveries that must not affect money."""
    posted, received = dt.datetime(2026, 1, 1, 10), dt.datetime(2026, 1, 1, 11)

    def row(identifier, amount, currency="USD", **links):
        return {
            **identifier,
            **links,
            "currency": currency,
            "amount_minor": amount,
            "posted_at": posted,
            "received_at": received,
        }

    orders = [
        row({"order_id": key}, value, currency)
        for key, value, currency in [
            ("paid", 1000, "USD"),
            ("partial", 1000, "USD"),
            ("refund", 1000, "USD"),
            ("over", 500, "USD"),
            ("waiting", 700, "USD"),
            ("yen", 1000, "JPY"),
        ]
    ]
    captures = [
        row({"capture_id": key}, amount, currency, order_id=order)
        for key, order, amount, currency in [
            ("split-a", "paid", 600, "USD"),
            ("split-b", "paid", 400, "USD"),
            ("small", "partial", 300, "USD"),
            ("ref", "refund", 1000, "USD"),
            ("over-cap", "over", 500, "USD"),
            ("yen-cap", "yen", 1000, "JPY"),
            ("wrong-currency", "paid", 50, "EUR"),
            ("orphan", "absent", 80, "USD"),
            ("conflict", "paid", 90, "USD"),
            ("conflict", "paid", 91, "USD"),
        ]
    ]
    captures += [
        dict(captures[0]),
        {**row({"capture_id": "late"}, 700, order_id="waiting"), "received_at": CUTOFF},
    ]
    refunds = [
        row({"refund_id": key}, amount, currency, capture_id=capture)
        for key, capture, amount, currency in [
            ("part-refund", "ref", 200, "USD"),
            ("too-much", "over-cap", 600, "USD"),
            ("lost", "missing", 25, "USD"),
            ("wrong-refund-currency", "ref", 5, "EUR"),
        ]
    ]
    refunds += [dict(refunds[0])]
    return orders, captures, refunds


def _stage(frame, source, cutoff):
    from pyspark.sql import functions as F

    expected = {
        part.strip().split()[0]: part.strip().split()[1].lower() for part in INPUT_SCHEMAS[source].split(",")
    }
    actual = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    expected = {name: "bigint" if kind == "long" else kind for name, kind in expected.items()}
    if actual != expected or len(frame.columns) != len(expected):
        raise ValueError(f"{source} requires exactly the declared input schema")
    if frame.limit(MAX_ROWS + 1).count() > MAX_ROWS:
        raise ValueError(f"{source} exceeds the {MAX_ROWS}-row input bound")
    key, parent = {
        "order": ("order_id", None),
        "capture": ("capture_id", "order_id"),
        "refund": ("refund_id", "capture_id"),
    }[source]
    rows = frame.select(
        F.lit(source).alias("source"),
        F.col(key).alias("record_id"),
        (F.col(parent) if parent else F.lit(None).cast("string")).alias("parent_id"),
        "currency",
        "amount_minor",
        "posted_at",
        "received_at",
    )
    invalid_key = (
        F.col("record_id").isNull()
        | (F.length("record_id") == 0)
        | F.col("record_id").rlike("[^A-Za-z0-9._:-]")
    )
    if parent:
        invalid_key = (
            invalid_key
            | F.col("parent_id").isNull()
            | (F.length("parent_id") == 0)
            | F.col("parent_id").rlike("[^A-Za-z0-9._:-]")
        )
    boundary = F.lit(cutoff.isoformat(sep=" ")).cast("timestamp_ntz")
    reason = (
        F.when(invalid_key, "invalid_id")
        .when(
            F.col("currency").isNull() | (F.length("currency") != 3) | F.col("currency").rlike("[^A-Z]"),
            "invalid_currency",
        )
        .when(
            F.col("amount_minor").isNull()
            | (F.col("amount_minor") <= 0)
            | (F.col("amount_minor") > MAX_AMOUNT),
            "invalid_amount",
        )
        .when(F.col("posted_at").isNull() | F.col("received_at").isNull(), "invalid_timestamp")
        .when(F.col("received_at") >= boundary, "not_yet_received")
        .when(F.col("posted_at") >= boundary, "not_yet_posted")
    )
    rows = rows.withColumn("reason", reason)
    rejected = (
        rows.filter("reason IS NOT NULL").groupBy(*FIELDS, "reason").agg(F.count("*").alias("deliveries"))
    )
    variants = (
        rows.filter("reason IS NULL")
        .groupBy(*FIELDS[:-1])
        .agg(F.min("received_at").alias("received_at"), F.count("*").alias("deliveries"))
    )
    counts = variants.groupBy("record_id").agg(F.count("*").alias("variants"))
    variants = variants.join(counts, "record_id")
    canonical = variants.filter("variants = 1").select(*FIELDS, "deliveries")
    conflicts = (
        variants.filter("variants > 1")
        .select(*FIELDS, "deliveries")
        .withColumn("reason", F.lit("conflicting_id"))
    )
    return canonical, rejected.unionByName(conflicts)


def _order_status(captured, refunded, expected):
    from pyspark.sql import functions as F

    return (
        F.when(captured == 0, "unpaid")
        .when(refunded > captured, "over_refunded")
        .when(refunded == captured, "fully_refunded")
        .when(refunded > 0, "partially_refunded")
        .when(captured < expected, "under_captured")
        .when(captured > expected, "over_captured")
        .otherwise("paid")
    )


def reconcile(orders, captures, refunds, *, cutoff=CUTOFF):
    """Return business state and separate delivery/error accounting as of cutoff."""
    from pyspark.sql import functions as F

    if not isinstance(cutoff, dt.datetime) or cutoff.tzinfo is not None:
        raise ValueError("cutoff must be a timezone-naive UTC datetime")
    order, order_errors = _stage(orders, "order", cutoff)
    capture, capture_errors = _stage(captures, "capture", cutoff)
    refund, refund_errors = _stage(refunds, "refund", cutoff)
    order_keys = order.select(
        F.col("record_id").alias("order_key"), F.col("currency").alias("order_currency")
    )
    capture = capture.join(order_keys, capture.parent_id == order_keys.order_key, "left").withColumn(
        "status",
        F.when(F.col("order_key").isNull(), "missing_order")
        .when(F.col("currency") != F.col("order_currency"), "currency_mismatch")
        .otherwise("attributed"),
    )
    links = capture.select(
        F.col("record_id").alias("capture_key"),
        F.col("parent_id").alias("order_key"),
        F.col("currency").alias("capture_currency"),
        F.col("posted_at").alias("capture_posted"),
        F.col("status").alias("capture_status"),
    )
    refund = refund.join(links, refund.parent_id == links.capture_key, "left").withColumn(
        "status",
        F.when(F.col("capture_key").isNull(), "missing_capture")
        .when(F.col("currency") != F.col("capture_currency"), "currency_mismatch")
        .when(F.col("posted_at") < F.col("capture_posted"), "refund_before_capture")
        .when(F.col("capture_status") != "attributed", "unmatched_capture_order")
        .otherwise("attributed"),
    )
    # Both joins have at most one right row per key. Check actual cardinality too.
    if (
        capture.count() != capture.select("record_id").distinct().count()
        or refund.count() != refund.select("record_id").distinct().count()
    ):
        raise AssertionError("a parent join multiplied logical transactions")
    cap_totals = (
        capture.filter("status = 'attributed'")
        .groupBy("parent_id")
        .agg(F.sum("amount_minor").alias("captured_minor"), F.count("*").alias("capture_count"))
        .withColumnRenamed("parent_id", "capture_order_id")
    )
    ref_totals = (
        refund.filter("status = 'attributed'")
        .groupBy("order_key")
        .agg(F.sum("amount_minor").alias("refunded_minor"), F.count("*").alias("refund_count"))
    )
    result = order.join(cap_totals, order.record_id == cap_totals.capture_order_id, "left")
    result = result.join(ref_totals, result.record_id == ref_totals.order_key, "left").fillna(
        0, subset=["captured_minor", "refunded_minor", "capture_count", "refund_count"]
    )
    result = result.select(
        F.col("record_id").alias("order_id"),
        "currency",
        F.col("amount_minor").alias("expected_minor"),
        "captured_minor",
        "refunded_minor",
        (F.col("captured_minor") - F.col("refunded_minor")).alias("net_minor"),
        (F.col("amount_minor") - F.col("captured_minor") + F.col("refunded_minor")).alias("balance_minor"),
        "capture_count",
        "refund_count",
        _order_status(F.col("captured_minor"), F.col("refunded_minor"), F.col("amount_minor")).alias(
            "status"
        ),
    )
    if result.count() != order.count():
        raise AssertionError("order summary changed parent cardinality")
    ledger = (
        order.withColumn("status", F.lit("active_order"))
        .unionByName(capture.select(*FIELDS, "deliveries", "status"))
        .unionByName(refund.select(*FIELDS, "deliveries", "status"))
    )
    exceptions = (
        order_errors.unionByName(capture_errors)
        .unionByName(refund_errors)
        .unionByName(
            ledger.filter("status NOT IN ('active_order', 'attributed')").withColumnRenamed(
                "status", "reason"
            )
        )
    )
    by_capture = (
        refund.filter("status = 'attributed'")
        .groupBy("parent_id")
        .agg(F.sum("amount_minor").alias("refund_sum"))
        .withColumnRenamed("parent_id", "refund_capture_id")
    )
    excess = (
        capture.join(by_capture, capture.record_id == by_capture.refund_capture_id, "inner")
        .filter("status = 'attributed' AND refund_sum > amount_minor")
        .select(*[capture[name] for name in FIELDS], capture.deliveries)
    )
    exceptions = exceptions.unionByName(excess.withColumn("reason", F.lit("over_refunded")))
    accounting = (
        ledger.filter("source != 'order'")
        .groupBy("source", "currency")
        .agg(
            F.count("*").alias("records"),
            F.sum("deliveries").alias("deliveries"),
            F.sum("amount_minor").alias("total_minor"),
            F.sum(F.when(F.col("status") == "attributed", F.col("amount_minor")).otherwise(0)).alias(
                "attributed_minor"
            ),
            F.sum(F.when(F.col("status") != "attributed", F.col("amount_minor")).otherwise(0)).alias(
                "unmatched_minor"
            ),
        )
    )
    return {
        "orders": result.orderBy("order_id"),
        "ledger": ledger.orderBy("source", "record_id"),
        "exceptions": exceptions.select(*FIELDS, "deliveries", "reason").orderBy(
            "source",
            "record_id",
            "reason",
            "parent_id",
            "currency",
            "amount_minor",
            "posted_at",
            "received_at",
        ),
        "accounting": accounting.orderBy("source", "currency"),
    }


def reference_rows(orders, captures, refunds, *, cutoff=CUTOFF):
    """Independent Python grouping and ledger arithmetic, without engine helpers."""
    canonical, errors = {}, []
    for source, rows, key, parent in [
        ("order", orders, "order_id", None),
        ("capture", captures, "capture_id", "order_id"),
        ("refund", refunds, "refund_id", "capture_id"),
    ]:
        groups, bad = defaultdict(list), defaultdict(int)
        for raw in rows:
            row = {
                "source": source,
                "record_id": raw[key],
                "parent_id": raw[parent] if parent else None,
                **{name: raw[name] for name in ("currency", "amount_minor", "posted_at", "received_at")},
            }
            ids = [row["record_id"]] + ([row["parent_id"]] if parent else [])
            if any(
                not isinstance(value, str) or not re.fullmatch("[A-Za-z0-9._:-]+", value) for value in ids
            ):
                reason = "invalid_id"
            elif not isinstance(row["currency"], str) or not re.fullmatch("[A-Z]{3}", row["currency"]):
                reason = "invalid_currency"
            elif type(row["amount_minor"]) is not int or not 0 < row["amount_minor"] <= 10**12:
                reason = "invalid_amount"
            elif row["posted_at"] is None or row["received_at"] is None:
                reason = "invalid_timestamp"
            elif row["received_at"] >= cutoff:
                reason = "not_yet_received"
            elif row["posted_at"] >= cutoff:
                reason = "not_yet_posted"
            else:
                reason = None
            if reason:
                bad[tuple(row[name] for name in FIELDS) + (reason,)] += 1
            else:
                groups[row["record_id"]].append(row)
        selected = {}
        for key, deliveries in groups.items():
            variants = defaultdict(list)
            for row in deliveries:
                variants[tuple(row[name] for name in FIELDS[:-1])].append(row)
            for payload, occurrences in variants.items():
                row = dict(zip(FIELDS[:-1], payload)) | {
                    "received_at": min(item["received_at"] for item in occurrences),
                    "deliveries": len(occurrences),
                }
                if len(variants) == 1:
                    selected[key] = row
                else:
                    errors.append(row | {"reason": "conflicting_id"})
        errors += [
            dict(zip(FIELDS, key[:-1])) | {"reason": key[-1], "deliveries": count}
            for key, count in bad.items()
        ]
        canonical[source] = selected
    orders = canonical["order"]
    for row in canonical["capture"].values():
        parent = orders.get(row["parent_id"])
        row["status"] = (
            "missing_order"
            if parent is None
            else "currency_mismatch"
            if parent["currency"] != row["currency"]
            else "attributed"
        )
    for row in canonical["refund"].values():
        parent = canonical["capture"].get(row["parent_id"])
        row["status"] = (
            "missing_capture"
            if parent is None
            else "currency_mismatch"
            if parent["currency"] != row["currency"]
            else "refund_before_capture"
            if row["posted_at"] < parent["posted_at"]
            else "unmatched_capture_order"
            if parent["status"] != "attributed"
            else "attributed"
        )
    for row in orders.values():
        row["status"] = "active_order"
    ledger = [row for group in canonical.values() for row in group.values()]
    errors += [
        {key: value for key, value in row.items() if key != "status"} | {"reason": row["status"]}
        for row in ledger
        if row["status"] not in {"active_order", "attributed"}
    ]
    results = []
    for key, order in orders.items():
        caps = [
            row
            for row in canonical["capture"].values()
            if row["parent_id"] == key and row["status"] == "attributed"
        ]
        refs = [
            row
            for row in canonical["refund"].values()
            if row["status"] == "attributed" and canonical["capture"][row["parent_id"]]["parent_id"] == key
        ]
        captured, refunded = (
            sum(row["amount_minor"] for row in caps),
            sum(row["amount_minor"] for row in refs),
        )
        expected = order["amount_minor"]
        status = (
            "unpaid"
            if captured == 0
            else "over_refunded"
            if refunded > captured
            else "fully_refunded"
            if refunded == captured
            else "partially_refunded"
            if refunded
            else "under_captured"
            if captured < expected
            else "over_captured"
            if captured > expected
            else "paid"
        )
        results.append(
            {
                "order_id": key,
                "currency": order["currency"],
                "expected_minor": expected,
                "captured_minor": captured,
                "refunded_minor": refunded,
                "net_minor": captured - refunded,
                "balance_minor": expected - captured + refunded,
                "capture_count": len(caps),
                "refund_count": len(refs),
                "status": status,
            }
        )
        for capture in caps:
            if (
                sum(row["amount_minor"] for row in refs if row["parent_id"] == capture["record_id"])
                > capture["amount_minor"]
            ):
                errors.append(
                    {name: capture[name] for name in [*FIELDS, "deliveries"]} | {"reason": "over_refunded"}
                )
    accounting = defaultdict(
        lambda: {"records": 0, "deliveries": 0, "total_minor": 0, "attributed_minor": 0, "unmatched_minor": 0}
    )
    for row in ledger:
        if row["source"] == "order":
            continue
        total = accounting[row["source"], row["currency"]]
        total["records"] += 1
        total["deliveries"] += row["deliveries"]
        total["total_minor"] += row["amount_minor"]
        total["attributed_minor" if row["status"] == "attributed" else "unmatched_minor"] += row[
            "amount_minor"
        ]
    return {
        "orders": sorted(results, key=lambda row: row["order_id"]),
        "ledger": sorted(ledger, key=lambda row: (row["source"], row["record_id"])),
        "exceptions": sorted(
            errors,
            key=lambda row: tuple(
                (row[name] is not None, row[name])
                for name in (
                    "source",
                    "record_id",
                    "reason",
                    "parent_id",
                    "currency",
                    "amount_minor",
                    "posted_at",
                    "received_at",
                )
            ),
        ),
        "accounting": [
            {"source": source, "currency": currency, **values}
            for (source, currency), values in sorted(accounting.items())
        ],
    }


def run(spark):
    return reconcile(
        *[
            spark.createDataFrame(rows, INPUT_SCHEMAS[source])
            for source, rows in zip(INPUT_SCHEMAS, fixture(), strict=True)
        ]
    )


def reference():
    import pyarrow as pa

    types = {"STRING": pa.string(), "LONG": pa.int64(), "TIMESTAMP_NTZ": pa.timestamp("us")}
    rows = reference_rows(*fixture())
    return {
        name: pa.Table.from_pylist(
            rows[name],
            schema=pa.schema(
                [(field.strip().split()[0], types[field.strip().split()[1]]) for field in schema.split(",")]
            ),
        )
        for name, schema in OUTPUT_SCHEMAS.items()
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start()
    spark = None
    try:
        spark = SparkSession.builder.remote(f"sc://localhost:{server.listening_address[1]}").getOrCreate()
        for name, frame in run(spark).items():
            print(name)
            frame.show(truncate=False)
    finally:
        if spark is not None:
            spark.stop()
        server.stop()
