# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Reconstruct demand-planning features using only information available at each decision."""

import argparse
import datetime as dt
import json
from pathlib import Path

REQUEST_SCHEMA = (
    "request_id string, product_id string, history_start timestamp_ntz, "
    "decision_at timestamp_ntz, horizon_end timestamp_ntz"
)
ORDER_SCHEMA = (
    "order_id string, revision bigint, product_id string, event_at timestamp_ntz, "
    "available_at timestamp_ntz, units bigint"
)
PRODUCT_SCHEMA = (
    "interval_id string, revision bigint, product_id string, valid_from timestamp_ntz, "
    "valid_to timestamp_ntz, available_at timestamp_ntz, category string, lead_days bigint"
)
COVERAGE_SCHEMA = "product_id string, observed_through timestamp_ntz, available_at timestamp_ntz"
FIELDS = {
    "requests": ("request_id", "product_id", "history_start", "decision_at", "horizon_end"),
    "orders": ("order_id", "revision", "product_id", "event_at", "available_at", "units"),
    "products": (
        "interval_id",
        "revision",
        "product_id",
        "valid_from",
        "valid_to",
        "available_at",
        "category",
        "lead_days",
    ),
    "coverage": ("product_id", "observed_through", "available_at"),
}
TIME_FIELDS = {
    "history_start",
    "decision_at",
    "horizon_end",
    "event_at",
    "available_at",
    "valid_from",
    "valid_to",
    "observed_through",
}


def utc_time(value):
    """Use UTC wall-clock values with TIMESTAMP_NTZ, independent of session time zone."""
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value)
        if value.tzinfo is None:
            raise ValueError("JSON timestamps require a UTC offset or Z")
    if not isinstance(value, dt.datetime):
        raise ValueError("timestamp required")  # noqa: TRY004 - all input-contract failures are ValueError
    if value.tzinfo is not None:
        if value.utcoffset() != dt.timedelta(0):
            raise ValueError("timestamps must use UTC")
        value = value.replace(tzinfo=None)
    return value


def normalize_inputs(requests, orders, products, coverage, evaluation_at):
    """Validate a bounded input bundle before constructing any query."""
    output = {}
    for name, rows in zip(FIELDS, (requests, orders, products, coverage)):
        if not isinstance(rows, list) or len(rows) > (128 if name == "requests" else 5000):
            raise ValueError(f"{name}: expected a bounded list")
        normalized, seen, versions = [], set(), {}
        for index, source in enumerate(rows):
            try:
                if not isinstance(source, dict) or set(source) != set(FIELDS[name]):
                    raise ValueError("unexpected or missing fields")
                row = dict(source)
                for field, value in row.items():
                    if field in TIME_FIELDS:
                        row[field] = utc_time(value)
                    elif field in {"revision", "units", "lead_days"}:
                        lower = 1 if field == "revision" else 0
                        upper = 365 if field == "lead_days" else 1_000_000_000
                        if type(value) is not int or not lower <= value <= upper:
                            raise ValueError(f"invalid {field}")
                    elif not isinstance(value, str) or not value.strip() or len(value) > 200:
                        raise ValueError(f"invalid {field}")
                if name == "requests":
                    if not row["history_start"] < row["decision_at"] < row["horizon_end"]:
                        raise ValueError("invalid request interval")
                    key = row["request_id"]
                    if key in seen:
                        raise ValueError("duplicate request_id")
                else:
                    key = tuple(row[field] for field in FIELDS[name])
                    if name == "orders" and row["available_at"] < row["event_at"]:
                        raise ValueError("order available before event")
                    if name == "products" and row["valid_from"] >= row["valid_to"]:
                        raise ValueError("invalid product interval")
                    if name == "coverage" and row["observed_through"] > row["available_at"]:
                        raise ValueError("coverage available before observed_through")
                    if name in {"orders", "products"}:
                        identity = (row["order_id" if name == "orders" else "interval_id"], row["revision"])
                        if identity in versions and versions[identity] != row:
                            raise ValueError("conflicting source revision")
                        versions[identity] = row
                    if key in seen:
                        continue
                seen.add(key)
                normalized.append(row)
            except (ValueError, TypeError, OverflowError) as error:
                raise ValueError(f"{name}[{index}]: {error}; input={source!r}") from error
        output[name] = normalized
    output["evaluation_at"] = utc_time(evaluation_at)
    return output


def build_features(spark, requests, orders, products, coverage, evaluation_at):
    """Return deferred DataFrames bound to these inputs, with no shared temporary views."""
    data = normalize_inputs(requests, orders, products, coverage, evaluation_at)

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    requests = spark.createDataFrame(data["requests"], REQUEST_SCHEMA)
    orders = spark.createDataFrame(data["orders"], ORDER_SCHEMA)
    products = spark.createDataFrame(data["products"], PRODUCT_SCHEMA)
    coverage = spark.createDataFrame(data["coverage"], COVERAGE_SCHEMA)
    # A Python datetime literal is TIMESTAMP, so compare UTC wall-clock NTZ
    # values using an explicitly typed literal instead of a session-zone cast.
    evaluation_at = F.lit(data["evaluation_at"].isoformat()).cast("timestamp_ntz")

    def latest(source, name, key, cutoff, ignore_availability=False):
        r, s = requests.alias("r"), source.alias("s")
        eligible = r.join(s, F.lit(True) if ignore_availability else F.col("s.available_at") <= cutoff)
        selected = eligible.select("r.request_id", *[F.col(f"s.{field}") for field in FIELDS[name]])
        window = Window.partitionBy("request_id", key).orderBy(F.col("revision").desc())
        return (
            selected.withColumn("position", F.row_number().over(window))
            .filter("position = 1")
            .drop("position")
        )

    # Rank all eligible revisions first. Filtering an older revision into a window
    # before ranking could resurrect a corrected or cancelled order.
    product_versions = latest(products, "products", "interval_id", F.col("r.decision_at"))
    candidates = requests.alias("r").join(
        product_versions.alias("p"),
        (F.col("r.request_id") == F.col("p.request_id"))
        & (F.col("r.product_id") == F.col("p.product_id"))
        & (F.col("p.valid_from") <= F.col("r.decision_at"))
        & (F.col("r.decision_at") < F.col("p.valid_to")),
    )
    dimensions = candidates.groupBy(F.col("r.request_id").alias("request_id")).agg(
        F.count("p.interval_id").alias("interval_count"),
        F.min("p.category").alias("category"),
        F.min("p.lead_days").alias("lead_days"),
    )
    classified = requests.join(dimensions, "request_id", "left").fillna({"interval_count": 0})
    accepted = classified.filter("interval_count = 1").drop("interval_count")
    rejected = (
        classified.filter("interval_count != 1")
        .select(
            *FIELDS["requests"],
            F.when(F.col("interval_count") == 0, "missing_product_interval")
            .otherwise("ambiguous_product_intervals")
            .alias("reason"),
        )
        .orderBy("request_id")
    )

    def totals(selected, future=False):
        joined = requests.alias("r").join(
            selected.alias("o"),
            (F.col("r.request_id") == F.col("o.request_id"))
            & (F.col("r.product_id") == F.col("o.product_id")),
        )
        lower = F.col("r.decision_at") if future else F.col("r.history_start")
        upper = F.col("r.horizon_end") if future else F.col("r.decision_at")
        return (
            joined.filter(
                (F.col("o.event_at") >= lower) & (F.col("o.event_at") < upper) & (F.col("o.units") > 0)
            )
            .groupBy(F.col("r.request_id").alias("request_id"))
            .agg(
                F.count("o.order_id").alias("order_count"),
                F.sum("o.units").alias("units"),
                F.max("o.event_at").alias("last_order_at"),
            )
        )

    def feature_table(selected):
        return (
            accepted.join(totals(selected), "request_id", "left")
            .fillna({"order_count": 0, "units": 0})
            .select(
                "request_id",
                "product_id",
                "decision_at",
                "category",
                "lead_days",
                "order_count",
                "units",
                "last_order_at",
            )
            .orderBy("request_id")
        )

    known = latest(orders, "orders", "order_id", F.col("r.decision_at"))
    naive = latest(orders, "orders", "order_id", evaluation_at, ignore_availability=True)
    # Outcomes are evaluated at a separate, explicit cutoff and never joined into features.
    evaluated = latest(orders, "orders", "order_id", evaluation_at)
    watermarks = (
        coverage.filter(F.col("available_at") <= evaluation_at)
        .groupBy("product_id")
        .agg(F.max("observed_through").alias("observed_through"))
    )
    outcomes = (
        accepted.join(watermarks, "product_id", "left")
        .join(
            totals(evaluated, future=True).select("request_id", F.col("units").alias("target_units")),
            "request_id",
            "left",
        )
        .withColumn(
            "complete",
            (F.col("horizon_end") <= evaluation_at) & (F.col("observed_through") >= F.col("horizon_end")),
        )
        .select(
            "request_id",
            "horizon_end",
            F.when(F.col("complete"), F.coalesce("target_units", F.lit(0)))
            .cast("long")
            .alias("target_units"),
            F.when(F.col("complete"), "observed").otherwise("unavailable").alias("outcome_status"),
        )
        .orderBy("request_id")
    )
    return {
        "features": feature_table(known),
        "naive_features": feature_table(naive),
        "outcomes": outcomes,
        "rejected": rejected,
    }


def demo_inputs():
    def time(day):
        return dt.datetime(2026, 1, day)

    requests = [
        {
            "request_id": "early",
            "product_id": "tea",
            "history_start": time(2),
            "decision_at": time(5),
            "horizon_end": time(7),
        },
        {
            "request_id": "later",
            "product_id": "tea",
            "history_start": time(5),
            "decision_at": time(8),
            "horizon_end": time(10),
        },
        {
            "request_id": "zero",
            "product_id": "coffee",
            "history_start": time(2),
            "decision_at": time(5),
            "horizon_end": time(7),
        },
        {
            "request_id": "missing",
            "product_id": "cocoa",
            "history_start": time(2),
            "decision_at": time(5),
            "horizon_end": time(7),
        },
        {
            "request_id": "overlap",
            "product_id": "juice",
            "history_start": time(2),
            "decision_at": time(5),
            "horizon_end": time(7),
        },
    ]
    orders = []
    for order_id, revision, event, available, units in [
        ("left", 1, 2, 2, 2),
        ("old", 1, 1, 1, 100),
        ("late-arrival", 1, 3, 6, 10),
        ("corrected", 1, 4, 4, 4),
        ("corrected", 2, 4, 6, 7),
        ("moved", 1, 4, 4, 3),
        ("moved", 2, 1, 4, 3),
        ("tie", 1, 4, 4, 1),
        ("tie", 2, 4, 4, 6),
        ("at-decision", 1, 5, 5, 5),
        ("at-horizon", 1, 7, 7, 11),
        ("outcome-late", 1, 6, 9, 2),
    ]:
        orders.append(
            {
                "order_id": order_id,
                "revision": revision,
                "product_id": "tea",
                "event_at": time(event),
                "available_at": time(available),
                "units": units,
            }
        )
    orders.append(dict(orders[0]))
    products = []
    for interval, revision, product, start, end, available, category, lead in [
        ("tea-old", 1, "tea", 1, 6, 1, "pantry", 3),
        ("tea-old", 2, "tea", 1, 6, 6, "pantry", 4),
        ("tea-new", 1, "tea", 6, 31, 6, "pantry", 2),
        ("coffee", 1, "coffee", 1, 31, 1, "pantry", 5),
        ("juice-a", 1, "juice", 1, 8, 1, "chilled", 1),
        ("juice-b", 1, "juice", 4, 31, 1, "chilled", 2),
    ]:
        products.append(
            {
                "interval_id": interval,
                "revision": revision,
                "product_id": product,
                "valid_from": time(start),
                "valid_to": time(end),
                "available_at": time(available),
                "category": category,
                "lead_days": lead,
            }
        )
    return {
        "requests": requests,
        "orders": orders,
        "products": products,
        "coverage": [{"product_id": "tea", "observed_through": time(8), "available_at": time(9)}],
        "evaluation_at": time(9),
    }


def python_reference(requests, orders, products, coverage, evaluation_at):
    """Independent per-request enumeration, without DataFrame or query helpers."""
    output = {name: [] for name in ("features", "naive_features", "outcomes", "rejected")}
    for request in sorted(requests, key=lambda item: item["request_id"]):
        decision = request["decision_at"]
        dimensions = []
        for interval in {row["interval_id"] for row in products}:
            versions = [
                row for row in products if row["interval_id"] == interval and row["available_at"] <= decision
            ]
            if versions:
                chosen = max(versions, key=lambda row: row["revision"])
                if (
                    chosen["product_id"] == request["product_id"]
                    and chosen["valid_from"] <= decision < chosen["valid_to"]
                ):
                    dimensions.append(chosen)
        if len(dimensions) != 1:
            output["rejected"].append(
                dict(
                    request,
                    reason="missing_product_interval" if not dimensions else "ambiguous_product_intervals",
                )
            )
            continue
        dimension = dimensions[0]
        for name in ("features", "naive_features", "outcomes"):
            chosen_orders = []
            for order_id in {row["order_id"] for row in orders}:
                versions = [
                    row
                    for row in orders
                    if row["order_id"] == order_id
                    and (
                        name == "naive_features"
                        or row["available_at"] <= (evaluation_at if name == "outcomes" else decision)
                    )
                ]
                if not versions:
                    continue
                chosen = max(versions, key=lambda row: row["revision"])
                lower = decision if name == "outcomes" else request["history_start"]
                upper = request["horizon_end"] if name == "outcomes" else decision
                if (
                    chosen["product_id"] == request["product_id"]
                    and lower <= chosen["event_at"] < upper
                    and chosen["units"] > 0
                ):
                    chosen_orders.append(chosen)
            if name == "outcomes":
                complete = request["horizon_end"] <= evaluation_at and any(
                    row["product_id"] == request["product_id"]
                    and row["available_at"] <= evaluation_at
                    and row["observed_through"] >= request["horizon_end"]
                    for row in coverage
                )
                output[name].append(
                    {
                        "request_id": request["request_id"],
                        "horizon_end": request["horizon_end"],
                        "target_units": sum(row["units"] for row in chosen_orders) if complete else None,
                        "outcome_status": "observed" if complete else "unavailable",
                    }
                )
            else:
                output[name].append(
                    {
                        "request_id": request["request_id"],
                        "product_id": request["product_id"],
                        "decision_at": decision,
                        "category": dimension["category"],
                        "lead_days": dimension["lead_days"],
                        "order_count": len(chosen_orders),
                        "units": sum(row["units"] for row in chosen_orders),
                        "last_order_at": max((row["event_at"] for row in chosen_orders), default=None),
                    }
                )
    return output


def run(spark):
    return build_features(spark, **demo_inputs())


def reference():
    import pyarrow as pa

    feature_schema = pa.schema(
        [
            ("request_id", pa.string()),
            ("product_id", pa.string()),
            ("decision_at", pa.timestamp("us")),
            ("category", pa.string()),
            ("lead_days", pa.int64()),
            ("order_count", pa.int64()),
            ("units", pa.int64()),
            ("last_order_at", pa.timestamp("us")),
        ]
    )
    schemas = {
        "features": feature_schema,
        "naive_features": feature_schema,
        "outcomes": pa.schema(
            [
                ("request_id", pa.string()),
                ("horizon_end", pa.timestamp("us")),
                ("target_units", pa.int64()),
                ("outcome_status", pa.string()),
            ]
        ),
        "rejected": pa.schema(
            [
                ("request_id", pa.string()),
                ("product_id", pa.string()),
                ("history_start", pa.timestamp("us")),
                ("decision_at", pa.timestamp("us")),
                ("horizon_end", pa.timestamp("us")),
                ("reason", pa.string()),
            ]
        ),
    }
    return {
        name: pa.Table.from_pylist(rows, schema=schemas[name])
        for name, rows in python_reference(**demo_inputs()).items()
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, help="JSON object with requests, orders, products, coverage, evaluation_at"
    )
    args = parser.parse_args()
    data = normalize_inputs(**json.loads(args.input.read_text())) if args.input else demo_inputs()
    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        for name, table in build_features(spark, **data).items():
            print(name)
            table.show(128, truncate=False)
    finally:
        spark.stop()
        server.stop()
