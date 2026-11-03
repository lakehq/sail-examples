# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Repair daily reports after versioned order replacements, including interrupted runs."""

import argparse
import atexit
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from pathlib import Path

STATE_COLUMNS = ["order_id", "source_version", "status", "store_id", "business_date", "amount_minor"]
STATE_SCHEMA = (
    "order_id string, source_version long, status string, store_id string, "
    "business_date string, amount_minor long"
)
REPORT_SCHEMA = "store_id string, business_date string, order_count long, amount_minor long"
INPUT_FIELDS = set(STATE_COLUMNS) | {"operation"}
MAX_BATCH = 512
MAX_ORDERS = 20_000
MAX_FILE_BYTES = 4_000_000
FAIL_POINTS = ("before_source", "after_source", "before_report", "after_report", "after_ack")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def key(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", value)


def normalize_batch(batch):
    if not isinstance(batch, dict) or set(batch) != {"batch_id", "records"} or not key(batch["batch_id"]):
        raise ValueError("batch requires batch_id and records")
    rows = batch["records"]
    if not isinstance(rows, list) or len(rows) > MAX_BATCH:
        raise ValueError(f"records must be a list of at most {MAX_BATCH} replacements")
    unique = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != INPUT_FIELDS:
            raise ValueError("replacement has unexpected or missing fields")
        if not key(row["order_id"]) or type(row["source_version"]) is not int:
            raise ValueError("invalid order identity or source_version")
        if not 1 <= row["source_version"] <= 1_000_000_000:
            raise ValueError("invalid source_version")
        if row["operation"] == "delete":
            if any(row[name] is not None for name in ("status", "store_id", "business_date", "amount_minor")):
                raise ValueError("delete requires null business fields")
        elif row["operation"] == "upsert":
            if row["status"] not in ("active", "cancelled") or not key(row["store_id"]):
                raise ValueError("invalid status or store_id")
            day = row["business_date"]
            if not isinstance(day, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day):
                raise ValueError("business_date requires YYYY-MM-DD")
            dt.date.fromisoformat(day)
            if type(row["amount_minor"]) is not int or not 0 <= row["amount_minor"] <= 1_000_000_000:
                raise ValueError("invalid amount_minor")
        else:
            raise ValueError("operation must be upsert or delete")
        identity = row["order_id"]
        if identity in unique and unique[identity] != row:
            raise ValueError("one replacement per order_id is required")
        unique[identity] = dict(row)
    return {"batch_id": batch["batch_id"], "records": [unique[k] for k in sorted(unique)]}


def source_row(record):
    return {
        name: ("deleted" if name == "status" and record["operation"] == "delete" else record[name])
        for name in STATE_COLUMNS
    }


def prepare(batch, old):
    """Compare bounded preimages without changing any authoritative state."""
    changes = []
    for record in batch["records"]:
        new = source_row(record)
        before = old.get(record["order_id"])
        if before and new["source_version"] <= before["source_version"]:
            if new["source_version"] == before["source_version"] and new != before:
                raise ValueError("equal source_version has a conflicting replacement")
            continue
        changes.append({"old": before, "new": new})
    return changes


def affected_groups(changes):
    return sorted(
        {
            (row["store_id"], row["business_date"])
            for change in changes
            for row in (change["old"], change["new"])
            if row is not None and row["status"] != "deleted"
        }
    )


def read_json(path):
    def pairs(items):
        result = {}
        for name, value in items:
            if name in result:
                raise ValueError("duplicate JSON field")
            result[name] = value
        return result

    if path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError("JSON file exceeds input limit")
    return json.loads(path.read_text(), object_pairs_hook=pairs)


def publish(path, value):
    """Atomic local publication for process interruption, without a power-loss promise."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_text(canonical(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextlib.contextmanager
def writer_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another writer holds this workspace") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def initialize(spark, root):
    identity_path = root / "workspace.json"
    if not identity_path.exists():
        if any(path.name != ".writer.lock" for path in root.iterdir()):
            raise ValueError("workspace identity is missing from existing state")
        publish(identity_path, {"format": 1, "workspace": uuid.uuid4().hex, "initialized": False})
    identity = read_json(identity_path)
    if (
        not isinstance(identity, dict)
        or set(identity) != {"format", "workspace", "initialized"}
        or type(identity["format"]) is not int
        or identity["format"] != 1
        or not isinstance(identity["workspace"], str)
        or not re.fullmatch("[0-9a-f]{32}", identity["workspace"])
        or type(identity["initialized"]) is not bool
    ):
        raise ValueError("invalid workspace identity")
    if identity["initialized"]:
        for label in ("orders", "daily"):
            log = root / label / "_delta_log"
            if not log.is_dir() or not any(
                re.fullmatch(r"[0-9]{20}\.json", path.name) for path in log.iterdir()
            ):
                raise ValueError("established workspace is missing a Delta transaction log")
    elif (root / "pending.json").exists() or (root / "receipts").exists():
        raise ValueError("unfinished workspace initialization contains batch state")
    names = {}
    for label, schema in (("orders", STATE_SCHEMA), ("daily", REPORT_SCHEMA)):
        path = root / label
        name = "repair_" + digest(str(path))[:20]
        location = str(path).replace("'", "''")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({schema}) USING delta LOCATION '{location}'")
        names[label] = name
    if not identity["initialized"]:
        publish(identity_path, {**identity, "initialized": True})
    return identity["workspace"], names


def existing(spark, root, ids):
    from pyspark.sql import functions as F

    if not ids:
        return {}
    rows = spark.read.format("delta").load(str(root / "orders")).where(F.col("order_id").isin(ids)).collect()
    result = {row.order_id: row.asDict() for row in rows}
    if len(result) != len(rows):
        raise ValueError("authoritative order keys are not unique")
    return result


def receipt_path(root, batch_id):
    return root / "receipts" / (digest(batch_id) + ".json")


def receipt(body):
    return {name: body[name] for name in ("format", "workspace", "batch_id", "payload_hash")}


def read_receipt(path):
    value = read_json(path)
    if (
        not isinstance(value, dict)
        or set(value) != {"format", "workspace", "batch_id", "payload_hash"}
        or type(value["format"]) is not int
        or value["format"] != 1
        or not key(value["batch_id"])
        or not isinstance(value["workspace"], str)
        or not re.fullmatch("[0-9a-f]{32}", value["workspace"])
        or not isinstance(value["payload_hash"], str)
        or not re.fullmatch("[0-9a-f]{64}", value["payload_hash"])
    ):
        raise ValueError("invalid batch acknowledgement")
    return value


def validate_pending(envelope, workspace):
    if not isinstance(envelope, dict) or set(envelope) != {"body", "sha256"}:
        raise ValueError("invalid pending envelope")
    body = envelope["body"]
    if (
        not isinstance(body, dict)
        or set(body) != {"format", "workspace", "batch_id", "payload_hash", "batch", "changes", "groups"}
        or digest(body) != envelope["sha256"]
    ):
        raise ValueError("invalid pending integrity")
    batch = normalize_batch(body["batch"])
    if (
        type(body["format"]) is not int
        or body["format"] != 1
        or body["workspace"] != workspace
        or batch != body["batch"]
        or body["batch_id"] != batch["batch_id"]
        or body["payload_hash"] != digest(batch)
        or not isinstance(body["changes"], list)
        or len(body["changes"]) > MAX_BATCH
    ):
        raise ValueError("pending batch or workspace mismatch")
    incoming = {row["order_id"]: source_row(row) for row in batch["records"]}
    seen = set()
    for change in body["changes"]:
        if not isinstance(change, dict) or set(change) != {"old", "new"}:
            raise ValueError("invalid pending replacement")
        new, old = change["new"], change["old"]
        if not isinstance(new, dict) or new != incoming.get(new.get("order_id")) or new["order_id"] in seen:
            raise ValueError("pending replacement does not match batch")
        seen.add(new["order_id"])
        if old is not None:
            if not isinstance(old, dict) or set(old) != set(STATE_COLUMNS):
                raise ValueError("invalid pending preimage")
            original = {**old, "operation": "delete" if old["status"] == "deleted" else "upsert"}
            if old["status"] == "deleted":
                original["status"] = None
            normalize_batch({"batch_id": "preimage", "records": [original]})
            if old["order_id"] != new["order_id"] or old["source_version"] >= new["source_version"]:
                raise ValueError("invalid pending source version")
    if body["groups"] != [list(group) for group in affected_groups(body["changes"])]:
        raise ValueError("pending affected groups do not match preimages")
    return body


def recover_locked(spark, root, workspace, names, interrupt=None):
    from pyspark.sql import functions as F

    pending = root / "pending.json"
    if not pending.exists():
        return
    body = validate_pending(read_json(pending), workspace)
    acknowledged = receipt_path(root, body["batch_id"])
    if acknowledged.exists():
        if read_receipt(acknowledged) != receipt(body):
            raise ValueError("acknowledgement does not match pending batch")
        pending.unlink()
        return

    def stop(point):
        if interrupt:
            interrupt(point)

    current = existing(spark, root, [row["new"]["order_id"] for row in body["changes"]])
    updates = []
    for change in body["changes"]:
        value = current.get(change["new"]["order_id"])
        if value == change["new"]:
            continue
        if value != change["old"]:
            raise ValueError("source no longer matches pending preimage or replacement")
        updates.append(change["new"])
    stop("before_source")
    if updates:
        view = names["orders"] + "_updates"
        spark.createDataFrame(updates, STATE_SCHEMA).createOrReplaceTempView(view)
        try:
            assignments = ", ".join(f"t.{name} = u.{name}" for name in STATE_COLUMNS[1:])
            columns = ", ".join(STATE_COLUMNS)
            values = ", ".join("u." + name for name in STATE_COLUMNS)
            spark.sql(f"""MERGE INTO {names["orders"]} t USING {view} u ON t.order_id = u.order_id
                WHEN MATCHED THEN UPDATE SET {assignments}
                WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})""")
        finally:
            spark.catalog.dropTempView(view)
    stop("after_source")
    stop("before_report")
    if body["groups"]:
        keys = spark.createDataFrame(body["groups"], "store_id string, business_date string")
        totals = (
            spark.read.format("delta")
            .load(str(root / "orders"))
            .where("status = 'active'")
            .join(keys, ["store_id", "business_date"], "inner")
            .groupBy("store_id", "business_date")
            .agg(F.count("*").alias("order_count"), F.sum("amount_minor").alias("amount_minor"))
        )
        repaired = keys.join(totals, ["store_id", "business_date"], "left").fillna(
            {"order_count": 0, "amount_minor": 0}
        )
        view = names["daily"] + "_updates"
        repaired.createOrReplaceTempView(view)
        try:
            spark.sql(f"""MERGE INTO {names["daily"]} t USING {view} u
                ON t.store_id = u.store_id AND t.business_date = u.business_date
                WHEN MATCHED AND u.order_count = 0 THEN DELETE
                WHEN MATCHED THEN UPDATE SET t.order_count = u.order_count, t.amount_minor = u.amount_minor
                WHEN NOT MATCHED AND u.order_count > 0 THEN INSERT
                    (store_id, business_date, order_count, amount_minor)
                    VALUES (u.store_id, u.business_date, u.order_count, u.amount_minor)""")
        finally:
            spark.catalog.dropTempView(view)
    stop("after_report")
    publish(acknowledged, receipt(body))
    stop("after_ack")
    pending.unlink()


def apply_batch(spark, state_dir, batch=None, interrupt=None):
    """Recover pending work, then commit a new bounded batch, under one local writer lock."""
    batch = normalize_batch(batch) if batch is not None else None
    root = Path(state_dir).resolve()
    with writer_lock(root):
        workspace, names = initialize(spark, root)
        if batch is not None and (root / "pending.json").exists():
            pending = validate_pending(read_json(root / "pending.json"), workspace)
            if pending["batch_id"] == batch["batch_id"] and pending["payload_hash"] != digest(batch):
                raise ValueError("batch_id was already used with different content")
        recover_locked(spark, root, workspace, names, interrupt)
        if batch is None:
            return
        body = {
            "format": 1,
            "workspace": workspace,
            "batch_id": batch["batch_id"],
            "payload_hash": digest(batch),
        }
        acknowledged = receipt_path(root, batch["batch_id"])
        if acknowledged.exists():
            if read_receipt(acknowledged) != body:
                raise ValueError("batch_id was already used with different content")
            return
        old = existing(spark, root, [row["order_id"] for row in batch["records"]])
        changes = prepare(batch, old)
        total = spark.read.format("delta").load(str(root / "orders")).count()
        if total + sum(change["old"] is None for change in changes) > MAX_ORDERS:
            raise ValueError("authoritative order limit exceeded")
        body.update(batch=batch, changes=changes, groups=[list(group) for group in affected_groups(changes)])
        publish(root / "pending.json", {"body": body, "sha256": digest(body)})
        recover_locked(spark, root, workspace, names, interrupt)


def demo_batches():
    def order(identity, version, store, day, amount, status="active"):
        return {
            "order_id": identity,
            "source_version": version,
            "operation": "upsert",
            "status": status,
            "store_id": store,
            "business_date": day,
            "amount_minor": amount,
        }

    return [
        {
            "batch_id": "initial",
            "records": [
                order("a", 1, "north", "2026-01-01", 1000),
                order("b", 1, "north", "2026-01-01", 500),
                order("c", 1, "south", "2026-01-01", 700),
            ],
        },
        {
            "batch_id": "corrections",
            "records": [
                order("a", 2, "south", "2026-01-02", 1200),
                order("b", 2, "north", "2026-01-01", 500, "cancelled"),
                {
                    "order_id": "c",
                    "source_version": 2,
                    "operation": "delete",
                    "status": None,
                    "store_id": None,
                    "business_date": None,
                    "amount_minor": None,
                },
            ],
        },
        {
            "batch_id": "late",
            "records": [
                order("a", 1, "north", "2026-01-01", 1000),
                order("b", 3, "north", "2026-01-02", 600),
            ],
        },
    ]


def rebuild(batches):
    """Independent full fold and complete daily rebuild, without query or journal helpers."""
    orders = {}
    for batch in batches:
        for record in batch["records"]:
            old = orders.get(record["order_id"])
            if old and old["source_version"] >= record["source_version"]:
                continue
            row = {name: record[name] for name in STATE_COLUMNS}
            if record["operation"] == "delete":
                row["status"] = "deleted"
            orders[record["order_id"]] = row
    groups = {}
    for row in orders.values():
        if row["status"] != "active":
            continue
        group = (row["store_id"], row["business_date"])
        count, amount = groups.get(group, (0, 0))
        groups[group] = count + 1, amount + row["amount_minor"]
    return {
        "orders": [orders[key] for key in sorted(orders)],
        "daily": [
            {"store_id": store, "business_date": day, "order_count": count, "amount_minor": amount}
            for (store, day), (count, amount) in sorted(groups.items())
        ],
    }


def outputs(spark, root):
    return {
        "orders": spark.read.format("delta")
        .load(str(root / "orders"))
        .select(*STATE_COLUMNS)
        .orderBy("order_id"),
        "daily": spark.read.format("delta").load(str(root / "daily")).orderBy("store_id", "business_date"),
    }


def run(spark):
    root = Path(tempfile.mkdtemp(prefix="daily-repair-"))
    atexit.register(shutil.rmtree, root, ignore_errors=True)
    batches = demo_batches()
    for batch in batches + [batches[-1]]:
        apply_batch(spark, root, batch)
    return outputs(spark, root)


def reference():
    import pyarrow as pa

    schemas = {
        "orders": pa.schema(
            [
                (name, pa.int64() if name in {"source_version", "amount_minor"} else pa.string())
                for name in STATE_COLUMNS
            ]
        ),
        "daily": pa.schema(
            [
                ("store_id", pa.string()),
                ("business_date", pa.string()),
                ("order_count", pa.int64()),
                ("amount_minor", pa.int64()),
            ]
        ),
    }
    return {
        name: pa.Table.from_pylist(rows, schema=schemas[name])
        for name, rows in rebuild(demo_batches()).items()
    }


def qualification_batches(count):
    if type(count) is not int or not 12 <= count <= 24:
        raise ValueError("qualification requires 12 to 24 batches")
    batches = []
    for number in range(count):
        rows = [
            {
                "order_id": f"order-{number * MAX_BATCH + offset}",
                "source_version": 1,
                "operation": "upsert",
                "status": "active",
                "store_id": "skewed",
                "business_date": "2026-01-01",
                "amount_minor": offset,
            }
            for offset in range(MAX_BATCH)
        ]
        batches.append({"batch_id": f"load-{number}", "records": rows})
    corrections = []
    for offset, row in enumerate(batches[0]["records"]):
        changed = {**row, "source_version": 2, "store_id": "moved", "business_date": "2026-01-02"}
        if offset % 3 == 0:
            changed.update(status="cancelled")
        elif offset % 3 == 1:
            changed.update(
                operation="delete", status=None, store_id=None, business_date=None, amount_minor=None
            )
        corrections.append(changed)
    return batches + [{"batch_id": "repair-after-checkpoints", "records": corrections}]


def qualify(spark, root, count):
    """Optional bounded lane: skewed history, checkpoint commits and post-source recovery."""
    if root.exists() and any(root.iterdir()):
        raise ValueError("qualification requires a new empty state directory")
    batches = qualification_batches(count)
    for batch in batches[:-1]:
        apply_batch(spark, root, batch)

    def interrupted(point):
        if point == "after_source":
            raise RuntimeError("qualification interruption after source commit")

    try:
        apply_batch(spark, root, batches[-1], interrupted)
    except RuntimeError as error:
        if str(error) != "qualification interruption after source commit":
            raise
    else:
        raise AssertionError("qualification did not reach the interruption boundary")
    apply_batch(spark, root)
    actual = {name: [row.asDict() for row in frame.collect()] for name, frame in outputs(spark, root).items()}
    assert actual == rebuild(batches), "qualification differs from independent full rebuild"
    checkpoints = {
        label: (root / label / "_delta_log" / "_last_checkpoint").exists() for label in ("orders", "daily")
    }
    assert all(checkpoints.values()), "qualification did not create both Delta checkpoints"
    assert not (root / "pending.json").exists(), "qualification left pending work"
    return {
        "batches": len(batches),
        "orders": len(actual["orders"]),
        "daily_groups": len(actual["daily"]),
        "checkpoints": checkpoints,
        "full_rebuild_match": True,
        "pending": (root / "pending.json").exists(),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, help="retained local workspace")
    parser.add_argument("--batch", type=Path, help="bounded JSON batch object")
    parser.add_argument("--recover", action="store_true", help="repair pending work without a new batch")
    parser.add_argument(
        "--qualify-batches", type=int, help="optional 12 to 24 generated load batches in an empty workspace"
    )
    args = parser.parse_args()
    modes = sum((args.batch is not None, args.recover, args.qualify_batches is not None))
    if modes > 1 or bool(modes) != (args.state_dir is not None):
        parser.error("use --state-dir with exactly one of --batch, --recover or --qualify-batches")
    if args.qualify_batches is not None and not 12 <= args.qualify_batches <= 24:
        parser.error("--qualify-batches must be from 12 to 24")
    batch = normalize_batch(read_json(args.batch)) if args.batch else None
    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        if args.qualify_batches is not None:
            print(canonical(qualify(spark, args.state_dir.resolve(), args.qualify_batches)))
            result = {}
        elif args.state_dir:
            apply_batch(spark, args.state_dir, batch)
            result = outputs(spark, args.state_dir.resolve())
        else:
            result = run(spark)
        for name, frame in result.items():
            print(name)
            frame.show(40, truncate=False)
    finally:
        spark.stop()
        server.stop()
