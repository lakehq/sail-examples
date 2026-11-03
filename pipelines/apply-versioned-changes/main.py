# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Apply versioned replacement records while preserving deletion and conflict barriers."""

import argparse
import atexit
import contextlib
import fcntl
import hashlib
import shutil
import tempfile
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

INPUT_SCHEMA = "change_id string, entity_id string, source_version long, operation string, value string"
STATE_SCHEMA = "entity_id string, source_version long, status string, value string"
INPUT_COLUMNS = ["change_id", "entity_id", "source_version", "operation", "value"]
STATE_COLUMNS = ["entity_id", "source_version", "status", "value"]

BATCHES = [
    [
        ("a1", "a", 1, "upsert", "bronze"),
        ("a2", "a", 2, "upsert", "silver"),
        ("b3", "b", 3, "upsert", "bronze"),
        ("c2", "c", 2, "delete", None),
        ("e6", "e", 6, "delete", None),
    ],
    [
        ("a2-conflict", "a", 2, "upsert", "gold"),
        ("b4", "b", 4, "delete", None),
        ("c1-stale", "c", 1, "upsert", "gold"),
        ("d7-left", "d", 7, "upsert", "silver"),
        ("d7-right", "d", 7, "upsert", "gold"),
    ],
    [
        ("a2-replay", "a", 2, "upsert", "silver"),
        ("b3-stale", "b", 3, "upsert", "gold"),
        ("c2-replay", "c", 2, "delete", None),
        ("d7-replay", "d", 7, "upsert", "gold"),
        ("e5-stale", "e", 5, "upsert", "gold"),
        ("bad-key", None, 1, "upsert", "gold"),
        ("bad-version", "f", 0, "upsert", "gold"),
    ],
    [
        ("a3", "a", 3, "upsert", "platinum"),
        ("b5", "b", 5, "upsert", "gold"),
        ("c3", "c", 3, "upsert", "bronze"),
        ("d8", "d", 8, "upsert", "silver"),
    ],
]


def temp_dir(prefix: str) -> Path:
    path = Path(tempfile.mkdtemp(prefix=prefix))
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


@contextlib.contextmanager
def state_lock(path: Path):
    """Exclude another writer using this example's local state path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another writer holds this state path") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def state_table(spark: SparkSession, path: Path) -> str:
    name = "versioned_state_" + hashlib.sha256(str(path).encode()).hexdigest()[:16]
    location = str(path).replace("'", "''")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({STATE_SCHEMA}) USING delta LOCATION '{location}'")
    return name


def read_state(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path)).select(*STATE_COLUMNS)


def materialize(frame: DataFrame, path: Path) -> DataFrame:
    schema = frame.schema
    frame.write.parquet(str(path))
    return frame.sparkSession.read.schema(schema).parquet(str(path))


def apply_batch(spark: SparkSession, path: Path, changes: DataFrame, batch: str) -> DataFrame:
    """Commit one reduced candidate per entity; return materialized per-input diagnostics.

    The state is authoritative. Diagnostics are separate files, not a transaction
    with it. A failure after the state commit can require replaying the batch.
    """
    path = Path(path).resolve()
    with state_lock(path):
        target = state_table(spark, path)
        invalid = (
            F.when(F.col("change_id").isNull() | (F.trim("change_id") == ""), "invalid_change_id")
            .when(F.col("entity_id").isNull() | (F.trim("entity_id") == ""), "invalid_entity_id")
            .when(F.col("source_version").isNull() | (F.col("source_version") <= 0), "invalid_version")
            .when(
                F.col("operation").isNull() | ~F.col("operation").isin("upsert", "delete"),
                "invalid_operation",
            )
            .when(
                ((F.col("operation") == "upsert") & F.col("value").isNull())
                | ((F.col("operation") == "delete") & F.col("value").isNotNull()),
                "invalid_payload",
            )
        )
        tagged = changes.select(*INPUT_COLUMNS).withColumn("_invalid", invalid)
        staged = temp_dir("versioned-batch-")
        # One input snapshot feeds both state decisions and retained diagnostics.
        tagged = materialize(tagged, staged / "input")
        valid = tagged.where("_invalid IS NULL")
        if not valid.limit(1).count():
            diagnostics = tagged.select(
                F.lit(batch).alias("batch"), *INPUT_COLUMNS, F.col("_invalid").alias("outcome")
            )
            return materialize(diagnostics, staged / "diagnostics")
        # Exact distinct tuples avoid relying on hash equality for conflict detection.
        variants = (
            valid.select("entity_id", "source_version", "operation", "value")
            .distinct()
            .groupBy("entity_id", "source_version")
            .agg(
                F.count("*").alias("variants"),
                F.max("operation").alias("operation"),
                F.max("value").alias("value"),
            )
        )
        latest = (
            variants.withColumn(
                "_rank",
                F.row_number().over(Window.partitionBy("entity_id").orderBy(F.desc("source_version"))),
            )
            .where("_rank = 1")
            .withColumn(
                "status",
                F.when(F.col("variants") > 1, "conflict")
                .when(F.col("operation") == "delete", "deleted")
                .otherwise("active"),
            )
            .withColumn("value", F.when(F.col("status") == "active", F.col("value")))
        )
        joined = latest.alias("u").join(read_state(spark, path).alias("s"), "entity_id", "left")
        equal_version = F.col("u.source_version") == F.col("s.source_version")
        equal_value = (F.col("u.status") == F.col("s.status")) & F.col("u.value").eqNullSafe(F.col("s.value"))
        outcome = (
            F.when(
                F.col("s.source_version").isNull(),
                F.when(F.col("u.status") == "conflict", "conflict").otherwise("accepted"),
            )
            .when(F.col("u.source_version") < F.col("s.source_version"), "stale")
            .when(
                F.col("u.source_version") > F.col("s.source_version"),
                F.when(F.col("u.status") == "conflict", "conflict").otherwise("accepted"),
            )
            .when((F.col("s.status") == "conflict") | ~equal_value, "conflict")
            .otherwise("duplicate")
        )
        next_status = F.when(equal_version & (outcome == "conflict"), "conflict").otherwise(F.col("u.status"))
        write = (
            F.col("s.source_version").isNull()
            | (F.col("u.source_version") > F.col("s.source_version"))
            | (equal_version & (outcome == "conflict") & (F.col("s.status") != "conflict"))
        )
        decisions = joined.select(
            "entity_id",
            F.col("u.source_version").alias("source_version"),
            next_status.alias("status"),
            F.when(next_status == "active", F.col("u.value")).alias("value"),
            outcome.alias("outcome"),
            write.alias("needs_write"),
        )
        # Freeze the decision against the old table before MERGE changes it.
        decisions = materialize(decisions, staged / "decisions")
        lookup = decisions.select("entity_id", F.col("source_version").alias("_latest"), "outcome")
        diagnostics = (
            tagged.join(lookup, "entity_id", "left")
            .withColumn(
                "outcome",
                F.when(F.col("_invalid").isNotNull(), F.col("_invalid"))
                .when(F.col("source_version") < F.col("_latest"), "superseded_in_batch")
                .otherwise(F.col("outcome")),
            )
            .withColumn("batch", F.lit(batch))
            .select("batch", *INPUT_COLUMNS, "outcome")
        )
        diagnostics = materialize(diagnostics, staged / "diagnostics")
        pending = decisions.where("needs_write").select(*STATE_COLUMNS)
        if pending.limit(1).count():
            source = target + "_changes"
            pending.createOrReplaceTempView(source)
            try:
                spark.sql(f"""
                    MERGE INTO {target} AS t USING {source} AS u ON t.entity_id = u.entity_id
                    WHEN MATCHED THEN UPDATE SET t.source_version = u.source_version,
                        t.status = u.status, t.value = u.value
                    WHEN NOT MATCHED THEN INSERT (entity_id, source_version, status, value)
                        VALUES (u.entity_id, u.source_version, u.status, u.value)
                """)
            finally:
                spark.catalog.dropTempView(source)
        return diagnostics


def fold_state(batches):
    """Independent event-by-event reference for the highest valid source version."""
    state = {}
    for batch in batches:
        for change_id, entity, version, operation, value in batch:
            if (
                not change_id
                or not change_id.strip(" ")
                or not entity
                or not entity.strip(" ")
                or version is None
                or version <= 0
            ):
                continue
            if operation not in {"upsert", "delete"}:
                continue
            if (operation == "upsert" and value is None) or (operation == "delete" and value is not None):
                continue
            incoming = (version, "active" if operation == "upsert" else "deleted", value)
            old = state.get(entity)
            if old is None or version > old[0]:
                state[entity] = incoming
            elif version == old[0] and (old[1] == "conflict" or old != incoming):
                state[entity] = (version, "conflict", None)
    return [
        {"entity_id": key, "source_version": v, "status": s, "value": x}
        for key, (v, s, x) in sorted(state.items())
    ]


def scenario():
    return list(enumerate(BATCHES + [BATCHES[-1], BATCHES[0]], start=1))


def run(spark: SparkSession) -> dict[str, DataFrame]:
    root = temp_dir("versioned-example-")
    path = root / "state"
    outcomes, checkpoints = [], []
    for number, batch in scenario():
        outcomes.append(apply_batch(spark, path, spark.createDataFrame(batch, INPUT_SCHEMA), str(number)))
        snapshot = root / f"checkpoint-{number}"
        read_state(spark, path).withColumn("batch", F.lit(str(number))).write.parquet(str(snapshot))
        checkpoints.append(spark.read.parquet(str(snapshot)))
    combined = outcomes[0]
    history = checkpoints[0]
    for output in outcomes[1:]:
        combined = combined.unionByName(output)
    for checkpoint in checkpoints[1:]:
        history = history.unionByName(checkpoint)
    state = read_state(spark, path)
    return {
        "current": state.where("status = 'active'").orderBy("entity_id"),
        "state": state.orderBy("entity_id"),
        "checkpoints": history.select("batch", *STATE_COLUMNS).orderBy("batch", "entity_id"),
        "outcomes": combined.orderBy("batch", "change_id"),
    }


def reference():
    import pyarrow as pa

    state_type = pa.schema(
        [
            ("entity_id", pa.string()),
            ("source_version", pa.int64()),
            ("status", pa.string()),
            ("value", pa.string()),
        ]
    )
    expected = [
        ["superseded_in_batch", "accepted", "accepted", "accepted", "accepted"],
        ["conflict", "accepted", "stale", "conflict", "conflict"],
        ["conflict", "stale", "duplicate", "conflict", "stale", "invalid_entity_id", "invalid_version"],
        ["accepted"] * 4,
        ["duplicate"] * 4,
        ["superseded_in_batch", "stale", "stale", "stale", "duplicate"],
    ]
    outcomes, checkpoints, seen = [], [], []
    for (number, batch), labels in zip(scenario(), expected):
        seen.append(batch)
        checkpoints.extend({"batch": str(number), **row} for row in fold_state(seen))
        outcomes.extend(
            {"batch": str(number), **dict(zip(INPUT_COLUMNS, row)), "outcome": label}
            for row, label in zip(batch, labels)
        )
    final = fold_state(seen)
    return {
        "current": pa.Table.from_pylist(
            [row for row in final if row["status"] == "active"], schema=state_type
        ),
        "state": pa.Table.from_pylist(final, schema=state_type),
        "checkpoints": pa.Table.from_pylist(
            sorted(checkpoints, key=lambda row: (row["batch"], row["entity_id"])),
            schema=pa.schema([("batch", pa.string()), *state_type]),
        ),
        "outcomes": pa.Table.from_pylist(
            sorted(outcomes, key=lambda row: (row["batch"], row["change_id"])),
            schema=pa.schema(
                [
                    ("batch", pa.string()),
                    ("change_id", pa.string()),
                    ("entity_id", pa.string()),
                    ("source_version", pa.int64()),
                    ("operation", pa.string()),
                    ("value", pa.string()),
                    ("outcome", pa.string()),
                ]
            ),
        ),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changes", type=Path, help="one JSON Lines batch in the input schema")
    parser.add_argument("--state-dir", type=Path, help="persistent local Delta state directory")
    parser.add_argument(
        "--outcomes-dir", type=Path, help="complete per-run diagnostics (default: beside state)"
    )
    args = parser.parse_args()
    if bool(args.changes) != bool(args.state_dir):
        parser.error("--changes and --state-dir must be supplied together")
    if args.outcomes_dir and not args.changes:
        parser.error("--outcomes-dir requires --changes and --state-dir")
    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        if args.changes:
            changes = (
                spark.read.schema(INPUT_SCHEMA).option("mode", "FAILFAST").json(str(args.changes.resolve()))
            )
            diagnostics = apply_batch(spark, args.state_dir, changes, args.changes.name)
            root = args.outcomes_dir or args.state_dir.with_name(args.state_dir.name + "-outcomes")
            output = root.resolve() / uuid.uuid4().hex
            diagnostics.write.parquet(str(output))
            print(f"Complete outcomes: {output}")
            read_state(spark, args.state_dir.resolve()).orderBy("entity_id").show(truncate=False)
            diagnostics.orderBy("change_id").show(20, truncate=False)
        else:
            results = run(spark)
            results["state"].show(truncate=False)
            results["outcomes"].show(40, truncate=False)
    finally:
        spark.stop()
        server.stop()
