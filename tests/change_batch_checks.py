"""Adversarial version barriers and a fresh-session replay after acknowledgement loss."""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "pipelines/apply-versioned-changes/main.py"
    spec = importlib.util.spec_from_file_location("change_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(spark, engine):
    module = load_example()
    phase = os.environ.get("CHANGE_CHECK_PHASE", "controls")

    def apply(path, rows, label="test"):
        return module.apply_batch(spark, path, spark.createDataFrame(rows, module.INPUT_SCHEMA), label)

    def state(path):
        return [tuple(row) for row in module.read_state(spark, path).orderBy("entity_id").collect()]

    if phase != "controls":
        path = Path(os.environ["CHANGE_CHECK_STATE"])
        batch = [("delivery", "item", 9, "delete", None)]
        result = apply(path, batch).collect()
        assert state(path) == [("item", 9, "deleted", None)]
        if phase == "commit-without-ack":
            assert result[0].outcome == "accepted"
            raise AssertionError("simulated acknowledgement loss after committed state")
        assert phase == "restart"
        assert result[0].outcome == "duplicate"
        assert apply(path, [("late", "item", 8, "upsert", "obsolete")]).first().outcome == "stale"
        assert state(path) == [("item", 9, "deleted", None)]
        return "fresh session replay preserved tombstone"

    with tempfile.TemporaryDirectory(prefix="change-controls-") as directory:
        root = Path(directory)
        path = root / "main"
        assert apply(path, []).count() == 0
        assert state(path) == []
        rows = [
            ("id", "   ", 9, "upsert", "invalid key"),
            ("   ", "ignored", 9, "upsert", "invalid label"),
            ("invalid-high", "a", 99, "delete", "not null"),
            ("valid-low", "a", 1, "upsert", "kept"),
            ("valid-copy", "a", 1, "upsert", "kept"),
            ("bad-op", "b", 4, "replace", "bad"),
            ("bad-version", "c", None, "upsert", "bad"),
            ("bad-upsert", "d", 1, "upsert", None),
        ]
        outcomes = {row.change_id: row.outcome for row in apply(path, rows).collect()}
        assert outcomes == {
            "id": "invalid_entity_id",
            "   ": "invalid_change_id",
            "invalid-high": "invalid_payload",
            "valid-low": "accepted",
            "valid-copy": "accepted",
            "bad-op": "invalid_operation",
            "bad-version": "invalid_version",
            "bad-upsert": "invalid_payload",
        }
        assert state(path) == [("a", 1, "active", "kept")]
        assert apply(path, []).count() == 0
        assert state(path) == [("a", 1, "active", "kept")]

        variants = [("up", "x", 7, "upsert", "value"), ("del", "x", 7, "delete", None)]
        # Same events in one batch and in either delivery order converge to quarantine.
        for number, batches in enumerate(
            ([variants], [[variants[0]], [variants[1]]], [[variants[1]], [variants[0]]])
        ):
            candidate = root / f"order-{number}"
            for batch in batches:
                apply(candidate, batch).collect()
            assert state(candidate) == [("x", 7, "conflict", None)]
            for event in variants:
                assert apply(candidate, [event]).first().outcome == "conflict"
                assert state(candidate) == [("x", 7, "conflict", None)]
            apply(candidate, [("new", "x", 8, "upsert", "resolved")]).collect()
            apply(candidate, variants).collect()
            assert state(candidate) == [("x", 8, "active", "resolved")]

        # Failure before MERGE cannot publish the already materialized decision files.
        original = spark.sql

        def interrupted(query, *args, **kwargs):
            if query.lstrip().upper().startswith("MERGE INTO"):
                raise RuntimeError("simulated interruption before merge")
            return original(query, *args, **kwargs)

        spark.sql = interrupted
        try:
            try:
                apply(path, [("next", "a", 2, "upsert", "next")])
            except RuntimeError as error:
                assert "before merge" in str(error)
            else:
                raise AssertionError("interruption was not reached")
        finally:
            spark.sql = original
        assert state(path) == [("a", 1, "active", "kept")]
        assert apply(path, [("next", "a", 2, "upsert", "next")]).first().outcome == "accepted"
        assert state(path) == [("a", 2, "active", "next")]

        # Change the underlying file after decisions were materialized. Retained
        # diagnostics must describe the original input, never a second file read.
        source = root / "mutable.jsonl"
        source_row = dict(zip(module.INPUT_COLUMNS, ("once", "frozen", 1, "upsert", "A")))
        source.write_text(json.dumps(source_row) + "\n")
        changes = spark.read.schema(module.INPUT_SCHEMA).json(str(source))
        writer_class = type(changes.write)
        original_write = writer_class.parquet

        def changed_source(writer, destination, *args, **kwargs):
            result = original_write(writer, destination, *args, **kwargs)
            if str(destination).endswith("/decisions"):
                source.write_text(json.dumps({**source_row, "value": "B"}) + "\n")
            return result

        writer_class.parquet = changed_source
        try:
            frozen = root / "frozen"
            retained = module.apply_batch(spark, frozen, changes, "frozen").collect()
        finally:
            writer_class.parquet = original_write
        assert state(frozen) == [("frozen", 1, "active", "A")]
        assert len(retained) == 1 and retained[0].value == "A" and retained[0].outcome == "accepted"

        with module.state_lock(path):
            child = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import fcntl,sys\nwith open(sys.argv[1], 'a') as f:\n"
                        " try: fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                        " except BlockingIOError: sys.exit(0)\n"
                        " sys.exit(1)\n"
                    ),
                    str(path.with_name(path.name + ".lock")),
                ],
                check=False,
                capture_output=True,
                timeout=10,
            )
            assert child.returncode == 0, "another process acquired the writer lock"
            try:
                apply(path, [("blocked", "a", 3, "upsert", "blocked")])
            except RuntimeError as error:
                assert "another writer" in str(error)
            else:
                raise AssertionError("second writer acquired the same state path")
        assert state(path) == [("a", 2, "active", "next")]
    return "empty/invalid/duplicate/conflict/order/failure/lock controls passed"
