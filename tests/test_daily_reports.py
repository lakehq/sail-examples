"""Bounded replacement identity and durable preimage journal contracts."""

import copy
import subprocess
import sys
from pathlib import Path

import pytest
from daily_report_checks import load_example
from harness import run

pytestmark = pytest.mark.examples("repair-daily-reports")


def test_full_rebuild_has_independent_literal_totals():
    module = load_example()
    batches = module.demo_batches()
    assert module.rebuild(batches[:1])["daily"] == [
        {"store_id": "north", "business_date": "2026-01-01", "order_count": 2, "amount_minor": 1500},
        {"store_id": "south", "business_date": "2026-01-01", "order_count": 1, "amount_minor": 700},
    ]
    assert module.rebuild(batches)["daily"] == [
        {"store_id": "north", "business_date": "2026-01-02", "order_count": 1, "amount_minor": 600},
        {"store_id": "south", "business_date": "2026-01-02", "order_count": 1, "amount_minor": 1200},
    ]
    assert module.rebuild(batches)["orders"][-1] == {
        "order_id": "c",
        "source_version": 2,
        "status": "deleted",
        "store_id": None,
        "business_date": None,
        "amount_minor": None,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_version", True),
        ("source_version", 1.0),
        ("source_version", 0),
        ("source_version", 1_000_000_001),
        ("amount_minor", True),
        ("amount_minor", 1.5),
        ("amount_minor", -1),
        ("amount_minor", 1_000_000_001),
        ("amount_minor", "100"),
        ("business_date", "2026-02-30"),
        ("business_date", "2026-1-01"),
        ("business_date", "2026-01-01T00:00:00Z"),
        ("store_id", " "),
        ("order_id", None),
        ("status", "deleted"),
        ("operation", "patch"),
    ],
)
def test_invalid_replacement_rejects_before_queries(field, value):
    module = load_example()
    batch = module.demo_batches()[0]
    batch["records"][0][field] = value
    with pytest.raises(ValueError):
        module.apply_batch(None, Path("unused-state"), batch)


def test_canonical_identity_ignores_row_order_and_exact_duplicate_deliveries():
    module = load_example()
    batch = module.demo_batches()[0]
    repeated = copy.deepcopy(
        {**batch, "records": list(reversed(batch["records"])) + [dict(batch["records"][0])]}
    )
    assert module.digest(module.normalize_batch(batch)) == module.digest(module.normalize_batch(repeated))
    repeated["records"][0]["amount_minor"] += 1
    assert module.digest(module.normalize_batch(batch)) != module.digest(module.normalize_batch(repeated))


def test_one_replacement_per_order_and_strict_delete_fields():
    module = load_example()
    batch = module.demo_batches()[0]
    batch["records"].append(dict(batch["records"][0], source_version=2))
    with pytest.raises(ValueError, match="one replacement"):
        module.normalize_batch(batch)
    deleted = module.demo_batches()[1]
    deleted["records"][-1]["amount_minor"] = 0
    with pytest.raises(ValueError, match="null business fields"):
        module.normalize_batch(deleted)
    with pytest.raises(ValueError, match="at most"):
        module.normalize_batch({"batch_id": "large", "records": [batch["records"][0]] * 513})


def test_pending_old_keys_survive_source_replacement_and_corruption_fails_closed():
    module = load_example()
    batch = module.normalize_batch(module.demo_batches()[1])
    old = {row["order_id"]: module.source_row(row) for row in module.demo_batches()[0]["records"]}
    changes = module.prepare(batch, old)
    body = {
        "format": 1,
        "workspace": "a" * 32,
        "batch_id": batch["batch_id"],
        "payload_hash": module.digest(batch),
        "batch": batch,
        "changes": changes,
        "groups": [list(group) for group in module.affected_groups(changes)],
    }
    envelope = {"body": body, "sha256": module.digest(body)}
    assert module.validate_pending(envelope, "a" * 32)["groups"] == [
        ["north", "2026-01-01"],
        ["south", "2026-01-01"],
        ["south", "2026-01-02"],
    ]
    with pytest.raises(ValueError, match="workspace"):
        module.validate_pending(envelope, "b" * 32)
    corrupt = copy.deepcopy(envelope)
    corrupt["body"]["groups"].pop(0)
    with pytest.raises(ValueError, match="integrity"):
        module.validate_pending(corrupt, "a" * 32)
    corrupt["sha256"] = module.digest(corrupt["body"])
    with pytest.raises(ValueError, match="groups"):
        module.validate_pending(corrupt, "a" * 32)


def test_atomic_file_publication_and_exclusive_process_lock(tmp_path):
    module = load_example()
    target = tmp_path / "record.json"
    module.publish(target, {"first": 1})
    module.publish(target, {"second": 2})
    assert module.read_json(target) == {"second": 2}
    assert not list(tmp_path.glob("*.tmp"))
    target.write_text('{"x":1,"x":2}')
    with pytest.raises(ValueError, match="duplicate"):
        module.read_json(target)
    with module.writer_lock(tmp_path):
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import fcntl,sys\nf=open(sys.argv[1], 'a')\n"
                    "try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
                    "except BlockingIOError: sys.exit(0)\nsys.exit(1)"
                ),
                str(tmp_path / ".writer.lock"),
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        assert child.returncode == 0


def test_malformed_acknowledgement_cannot_match_through_boolean_integer_equality(tmp_path):
    module = load_example()
    path = tmp_path / "receipt.json"
    value = {"format": 1, "workspace": "a" * 32, "batch_id": "example", "payload_hash": "b" * 64}
    module.publish(path, value)
    assert module.read_receipt(path) == value
    module.publish(path, {**value, "format": True})
    with pytest.raises(ValueError, match="invalid batch acknowledgement"):
        module.read_receipt(path)


def test_established_workspace_rejects_missing_or_empty_delta_logs_before_sql(tmp_path):
    module = load_example()
    module.publish(tmp_path / "workspace.json", {"format": 1, "workspace": "a" * 32, "initialized": True})
    with pytest.raises(ValueError, match="missing a Delta transaction log"):
        module.initialize(None, tmp_path)
    for label in ("orders", "daily"):
        (tmp_path / label / "_delta_log").mkdir(parents=True)
    with pytest.raises(ValueError, match="missing a Delta transaction log"):
        module.initialize(None, tmp_path)


def test_longer_fixture_exceeds_checkpoint_and_source_batch_boundaries():
    module = load_example()
    batches = module.qualification_batches(18)
    assert len(batches) == 19
    assert all(len(batch["records"]) == 512 for batch in batches)
    assert all(module.normalize_batch(batch) for batch in batches)
    rebuilt = module.rebuild(batches)
    assert len(rebuilt["orders"]) == 9216
    assert len(rebuilt["daily"]) == 2
    assert sum(row["order_count"] for row in rebuilt["daily"]) == 8874
    assert rebuilt["daily"] == [
        {"store_id": "moved", "business_date": "2026-01-02", "order_count": 170, "amount_minor": 43435},
        {"store_id": "skewed", "business_date": "2026-01-01", "order_count": 8704, "amount_minor": 2223872},
    ]
    for invalid in (11, 25, True, 12.0):
        with pytest.raises(ValueError, match="12 to 24"):
            module.qualification_batches(invalid)


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_native_daily_repairs_match_full_rebuild(engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("daily_report_checks.py"),
        tmp_path / engine,
        [],
        360,
        spark_extras=["delta"],
    )
    assert meta["status"] == "ok", f"{engine}: {meta}"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_fresh_process_recovers_old_report_keys_after_source_commit(engine, tmp_path):
    shared = {"DAILY_REPORT_STATE": str(tmp_path / "state")}
    for phase, expected in (("interrupt", "failed"), ("recover", "ok")):
        meta = run(
            engine,
            "fact",
            Path(__file__).with_name("daily_report_checks.py"),
            tmp_path / phase,
            [],
            180,
            {**shared, "DAILY_REPORT_PHASE": phase},
            ["delta"],
        )
        assert meta["status"] == expected, f"{engine}, {phase}: {meta}"
        if phase == "interrupt":
            assert "simulated process interruption after source commit" in meta["message"]
