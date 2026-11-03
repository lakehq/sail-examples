"""Native Delta report repairs and restart controls against a full Python rebuild."""

import importlib.util
import json
import os
import tempfile
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "pipelines/repair-daily-reports/main.py"
    spec = importlib.util.spec_from_file_location("daily_report_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(spark, engine):
    module = load_example()

    def collected(root):
        return {
            name: [row.asDict() for row in frame.collect()]
            for name, frame in module.outputs(spark, root).items()
        }

    phase = os.environ.get("DAILY_REPORT_PHASE", "controls")
    if phase != "controls":
        root = Path(os.environ["DAILY_REPORT_STATE"])
        initial, correction, _ = module.demo_batches()
        if phase == "interrupt":
            module.apply_batch(spark, root, initial)

            def lost_process(point):
                if point == "after_source":
                    raise AssertionError("simulated process interruption after source commit")

            module.apply_batch(spark, root, correction, lost_process)
            raise AssertionError("failure boundary was not reached")
        assert phase == "recover"
        pending = module.read_json(root / "pending.json")
        assert ["north", "2026-01-01"] in pending["body"]["groups"]
        # Recovery uses the journal's old keys even though source revisions already changed.
        module.apply_batch(spark, root)
        assert collected(root) == module.rebuild([initial, correction]), engine
        assert not (root / "pending.json").exists()
        assert collected(root)["daily"] == [
            {"store_id": "south", "business_date": "2026-01-02", "order_count": 1, "amount_minor": 1200}
        ]
        module.apply_batch(spark, root, correction)
        assert collected(root) == module.rebuild([initial, correction])
        return "fresh process repaired removed groups from retained preimages"

    with tempfile.TemporaryDirectory(prefix="daily-controls-") as directory:
        root = Path(directory)
        batches = module.demo_batches()
        applied = []
        for batch in batches:
            module.apply_batch(spark, root, batch)
            applied.append(batch)
            assert collected(root) == module.rebuild(applied), (engine, batch["batch_id"])
        assert collected(root)["daily"] == [
            {"store_id": "north", "business_date": "2026-01-02", "order_count": 1, "amount_minor": 600},
            {"store_id": "south", "business_date": "2026-01-02", "order_count": 1, "amount_minor": 1200},
        ]

        # Every interruption boundary reuses the same source workspace, not a fresh rebuild.
        for version, point in enumerate(module.FAIL_POINTS, start=3):
            new = dict(
                batches[1]["records"][0],
                source_version=version,
                store_id=f"store-{version}",
                business_date=f"2026-01-{version:02}",
            )
            batch = {"batch_id": f"move-{version}", "records": [new]}

            def interrupt(actual, expected=point):
                if actual == expected:
                    raise RuntimeError("interrupted at " + expected)

            try:
                module.apply_batch(spark, root, batch, interrupt)
            except RuntimeError as error:
                assert str(error) == "interrupted at " + point
            else:
                raise AssertionError("failure hook was not reached")
            assert (root / "pending.json").exists()
            retained = module.read_json(root / "pending.json")
            assert retained["body"]["changes"][0]["old"]["store_id"] != new["store_id"]
            if point != "before_source":
                assert module.existing(spark, root, ["a"])["a"] == module.source_row(new)
            # Batch identity is bound even before acknowledgement, and rejection cannot repair/write.
            changed = {"batch_id": batch["batch_id"], "records": [dict(new, amount_minor=99)]}
            raw = (root / "pending.json").read_bytes()
            try:
                module.apply_batch(spark, root, changed)
            except ValueError as error:
                assert "different content" in str(error)
            else:
                raise AssertionError("pending batch identity was reused")
            assert (root / "pending.json").read_bytes() == raw
            module.apply_batch(spark, root)
            applied.append(batch)
            assert collected(root) == module.rebuild(applied), (engine, point)
            assert not (root / "pending.json").exists()
            module.apply_batch(spark, root, batch)
            assert collected(root) == module.rebuild(applied)

        before = collected(root)
        last = applied[-1]["records"][0]
        conflicting = {"batch_id": "equal-conflict", "records": [dict(last, amount_minor=42)]}
        try:
            module.apply_batch(spark, root, conflicting)
        except ValueError as error:
            assert "conflicting replacement" in str(error)
        else:
            raise AssertionError("equal version conflict was accepted")
        assert not (root / "pending.json").exists()
        assert collected(root) == before
        equivalent = {"batch_id": applied[-1]["batch_id"], "records": [last, dict(last)]}
        module.apply_batch(spark, root, equivalent)
        assert collected(root) == before
        module.apply_batch(spark, root, {"batch_id": "equal-new-delivery", "records": [last]})
        assert collected(root) == before
        try:
            module.apply_batch(spark, root, {**equivalent, "records": [dict(last, amount_minor=1)]})
        except ValueError as error:
            assert "different content" in str(error)
        else:
            raise AssertionError("acknowledged identity was reused")

        module.apply_batch(spark, root, {"batch_id": "empty", "records": []})
        assert collected(root) == before
        tombstone = dict(
            batches[1]["records"][2],
            source_version=1,
            operation="upsert",
            status="active",
            store_id="stale",
            business_date="2026-01-01",
            amount_minor=999,
        )
        module.apply_batch(spark, root, {"batch_id": "stale-delete-replay", "records": [tombstone]})
        assert collected(root) == before
        free = dict(last, order_id="free", source_version=1, store_id="free", amount_minor=0)
        free_batch = {"batch_id": "zero-amount", "records": [free]}
        module.apply_batch(spark, root, free_batch)
        applied.append(free_batch)
        assert collected(root) == module.rebuild(applied)
        assert {
            "store_id": "free",
            "business_date": free["business_date"],
            "order_count": 1,
            "amount_minor": 0,
        } in collected(root)["daily"]
        receipts = list((root / "receipts").glob("*.json"))
        assert len(receipts) == len(batches) + len(module.FAIL_POINTS) + 4
        assert all(
            set(json.loads(path.read_text())) == {"format", "workspace", "batch_id", "payload_hash"}
            for path in receipts
        )
        # A completed workspace must not recreate a missing report and accept its receipt.
        saved = root / "daily-retained"
        (root / "daily").rename(saved)
        try:
            try:
                module.apply_batch(spark, root, applied[-1])
            except ValueError as error:
                assert "missing a Delta transaction log" in str(error)
            else:
                raise AssertionError("missing report was silently recreated")
            assert not (root / "daily").exists()
        finally:
            saved.rename(root / "daily")
        assert collected(root) == module.rebuild(applied)
    return {"boundaries": len(module.FAIL_POINTS), "full_rebuild_matches": True}
