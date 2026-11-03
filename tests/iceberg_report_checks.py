"""Exact sales-report checks across local Iceberg snapshots and process restarts."""

import copy
import importlib.util
import json
import os
import uuid
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "analytics/reproduce-iceberg-report/main.py"
    spec = importlib.util.spec_from_file_location("iceberg_report_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def literal_rows(copies=1, changed=False):
    values = (
        [("Components", 1, 3, 4500), ("Hardware", 2, 3, 7500)]
        if changed
        else [("Accessories", 1, 3, 2400), ("Components", 2, 3, 5500)]
    )
    return [
        {
            "category": category,
            "orders": orders * copies,
            "units": units * copies,
            "sales_minor": sales * copies,
        }
        for category, orders, units, sales in values
    ]


def expect_replay_error(module, spark, state, fragment):
    try:
        module.replay(spark, state)
    except (ValueError, FileNotFoundError) as error:
        assert fragment in str(error), str(error)
    else:
        raise AssertionError("replay accepted invalid state")


def check(spark, engine):
    assert engine == "sail"
    module = load_example()
    state = Path(os.environ["ICEBERG_REPORT_STATE"])
    phase = os.environ["ICEBERG_REPORT_PHASE"]
    copies = int(os.environ.get("ICEBERG_REPORT_COPIES", "1"))
    if phase == "prepare":
        rows = module.prepare(spark, state, copies)
        assert rows == literal_rows(copies)
        (state / "initial-order-files.json").write_text(
            json.dumps([str(path) for path in (state / "orders").rglob("*.parquet")])
        )
    elif phase == "change":
        rows = module.change(spark, state)
        assert rows == literal_rows(copies, changed=True)
        manifest = module.load_manifest(state)
        receipt = json.loads((state / "changes.json").read_text())
        assert [(step["table"], step["operation"]) for step in receipt["steps"]] == [
            ("orders", "append"),
            ("orders", "update"),
            ("orders", "delete"),
            ("products", "update"),
        ]
        inputs = copy.deepcopy(manifest["tables"])
        expected_states = [
            [("Accessories", 1, 3, 2400), ("Components", 3, 5, 10500)],
            [("Accessories", 1, 3, 2400), ("Components", 3, 6, 12000)],
            [("Components", 3, 6, 12000)],
            [("Components", 1, 3, 4500), ("Hardware", 2, 3, 7500)],
        ]
        for step, expected in zip(receipt["steps"], expected_states, strict=True):
            inputs[step["table"]] = step["selected"]
            actual = module.rows_at(spark, inputs, manifest["params"])
            assert actual == [
                {
                    "category": category,
                    "orders": orders * copies,
                    "units": units * copies,
                    "sales_minor": sales * copies,
                }
                for category, orders, units, sales in expected
            ]
        order_ids = [manifest["tables"]["orders"]["snapshot_id"]] + [
            step["selected"]["snapshot_id"] for step in receipt["steps"] if step["table"] == "orders"
        ]
        assert len(set(order_ids)) == 4
        try:
            module.change(spark, state)
        except ValueError as error:
            assert "already started" in str(error)
        else:
            raise AssertionError("source mutations were applied twice")
    elif phase == "replay":
        rows = module.replay(spark, state)
        assert rows == literal_rows(copies)
        current = {name: module.describe_table(state / name) for name in module.SCHEMAS}
        assert module.rows_at(spark, current, module.PARAMS) == literal_rows(copies, changed=True)
    elif phase == "guards":
        module.prepare(spark, state, copies)
        selected_files = sorted((state / "orders").rglob("*.parquet"))
        assert selected_files, "the initial snapshot must have physical order data"
        module.change(spark, state)
        assert module.replay(spark, state) == literal_rows(copies)
        head = module.current_metadata(state / "orders")
        path = state / "orders/metadata" / head["name"]
        original_bytes = path.read_bytes()
        original_id = module.load_manifest(state)["tables"]["orders"]["snapshot_id"]
        for mutation, fragment in [("uuid", "UUID"), ("history", "history"), ("schema", "schema")]:
            changed = copy.deepcopy(head["metadata"])
            if mutation == "uuid":
                changed["table-uuid"] = str(uuid.uuid4())
            elif mutation == "history":
                changed["snapshots"] = [
                    row for row in changed["snapshots"] if row["snapshot-id"] != original_id
                ]
            else:
                changed["schemas"][0]["fields"][0]["name"] = "different_order_key"
            try:
                path.write_text(json.dumps(changed))
                expect_replay_error(module, spark, state, fragment)
            finally:
                path.write_bytes(original_bytes)
        query = module.QUERY
        try:
            module.QUERY += " LIMIT 1"
            expect_replay_error(module, spark, state, "query or code changed")
        finally:
            module.QUERY = query
        data_file = selected_files[0]
        moved = data_file.with_suffix(".held")
        try:
            data_file.rename(moved)
            try:
                module.replay(spark, state)
            except Exception as error:  # noqa: BLE001 - the actual reader must fail, without a current-data fallback
                assert data_file.name in str(error), str(error)
            else:
                raise AssertionError("replay silently ignored missing original snapshot data")
        finally:
            moved.rename(data_file)
        assert module.replay(spark, state) == literal_rows(copies)
    else:
        raise AssertionError(f"unknown check phase: {phase}")
    return {"phase": phase, "process_id": os.getpid(), "copies": copies, "exact_rows_checked": True}
