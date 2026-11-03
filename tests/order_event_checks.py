"""Literal snapshot classifications and independent parent/child arithmetic."""

import base64
import hashlib
import importlib.util
import json
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REASONS = {
    3: ["items[0].invalid_quantity"],
    4: ["items[1].duplicate_line_id"],
    5: ["missing_items"],
    6: ["null_items"],
    7: ["unsupported_schema_version"],
    8: ["malformed_json"],
    9: ["duplicate_json_member"],
    10: ["items[0].invalid_unit_price_minor"],
    11: ["duplicate_order_id"],
    12: ["duplicate_order_id"],
    13: ["top_level_not_object"],
    14: ["empty_record"],
}


def example():
    spec = importlib.util.spec_from_file_location(
        "order_example", ROOT / "pipelines/normalize-order-events/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def reference(paths):
    """Only the authored 15-record demonstration, using literal classifications, not parser decisions."""
    outputs = {"orders": [], "lines": [], "rejects": []}
    for path in paths:
        content = path.read_bytes()
        for line_number, raw in enumerate(content.splitlines(keepends=True), 1):
            position = (line_number - 1) % 15 + 1
            source = {
                "source_id": path.name,
                "source_sha256": hashlib.sha256(content).hexdigest(),
                "line_number": line_number,
            }
            evidence = {
                "raw_record_base64": base64.b64encode(raw).decode("ascii"),
                "raw_json": raw.decode("utf-8"),
            }
            if position in REASONS:
                outputs["rejects"].append({**source, **evidence, "reasons": REASONS[position]})
                continue
            assert position in (1, 2, 15)
            value = json.loads(raw)
            total = 0
            for item in value["items"]:
                amount = item["quantity"] * item["unit_price_minor"]
                outputs["lines"].append(
                    {
                        **source,
                        "order_id": value["order_id"],
                        **{key: item[key] for key in ("line_id", "sku", "quantity", "unit_price_minor")},
                        "gross_minor": amount,
                    }
                )
                total += amount
            outputs["orders"].append(
                {
                    **source,
                    **{key: value[key] for key in ("order_id", "customer_id", "currency")},
                    "item_count": len(value["items"]),
                    "gross_minor": total,
                    **evidence,
                }
            )
    return outputs


def collect(outputs):
    return {name: [row.asDict(recursive=True) for row in frame.collect()] for name, frame in outputs.items()}


def check(spark, engine):
    module = example()
    case = os.environ.get("SAIL_EXAMPLES_ORDER_CASE", "contract")
    with tempfile.TemporaryDirectory(prefix="order-contract-") as directory:
        root = Path(directory)
        if case in ("contract", "larger"):
            copies = 1 if case == "contract" else 1000
            paths = module.demo_files(root, copies)
            actual = collect(module.run(spark, paths))
            expected = reference(paths)
            assert actual == expected, (engine, case, actual, expected)
            assert {name: len(rows) for name, rows in actual.items()} == {
                "orders": 3 * copies,
                "lines": 3 * copies,
                "rejects": 12 * copies,
            }
            assert [(row["item_count"], row["gross_minor"]) for row in actual["orders"][:3]] == [
                (2, 3000),
                (0, 0),
                (1, 0),
            ]
            assert {row["line_id"]: row["gross_minor"] for row in actual["lines"][:3]} == {
                "L1": 2500,
                "L2": 500,
                "FREE": 0,
            }
            assert json.loads(actual["orders"][0]["raw_json"])["promotion"] == {"campaign": "spring"}
        elif case == "boundaries":
            empty = root / "empty.jsonl"
            empty.write_bytes(b"")
            assert collect(module.run(spark, [empty])) == {"orders": [], "lines": [], "rejects": []}
            bad = root / "invalid.jsonl"
            valid_neighbor = {
                "schema_version": 1,
                "order_id": "NEIGHBOR",
                "customer_id": "C",
                "currency": "GBP",
                "items": [],
            }
            bad.write_bytes(
                b'\xff\n{}\r\n\n{"extension":1e9999999999999999999}\n'
                + b'{"extension":'
                + b"1" * 5000
                + b"}\n"
                + json.dumps(valid_neighbor).encode()
                + b"\n"
            )
            rejected = collect(module.run(spark, [bad]))
            assert rejected["lines"] == []
            assert [
                (row["order_id"], row["item_count"], row["gross_minor"]) for row in rejected["orders"]
            ] == [("NEIGHBOR", 0, 0)]
            assert [row["reasons"] for row in rejected["rejects"]] == [
                ["invalid_utf8"],
                [
                    "unsupported_schema_version",
                    "invalid_order_id",
                    "invalid_customer_id",
                    "unsupported_currency",
                    "missing_items",
                ],
                ["empty_record"],
                ["numeric_representation_limit"],
                ["numeric_representation_limit"],
            ]
            assert (
                b"".join(
                    base64.b64decode(row["raw_record_base64"])
                    for row in sorted(
                        rejected["rejects"] + rejected["orders"], key=lambda row: row["line_number"]
                    )
                )
                == bad.read_bytes()
            )
            first, second = root / "first.jsonl", root / "second.jsonl"
            value = {
                "schema_version": 1,
                "order_id": "REPEATED",
                "customer_id": "C",
                "currency": "USD",
                "items": [],
            }
            first.write_text(json.dumps(value) + "\n")
            second.write_bytes(first.read_bytes())
            third = root / "third.jsonl"
            third.write_text(
                json.dumps({**value, "order_id": "EMPTY-ACCEPTED"})
                + "\n"
                + json.dumps({**value, "order_id": "EMPTY-ACCEPTED", "items": None})
                + "\n"
            )
            repeated = collect(module.run(spark, [first, second, third]))
            assert repeated["lines"] == []
            assert [
                (row["order_id"], row["item_count"], row["gross_minor"]) for row in repeated["orders"]
            ] == [("EMPTY-ACCEPTED", 0, 0)]
            assert [row["reasons"] for row in repeated["rejects"]] == [
                ["duplicate_order_id"],
                ["duplicate_order_id"],
                ["null_items"],
            ]
            assert {row["source_id"] for row in repeated["rejects"]} == {
                "first.jsonl",
                "second.jsonl",
                "third.jsonl",
            }
        else:
            raise AssertionError("unknown order check case")
    return {"engine": engine, "case": case, "ok": True}
