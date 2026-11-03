"""Strict nested order boundaries and native normalization/readback checks."""

import base64
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
from harness import run
from order_event_checks import example, reference


def value():
    return {
        "schema_version": 1,
        "order_id": "ORDER-1",
        "customer_id": "C-1",
        "currency": "USD",
        "items": [{"line_id": "L1", "sku": "SKU", "quantity": 2, "unit_price_minor": 1250}],
    }


def parse(module, record):
    return module.parse_record(json.dumps(record).encode())


@pytest.mark.parametrize(
    "field,bad,reason",
    [
        ("schema_version", True, "unsupported_schema_version"),
        ("schema_version", "1", "unsupported_schema_version"),
        ("schema_version", 1.0, "unsupported_schema_version"),
        ("schema_version", None, "unsupported_schema_version"),
        ("schema_version", 2, "unsupported_schema_version"),
        ("order_id", True, "invalid_order_id"),
        ("order_id", "contains space", "invalid_order_id"),
        ("order_id", "é", "invalid_order_id"),
        ("customer_id", None, "invalid_customer_id"),
        ("currency", None, "unsupported_currency"),
        ("currency", "JPY", "unsupported_currency"),
        ("items", None, "null_items"),
        ("items", {}, "items_not_array"),
    ],
)
def test_header_types_and_version_are_not_coerced(field, bad, reason):
    module, record = example(), value()
    record[field] = bad
    assert parse(module, record)["reasons"] == [reason]


@pytest.mark.parametrize(
    "field,bad,reason",
    [
        ("line_id", None, "invalid_line_id"),
        ("sku", "", "invalid_sku"),
        ("quantity", True, "invalid_quantity"),
        ("quantity", 1.0, "invalid_quantity"),
        ("quantity", "2", "invalid_quantity"),
        ("quantity", 0, "invalid_quantity"),
        ("quantity", 1_000_001, "invalid_quantity"),
        ("unit_price_minor", False, "invalid_unit_price_minor"),
        ("unit_price_minor", 12.5, "invalid_unit_price_minor"),
        ("unit_price_minor", "1250", "invalid_unit_price_minor"),
        ("unit_price_minor", -1, "invalid_unit_price_minor"),
        ("unit_price_minor", 1_000_000_001, "invalid_unit_price_minor"),
    ],
)
def test_invalid_child_rejects_whole_order(field, bad, reason):
    module, record = example(), value()
    record["items"][0][field] = bad
    actual = parse(module, record)
    assert actual["reasons"] == ["items[0]." + reason]
    assert actual["items"] is None


def test_missing_empty_duplicate_and_non_object_children_are_deliberate():
    module, record = example(), value()
    del record["items"]
    assert parse(module, record)["reasons"] == ["missing_items"]
    record["items"] = []
    assert parse(module, record)["items"] == []
    record["items"] = [value()["items"][0], value()["items"][0]]
    assert parse(module, record)["reasons"] == ["items[1].duplicate_line_id"]
    record["items"] = [None]
    assert parse(module, record)["reasons"] == ["items[0].not_object"]
    record["items"] = [value()["items"][0]] * 101
    assert parse(module, record)["reasons"] == ["item_count_limit"]


@pytest.mark.parametrize(
    "raw,reason",
    [
        (b"\xff\n", "invalid_utf8"),
        (b"\n", "empty_record"),
        (b"{", "malformed_json"),
        (b"[]", "top_level_not_object"),
        (b"null", "top_level_not_object"),
        (b'{"x":1,"x":2}', "duplicate_json_member"),
        (b'{"extension":{"x":1,"x":2}}', "duplicate_json_member"),
        (b'{"x":NaN}', "nonfinite_json_number"),
        (b'{"x":Infinity}', "nonfinite_json_number"),
        (b'{"extension":1e9999999999999999999}', "numeric_representation_limit"),
    ],
)
def test_boundary_failures_retain_exact_original_bytes(raw, reason):
    actual = example().parse_record(raw)
    assert actual["reasons"] == [reason]
    assert base64.b64decode(actual["raw_record_base64"]) == raw


def test_record_size_limit_retains_complete_evidence():
    module = example()
    raw = b"x" * (module.MAX_RECORD_BYTES + 1)
    actual = module.parse_record(raw)
    assert actual["reasons"] == ["record_byte_limit"]
    assert base64.b64decode(actual["raw_record_base64"]) == raw


def test_valid_json_integer_representation_limit_is_not_a_syntax_failure():
    raw = b'{"extension":' + b"1" * 5000 + b"}"
    actual = example().parse_record(raw)
    assert actual["reasons"] == ["numeric_representation_limit"]
    assert base64.b64decode(actual["raw_record_base64"]) == raw


def test_worker_rechecks_source_digest_and_preserves_physical_line_endings(tmp_path):
    import pyarrow as pa

    module = example()
    path = tmp_path / "records.jsonl"
    path.write_bytes(b"{}\r\n\n{}")
    sources = module.manifest([path])
    batches = [pa.RecordBatch.from_pylist(sources)]
    rows = [row for batch in module.read_sources(batches) for row in batch.to_pylist()]
    assert [base64.b64decode(row["raw_record_base64"]) for row in rows] == [b"{}\r\n", b"\n", b"{}"]
    assert [row["line_number"] for row in rows] == [1, 2, 3]
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="source_file_sha256_mismatch"):
        list(module.read_sources(batches))


def test_original_unknown_fields_are_not_lost_or_coerced():
    module, record = example(), value()
    record["extension"] = {"null": None, "fraction": 0.125}
    record["items"][0]["color"] = "blue"
    raw = (json.dumps(record, indent=None) + "\r\n").encode()
    actual = module.parse_record(raw)
    assert actual["reasons"] == [] and actual["raw_json"].encode() == raw
    assert json.loads(actual["raw_json"])["extension"] == record["extension"]


def test_integer_bound_is_safe_for_maximum_order():
    module, record = example(), value()
    record["items"] = [
        {
            "line_id": f"L{index}",
            "sku": "SKU",
            "quantity": module.MAX_QUANTITY,
            "unit_price_minor": module.MAX_PRICE,
        }
        for index in range(module.MAX_ITEMS)
    ]
    assert parse(module, record)["reasons"] == []
    assert module.MAX_ITEMS * module.MAX_QUANTITY * module.MAX_PRICE == 100_000_000_000_000_000 < 2**63


def test_file_bounds_and_source_identity_are_explicit(tmp_path):
    module = example()
    missing = tmp_path / "missing.jsonl"
    with pytest.raises(FileNotFoundError):
        module.manifest([missing])
    too_large = tmp_path / "large.jsonl"
    too_large.write_bytes(b"x" * (module.MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="source_file_byte_limit"):
        module.manifest([too_large])
    too_many = tmp_path / "many.jsonl"
    too_many.write_bytes(b"\n" * (module.MAX_RECORDS_PER_FILE + 1))
    with pytest.raises(ValueError, match="source_file_record_limit"):
        module.manifest([too_many])
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    with pytest.raises(ValueError, match="unique basenames"):
        module.manifest([first / "same.jsonl", second / "same.jsonl"])


def test_authored_reference_has_literal_counts_and_money(tmp_path):
    module = example()
    expected = reference(module.demo_files(tmp_path))
    assert {name: len(rows) for name, rows in expected.items()} == {"orders": 3, "lines": 3, "rejects": 12}
    assert [row["gross_minor"] for row in expected["orders"]] == [3000, 0, 0]


@pytest.mark.parametrize("copies", [0, 1001, True, 1.0])
def test_demo_size_is_bounded(tmp_path, copies):
    with pytest.raises(ValueError, match="copies"):
        example().demo_files(tmp_path, copies)


def test_cli_accepts_repeatable_inputs_without_generating_demo_copies():
    module = example()
    args = module.parse_cli(["--input", "first.jsonl", "--input", "second.jsonl", "--json"])
    assert args.input == [Path("first.jsonl"), Path("second.jsonl")] and args.json
    assert module.parse_cli([]).input is None and module.parse_cli([]).copies == 1
    with pytest.raises(SystemExit) as error:
        module.parse_cli(["--input", "first.jsonl", "--copies", "1"])
    assert error.value.code == 2


def test_order_cli_retains_complete_datasets_and_refuses_an_existing_output_directory(tmp_path):
    import pyarrow.parquet as pq

    module = example()
    source, output = tmp_path / "source.jsonl", tmp_path / "normalized"
    source.write_text(json.dumps(value()) + "\n[]\n")
    command = [
        "uv",
        "run",
        "--quiet",
        "--script",
        str(Path(module.__file__)),
        "--input",
        str(source),
        "--output-dir",
        str(output),
        "--json",
    ]
    env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=300, check=False)
    assert completed.returncode == 0, completed.stderr[-2000:]
    assert json.loads(completed.stdout) == {"orders": 1, "lines": 1, "rejects": 1}
    retained = {name: pq.read_table(output / name).to_pylist() for name in ("orders", "lines", "rejects")}
    assert retained["orders"][0]["gross_minor"] == retained["lines"][0]["gross_minor"] == 2500
    assert retained["rejects"][0]["reasons"] == ["top_level_not_object"]
    assert base64.b64decode(retained["rejects"][0]["raw_record_base64"]) == b"[]\n"
    before = {
        str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in output.rglob("*")
        if path.is_file()
    }
    refused = subprocess.run(command, env=env, capture_output=True, text=True, timeout=300, check=False)
    assert refused.returncode != 0 and "FileExistsError" in refused.stderr
    after = {
        str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in output.rglob("*")
        if path.is_file()
    }
    assert after == before


@pytest.mark.parametrize("case", ["contract", "boundaries", "larger"])
@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_order_file_ingestion_and_normalization(case, engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("order_event_checks.py"),
        tmp_path / engine,
        [],
        360,
        {"SAIL_EXAMPLES_ORDER_CASE": case},
    )
    assert meta["status"] == "ok", f"{engine} {case} [{meta['status']}]: {meta.get('message')}"
