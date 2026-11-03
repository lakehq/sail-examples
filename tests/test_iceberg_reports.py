"""Saved report dependencies are checked before snapshot replay."""

import copy
import hashlib
import json
import subprocess
from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run
from iceberg_report_checks import literal_rows, load_example


def metadata_fixture(tmp_path):
    module = load_example()
    state = (tmp_path / "state").resolve()
    state.mkdir()
    tables = {}
    for index, name in enumerate(["orders", "products"], 1):
        table = state / name
        (table / "metadata").mkdir(parents=True)
        schema = {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "key", "required": False, "type": "long"}],
        }
        metadata = {
            "format-version": 2,
            "table-uuid": f"00000000-0000-0000-0000-{index:012d}",
            "location": str(table),
            "current-schema-id": 0,
            "schemas": [schema],
            "current-snapshot-id": index * 100,
            "snapshots": [{"snapshot-id": index * 100, "schema-id": 0, "timestamp-ms": 1000}],
        }
        path = table / "metadata/v1.metadata.json"
        path.write_text(json.dumps(metadata))
        (table / "metadata/version-hint.text").write_text("1\n")
        tables[name] = {
            "path": str(table),
            "uuid": metadata["table-uuid"],
            "snapshot_id": index * 100,
            "metadata_file": path.name,
            "metadata_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "schema": schema,
        }
    result = state / "original-report.json"
    result.write_text(json.dumps(literal_rows()))
    runtime = {
        "python": "test",
        "pysail": "test",
        "pyspark-client": "test",
        "pyarrow": "test",
        "server": "test",
    }
    manifest = {
        "format_version": 1,
        "state_path": str(state),
        "copies": 1,
        "params": dict(module.PARAMS),
        "configs": dict(module.CONFIGS),
        "runtime": runtime,
        "query_sha256": module.digest(module.QUERY),
        "code_sha256": module.file_digest(module.__file__),
        "tables": tables,
        "result_sha256": module.file_digest(result),
    }
    return module, state, manifest, runtime


def validate_fixture(module, state, manifest, runtime):
    return module.validate_manifest(state, manifest, runtime, dict(module.CONFIGS))


def test_valid_dependencies_preserve_literal_original_rows(tmp_path):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    assert validate_fixture(module, state, manifest, runtime) == literal_rows()


@pytest.mark.parametrize(
    "field,value",
    [
        ("format_version", True),
        ("format_version", 1.0),
        ("copies", True),
        ("copies", 1.0),
        ("copies", "1"),
        ("copies", 0),
        ("copies", 5001),
        ("copies", None),
    ],
)
def test_manifest_integer_fields_do_not_accept_coercions(tmp_path, field, value):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    manifest[field] = value
    with pytest.raises(ValueError):
        validate_fixture(module, state, manifest, runtime)


@pytest.mark.parametrize("value", [True, 100.0, "100", 0, -1, None])
def test_snapshot_ids_require_positive_integers(tmp_path, value):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    manifest["tables"]["orders"]["snapshot_id"] = value
    with pytest.raises(ValueError, match="snapshot ID"):
        validate_fixture(module, state, manifest, runtime)


@pytest.mark.parametrize("field", ["query_sha256", "code_sha256", "result_sha256", "state_path"])
def test_changed_manifest_dependency_rejects_replay(tmp_path, field):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    manifest[field] = "different"
    with pytest.raises(ValueError):
        validate_fixture(module, state, manifest, runtime)


@pytest.mark.parametrize(
    "mutation",
    [
        "params",
        "runtime",
        "configs",
        "missing_table",
        "extra_table",
        "extra_field",
        "table_path",
        "uuid",
        "metadata_digest",
        "metadata_path",
        "schema_boolean",
    ],
)
def test_changed_structured_dependencies_reject_replay(tmp_path, mutation):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    runtime = copy.deepcopy(runtime)
    if mutation in {"params", "runtime", "configs"}:
        manifest[mutation][next(iter(manifest[mutation]))] = "different"
    elif mutation == "missing_table":
        manifest["tables"].pop("products")
    elif mutation == "extra_table":
        manifest["tables"]["unknown"] = {}
    elif mutation == "extra_field":
        manifest["unknown"] = 1
    else:
        saved = manifest["tables"]["orders"]
        if mutation == "table_path":
            saved["path"] = str(state / "products")
        elif mutation == "uuid":
            saved["uuid"] = "00000000-0000-0000-0000-000000000099"
        elif mutation == "metadata_digest":
            saved["metadata_sha256"] = "0" * 64
        elif mutation == "metadata_path":
            saved["metadata_file"] = "../v1.metadata.json"
        else:
            saved["schema"] = copy.deepcopy(saved["schema"])
            saved["schema"]["fields"][0]["id"] = True
    with pytest.raises(ValueError):
        validate_fixture(module, state, manifest, runtime)


@pytest.mark.parametrize("hint", ["", "../1", "v1.metadata.json", "1\n2", "1.0", "0" * 40])
def test_invalid_hint_does_not_fall_back_to_existing_metadata(tmp_path, hint):
    module, state, _, _ = metadata_fixture(tmp_path)
    (state / "orders/metadata/version-hint.text").write_text(hint)
    with pytest.raises(ValueError, match="hint"):
        module.current_metadata(state / "orders")


def test_missing_hint_rejects_even_when_metadata_exists(tmp_path):
    module, state, _, _ = metadata_fixture(tmp_path)
    (state / "orders/metadata/version-hint.text").unlink()
    with pytest.raises(ValueError, match="hint is missing"):
        module.current_metadata(state / "orders")


def test_unreferenced_higher_filename_does_not_select_a_head(tmp_path):
    module, state, _, _ = metadata_fixture(tmp_path)
    (state / "orders/metadata/v99.metadata.json").write_text("not committed metadata")
    assert module.current_metadata(state / "orders")["name"] == "v1.metadata.json"


@pytest.mark.parametrize("mutation", ["missing_snapshot", "new_uuid", "new_schema", "missing_original_file"])
def test_changed_current_metadata_rejects_saved_dependencies(tmp_path, mutation):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    original = state / "orders/metadata/v1.metadata.json"
    head = json.loads(original.read_text())
    if mutation == "missing_snapshot":
        head["snapshots"] = [{"snapshot-id": 999, "timestamp-ms": 2000}]
        head["current-snapshot-id"] = 999
    elif mutation == "new_uuid":
        head["table-uuid"] = "00000000-0000-0000-0000-000000000099"
    elif mutation == "new_schema":
        head["schemas"][0]["fields"][0]["name"] = "other"
    else:
        original.unlink()
    (state / "orders/metadata/v2.metadata.json").write_text(json.dumps(head))
    (state / "orders/metadata/version-hint.text").write_text("2")
    with pytest.raises(ValueError):
        validate_fixture(module, state, manifest, runtime)


def test_report_boolean_totals_do_not_equal_integer_expectations(tmp_path):
    module, state, manifest, runtime = metadata_fixture(tmp_path)
    rows = literal_rows()
    rows[0]["orders"] = True
    path = state / "original-report.json"
    path.write_text(json.dumps(rows))
    manifest["result_sha256"] = module.file_digest(path)
    with pytest.raises(ValueError, match="nonnegative integers"):
        validate_fixture(module, state, manifest, runtime)


def test_manifest_checksum_detects_changed_bytes(tmp_path):
    module, state, manifest, _ = metadata_fixture(tmp_path)
    path = state / "report-manifest.json"
    path.write_text(json.dumps(manifest))
    (state / "report-manifest.sha256").write_text(module.file_digest(path))
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError, match="manifest checksum"):
        module.load_manifest(state)


def test_local_state_lock_refuses_overlapping_operations(tmp_path):
    module, state, _, _ = metadata_fixture(tmp_path)
    with (
        module.state_lock(state),
        pytest.raises(RuntimeError, match="state is busy"),
        module.state_lock(state),
    ):
        pytest.fail("a second operation acquired the state lock")
    with module.state_lock(state):
        assert state.is_dir()


def run_phase(phase, out, state, copies=1):
    return run(
        "sail",
        "fact",
        Path(__file__).with_name("iceberg_report_checks.py"),
        out,
        script_dependencies(ROOT / "analytics/reproduce-iceberg-report/main.py"),
        180,
        {
            "ICEBERG_REPORT_PHASE": phase,
            "ICEBERG_REPORT_STATE": str(state),
            "ICEBERG_REPORT_COPIES": str(copies),
        },
    )


def test_saved_report_reopens_after_mutations_in_three_processes(tmp_path):
    process_ids = []
    for phase in ["prepare", "change", "replay"]:
        meta = run_phase(phase, tmp_path / phase, tmp_path / "state")
        assert meta["status"] == "ok", f"{phase}: {meta}"
        process_ids.append(meta["observed"]["process_id"])
    assert len(set(process_ids)) == 3


def test_actual_snapshot_reads_fail_closed_and_recover_after_restoration(tmp_path):
    meta = run_phase("guards", tmp_path / "run", tmp_path / "state")
    assert meta["status"] == "ok", meta


def test_retained_state_cli_reopens_in_separate_invocations(tmp_path):
    script = ROOT / "analytics/reproduce-iceberg-report/main.py"
    state = tmp_path / "cli-state"
    for phase in ["prepare", "change", "replay"]:
        proc = subprocess.run(
            [
                "uv",
                "run",
                "--quiet",
                "--python",
                "3.12",
                "--script",
                str(script),
                "--state",
                str(state),
                "--phase",
                phase,
            ],
            cwd=script.parent,
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
        assert proc.returncode == 0, f"{phase}: {proc.stderr[-4000:]}"
        result = json.loads(proc.stdout)
        assert result == {"phase": phase, "report": literal_rows(changed=phase == "change")}
