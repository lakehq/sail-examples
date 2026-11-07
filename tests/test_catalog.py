"""Every example declares a Python version its pinned packages support, and pins every package.

An example that pins a package the Sail engine also pins uses the same version, so a release bump in
tests/engines.toml cannot leave an example behind. Metadata that names Spark extras must name ones that
tests/engines.toml defines.
"""

import re
import tomllib
from pathlib import Path

import pytest
from catalog import check_spark_extras, discover_examples

EXAMPLES = discover_examples()
REQUIRES_PYTHON = ">=3.12"  # pandas 3 and pyarrow 26 need 3.11, numpy 2.5 needs 3.12
ENGINES = tomllib.loads((Path(__file__).resolve().parent / "engines.toml").read_text())


def package(dep: str) -> str:
    return re.split(r"[=<>!~\[; ]", dep, maxsplit=1)[0].lower()


SAIL_PINS = {package(dep): dep for dep in ENGINES["sail"]["deps"]}


HEADER = re.compile(r"^# /// script\n((?:#.*\n)*?)# ///$", re.MULTILINE)


def header(path):
    match = HEADER.search(path.read_text())
    assert match, f"{path} has no PEP 723 header"
    body = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in match.group(1).splitlines())
    return tomllib.loads(body)


def scripts(example) -> list[Path]:
    """The example's main.py and any other script beside it with its own PEP 723 header."""
    return [path for path in sorted(example.path.parent.glob("*.py")) if HEADER.search(path.read_text())]


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_header_python_and_pins(example):
    meta = header(example.path)
    assert meta.get("requires-python") == REQUIRES_PYTHON, (
        f"requires-python is {meta.get('requires-python')!r}"
    )
    unpinned = [dep for dep in meta.get("dependencies", []) if "==" not in dep]
    assert not unpinned, f"unpinned dependencies: {unpinned}"


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_header_matches_engine_pins(example):
    drifted = [
        f"{path.name}: {dep}"
        for path in scripts(example)
        for dep in header(path).get("dependencies", [])
        if package(dep) in SAIL_PINS and dep != SAIL_PINS[package(dep)]
    ]
    assert not drifted, f"{drifted} differ from tests/engines.toml {sorted(SAIL_PINS.values())}"


@pytest.mark.parametrize(
    "extras",
    [["nope"], "delta", [1]],
    ids=["unknown-name", "not-a-list", "not-a-name"],
)
def test_unknown_spark_extras_are_rejected(extras):
    with pytest.raises(ValueError, match="spark_extras"):
        check_spark_extras(Path("README.md"), {"spark_extras": extras})


def test_known_spark_extras_are_accepted():
    check_spark_extras(Path("README.md"), {"spark_extras": ["delta"]})
    check_spark_extras(Path("README.md"), {})


def test_release_override_reaches_standalone_scripts(monkeypatch, tmp_path):
    from harness import SAIL_DEPS_OVERRIDE, script_command

    script = tmp_path / "main.py"
    script.write_text('# /// script\n# dependencies = ["pysail==0.1.0", "pillow==13.0.0"]\n# ///\n')
    monkeypatch.setenv(SAIL_DEPS_OVERRIDE, '["pysail==0.7.3", "pyspark-client==4.1.3"]')
    command = script_command(script, "--input", "records.jsonl")
    assert "pysail==0.7.3" in command
    assert "pysail==0.1.0" not in command
    assert "pillow==13.0.0" in command
    assert "--no-project" in command and "--isolated" in command
    assert command[-4:] == ["python", str(script), "--input", "records.jsonl"]


def test_default_standalone_command_uses_script_pins(monkeypatch, tmp_path):
    from harness import SAIL_DEPS_OVERRIDE, script_command

    monkeypatch.delenv(SAIL_DEPS_OVERRIDE, raising=False)
    script = tmp_path / "main.py"
    command = script_command(script)
    assert command[-2:] == ["--script", str(script)]
    assert "--with" not in command


def test_output_history_groups_equal_results_and_preserves_changes(tmp_path):
    import json

    from harness import record_output

    path = tmp_path / "outputs.json"
    for version, output in [("0.7.3", "X"), ("0.7.1", "X"), ("0.7.2", "Y"), ("0.7.4", "X"), ("0.7.10", "Z")]:
        record_output(path, {"output": output, "run": {"sail": version, "script_sha256": "abc"}})
    snapshots = json.loads(path.read_text())["snapshots"]
    assert [item["output"] for item in snapshots] == ["X", "Y", "X", "Z"]
    assert [[run["sail"] for run in item["runs"]] for item in snapshots] == [
        ["0.7.1"],
        ["0.7.2"],
        ["0.7.3", "0.7.4"],
        ["0.7.10"],
    ]
    original = path.read_bytes()
    record_output(path, {"output": "X", "run": {"sail": "0.7.4", "script_sha256": "abc"}})
    assert path.read_bytes() == original
    for output, source in [("changed", "abc"), ("X", "changed")]:
        with pytest.raises(ValueError, match="already has a different capture"):
            record_output(path, {"output": output, "run": {"sail": "0.7.4", "script_sha256": source}})
        assert path.read_bytes() == original


def test_equal_output_keeps_each_release_source_hash(tmp_path):
    import json

    from harness import record_output

    path = tmp_path / "outputs.json"
    for version, source in [("0.7.2", "old"), ("0.7.3", "new")]:
        record_output(path, {"output": "same", "run": {"sail": version, "script_sha256": source}})
    snapshots = json.loads(path.read_text())["snapshots"]
    assert len(snapshots) == 1
    assert [run["script_sha256"] for run in snapshots[0]["runs"]] == ["old", "new"]


def test_reviewed_revision_preserves_old_capture_and_is_idempotent(tmp_path):
    import json

    from harness import record_output

    path = tmp_path / "outputs.json"
    first = {"output": "old\n", "run": {"sail": "0.7.3", "script_sha256": "old"}}
    second = {"output": "new\n", "run": {"sail": "0.7.3", "script_sha256": "new"}}
    record_output(path, first)
    record_output(path, second, revision_reason="Reviewed the example change")
    history = json.loads(path.read_text())
    assert history["snapshots"][0] == {"output": first["output"], "runs": [first["run"]]}
    current = history["snapshots"][1]["runs"][0]
    assert current["revision"] == 2
    assert current["revision_reason"] == "Reviewed the example change"
    original = path.read_bytes()
    record_output(path, second, revision_reason="Retry")
    assert path.read_bytes() == original
    with pytest.raises(ValueError, match="already has a different capture"):
        record_output(path, first)


def test_elapsed_time_does_not_require_capture_revision(tmp_path):
    from harness import record_output

    path = tmp_path / "outputs.json"
    run = {"sail": "0.7.3", "script_sha256": "same"}
    record_output(path, {"output": "4 passed in 1.25s\n", "run": run})
    original = path.read_bytes()
    record_output(path, {"output": "4 passed in 0.96s\n", "run": run})
    assert path.read_bytes() == original


def test_revision_requires_reason_and_preserves_failed_run_history(tmp_path):
    import json

    from harness import record_output, record_output_failure

    path = tmp_path / "outputs.json"
    old = {"output": "old", "run": {"sail": "0.7.3", "script_sha256": "old"}}
    new = {"output": "new", "run": {"sail": "0.7.3", "script_sha256": "new"}}
    record_output(path, old, "queens")
    record_output_failure(path, "0.7.3", "queens")
    original = path.read_bytes()
    with pytest.raises(ValueError, match="reason"):
        record_output(path, new, "queens", revision_reason=" ")
    assert path.read_bytes() == original
    record_output(path, new, "queens", revision_reason="Reviewed a corrected query")
    history = json.loads(path.read_text())["variants"]["queens"]
    assert "failed_versions" not in history
    assert len(history["snapshots"]) == 2


@pytest.mark.parametrize("version", ["latest", "../other", "0.7.3rc1", "0.7.3; echo unsafe"])
def test_capture_requires_an_explicit_stable_release(version):
    from harness import release_key

    with pytest.raises(ValueError, match="stable Sail release"):
        release_key(version)


def test_entry_point_capture_records_only_successful_stdout(tmp_path, monkeypatch):
    import runner

    monkeypatch.setattr(runner.importlib.metadata, "version", lambda name: "0.7.3")
    path = tmp_path / "main.py"
    path.write_text('import sys\nprint("result")\nprint("diagnostic", file=sys.stderr)\nsys.exit(0)\n')
    result = runner.capture_entry_point(path)
    assert result["output"] == "result\n"
    assert result["run"]["sail"] == "0.7.3"
    assert len(result["run"]["script_sha256"]) == 64
    path.write_text('print("partial output")\nraise SystemExit(1)\n')
    with pytest.raises(RuntimeError, match="exited with 1"):
        runner.capture_entry_point(path)
    path.write_text("pass\n")
    with pytest.raises(ValueError, match="no output"):
        runner.capture_entry_point(path)


def test_failed_rerun_cannot_reuse_success_metadata(tmp_path, monkeypatch):
    import harness

    output = tmp_path / "capture"
    output.mkdir()
    (output / "meta.json").write_text('{"status": "ok", "capture": {}}')

    class FailedProcess:
        def communicate(self, timeout):
            return "", "dependency resolution failed"

    monkeypatch.setattr(harness.subprocess, "Popen", lambda *args, **kwargs: FailedProcess())
    result = harness.run("sail", "entry-point", tmp_path / "main.py", output, [], 5)
    assert result["status"] == "error"
    assert "dependency resolution failed" in result["message"]
    assert not (output / "meta.json").exists()


def test_runner_reports_the_location_of_a_bare_assertion(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace

    import runner

    script = tmp_path / "bare_assertion.py"
    script.write_text("def check(spark, engine):\n    assert False\n")
    output = tmp_path / "result"
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(runner, "_SERVER", None)
    monkeypatch.setattr(runner, "engine_versions", lambda engine: {})
    monkeypatch.setattr(runner, "make_session", lambda engine: SimpleNamespace(stop=lambda: None))
    monkeypatch.setattr(
        runner.sys, "argv", ["runner.py", "--engine", "sail", "--fact", str(script), "--out", str(output)]
    )
    assert runner.main() == 1
    result = json.loads((output / "meta.json").read_text())
    assert result["status"] == "failed"
    assert "bare_assertion.py" in result["message"]
    assert "assert False" in result["message"]


def test_failed_capture_preserves_results_and_successful_retry_clears_failure(tmp_path):
    import json

    from harness import record_output, record_output_failure

    path = tmp_path / "outputs.json"
    record_output_failure(path, "0.7.3")
    assert json.loads(path.read_text()) == {"snapshots": [], "failed_versions": ["0.7.3"]}
    capture = {"output": "result", "run": {"sail": "0.7.3", "script_sha256": "abc"}}
    record_output(path, capture)
    original = json.loads(path.read_text())
    assert "failed_versions" not in original
    record_output_failure(path, "0.7.3")
    assert json.loads(path.read_text())["snapshots"] == original["snapshots"]
    record_output(path, capture)
    assert json.loads(path.read_text()) == original
