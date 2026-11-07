"""Reviewed output stays immutable while new runs produce useful comparisons."""

import hashlib
import json

import pytest
from output_comparison import compare_output


@pytest.fixture
def example(tmp_path):
    script = tmp_path / "main.py"
    script.write_text('print("result")\n')
    history = script.with_name("outputs.json")
    runs = [
        ("0.7.2", "old\n"),
        ("0.7.3", "reviewed\n"),
        ("0.7.10", "future\n"),
    ]
    history.write_text(
        json.dumps(
            {
                "snapshots": [
                    {
                        "output": output,
                        "runs": [
                            {
                                "sail": version,
                                "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
                            }
                        ],
                    }
                    for version, output in runs
                ],
            }
        )
    )
    return script, history, tmp_path / "report"


@pytest.mark.parametrize(
    "version,expected", [("0.7.3", "reviewed\n"), ("0.7.4", "reviewed\n"), ("0.7.11", "future\n")]
)
def test_uses_latest_reviewed_capture_no_newer_than_target(example, version, expected):
    script, history, report = example
    original = history.read_bytes()
    result = compare_output(script, expected, version, report)
    assert result["status"] == "matched"
    assert (report / "expected.txt").read_text() == expected
    assert (report / "output.diff").read_text() == ""
    assert history.read_bytes() == original


def test_diff_does_not_accept_changed_output_or_changed_source(example):
    script, history, report = example
    original = history.read_bytes()
    script.write_text('print("new")\n')
    for _ in range(2):
        result = compare_output(script, "new\n", "0.7.4", report)
        assert result["status"] == "changed"
        assert result["source_changed"] is True
        difference = (report / "output.diff").read_text()
        assert "-reviewed\n+new\n" in difference
        assert "Sail 0.7.3" in difference and "Sail 0.7.4" in difference
        assert (report / "actual.txt").read_text() == "new\n"
        assert history.read_bytes() == original


@pytest.mark.parametrize("actual", ["reviewed \n", "reviewed", "Reviewed\n", "reviewed\nextra\n"])
def test_formatting_values_and_final_newline_changes_are_reported(example, actual):
    script, _, report = example
    assert compare_output(script, actual, "0.7.3", report)["status"] == "changed"
    assert (report / "output.diff").read_text()


def test_line_endings_do_not_report_a_change(example):
    script, _, report = example
    assert compare_output(script, "reviewed\r\n", "0.7.3", report)["status"] == "matched"


def test_pytest_duration_is_ignored_but_counts_and_result_values_are_not(example):
    from output_comparison import comparable_output

    assert comparable_output("4 passed in 1.25s\n") == comparable_output("4 passed in 0.96s\n")
    assert comparable_output("4 passed in 1.25s\n") != comparable_output("3 passed in 1.25s\n")
    assert comparable_output("query finished in 1.25s\n") == "query finished in 1.25s\n"
    assert comparable_output("score=1.25\n") == "score=1.25\n"
    script, history, report = example
    data = json.loads(history.read_text())
    data["snapshots"][1]["output"] = "4 passed in 1.25s\n"
    history.write_text(json.dumps(data))
    assert compare_output(script, "4 passed in 0.96s\n", "0.7.3", report)["status"] == "matched"
    assert (report / "actual.txt").read_text() == "4 passed in 0.96s\n"
    assert (report / "expected.txt").read_text() == "4 passed in 1.25s\n"


@pytest.mark.parametrize("history_state", ["absent", "failed", "newer-only"])
def test_missing_baseline_is_reported_without_creating_a_capture(example, history_state):
    script, history, report = example
    compare_output(script, "changed\n", "0.7.3", report)
    if history_state == "absent":
        history.unlink()
    elif history_state == "failed":
        history.write_text('{"snapshots": [], "failed_versions": ["0.7.1"]}')
    original = history.read_bytes() if history.exists() else None
    result = compare_output(script, "new\n", "0.7.1", report)
    assert result["status"] == "missing-baseline"
    assert (report / "expected.txt").read_text() == ""
    assert (report / "output.diff").read_text() == ""
    assert (history.read_bytes() if history.exists() else None) == original


def test_malformed_history_does_not_silently_disable_comparison(example):
    script, history, report = example
    history.write_text('{"snapshots": [{"output": "oops", "runs": [{"sail": "latest"}]}]}')
    with pytest.raises(ValueError, match="stable Sail release"):
        compare_output(script, "new\n", "0.7.3", report)


def test_comparison_uses_latest_reviewed_revision_for_release(example):
    from harness import record_output

    script, history, report = example
    record_output(
        history,
        {
            "output": "revised\n",
            "run": {"sail": "0.7.3", "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest()},
        },
        revision_reason="Reviewed a corrected result",
    )
    assert compare_output(script, "revised\n", "0.7.3", report)["status"] == "matched"
    assert compare_output(script, "reviewed\n", "0.7.3", report)["status"] == "changed"
    assert compare_output(script, "old\n", "0.7.2", report)["status"] == "matched"
