"""Versioned state must survive retries, conflicting deliveries and session restarts."""

import json
import os
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from catalog import ROOT
from harness import run


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_change_batch_controls(engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("change_batch_checks.py"),
        tmp_path / engine,
        [],
        300,
        spark_extras=["delta"],
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_change_batch_fresh_session_replay(engine, tmp_path):
    shared = {"CHANGE_CHECK_STATE": str(tmp_path / "durable-state")}
    for phase, status in [("commit-without-ack", "failed"), ("restart", "ok")]:
        meta = run(
            engine,
            "fact",
            Path(__file__).with_name("change_batch_checks.py"),
            tmp_path / phase,
            [],
            120,
            {**shared, "CHANGE_CHECK_PHASE": phase},
            ["delta"],
        )
        assert meta["status"] == status, f"{engine} {phase}: {meta}"
        if phase == "commit-without-ack":
            assert "simulated acknowledgement loss" in meta.get("message", "")


def test_change_cli_retains_complete_outcomes_across_runs(tmp_path):
    source = tmp_path / "changes.jsonl"
    rows = [
        {
            "change_id": str(i),
            "entity_id": str(i),
            "source_version": 1,
            "operation": "upsert",
            "value": f"value-{i}",
        }
        for i in range(26)
    ]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))
    script = ROOT / "pipelines/apply-versioned-changes/main.py"
    outputs = []
    for expected in ("accepted", "duplicate"):
        result = subprocess.run(
            [
                "uv",
                "run",
                "--quiet",
                "--script",
                str(script),
                "--changes",
                str(source),
                "--state-dir",
                str(tmp_path / "state"),
            ],
            cwd=script.parent,
            env={key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"},
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        output = Path(
            next(
                line.removeprefix("Complete outcomes: ")
                for line in result.stdout.splitlines()
                if line.startswith("Complete outcomes: ")
            )
        )
        outcomes = pq.read_table(output).to_pylist()
        assert len(outcomes) == 26
        assert {row["change_id"] for row in outcomes} == {str(i) for i in range(26)}
        assert {row["outcome"] for row in outcomes} == {expected}
        outputs.append(output)
    assert outputs[0] != outputs[1] and outputs[0].is_dir()
