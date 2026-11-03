"""Maintained vectors agree with fresh inference and survive unpublished updates."""

from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run


def run_case(engine, phase, tmp_path, state):
    return run(
        engine,
        "fact",
        Path(__file__).with_name("embedding_maintenance_checks.py"),
        tmp_path,
        [] if phase == "inputs" else script_dependencies(ROOT / "ai/maintain-document-embeddings/main.py"),
        300,
        {
            "EMBEDDING_CHECK_PHASE": phase,
            "EMBEDDING_CHECK_ROOT": str(state),
            "SAIL_EXECUTION__BATCH_SIZE": "37",
            **({"HF_HUB_OFFLINE": "1"} if phase == "inputs" else {}),
        },
    )


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_embedding_input_contracts_without_model(engine, tmp_path):
    meta = run_case(engine, "inputs", tmp_path / "run", tmp_path / "durable")
    assert meta["status"] == "ok", f"{engine}: {meta}"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_embedding_updates_match_full_cpu_recomputation(engine, tmp_path):
    meta = run_case(engine, "maintenance", tmp_path / "run", tmp_path / "durable")
    assert meta["status"] == "ok", f"{engine}: {meta}"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_embedding_publication_failure_reopens_in_new_process(engine, tmp_path):
    for phase in ("interrupt", "reopen"):
        meta = run_case(engine, phase, tmp_path / phase, tmp_path / "durable")
        assert meta["status"] == "ok", f"{engine} {phase}: {meta}"
    assert (tmp_path / "interrupt/meta.json").is_file()
    assert (tmp_path / "reopen/meta.json").is_file()
