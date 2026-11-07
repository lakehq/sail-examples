"""Model inference preserves rows while bounding tensors inside larger Arrow batches."""

from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run
from inference_checks import CASES


@pytest.mark.parametrize(
    "case",
    [pytest.param(case, marks=pytest.mark.examples(Path(values[0]).name)) for case, values in CASES.items()],
)
@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_bounded_model_inference(case, engine, tmp_path):
    directory = CASES[case][0]
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("inference_checks.py"),
        tmp_path / engine,
        script_dependencies(ROOT / directory / "main.py"),
        300,
        {"SAIL_EXAMPLES_INFERENCE_CASE": case},
    )
    assert meta["status"] == "ok", f"{engine} {case} [{meta['status']}]: {meta.get('message')}"
