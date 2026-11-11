"""The robotics transforms must reject broken records and preserve unmatched samples."""

from pathlib import Path

import pytest
from harness import run

pytestmark = pytest.mark.examples(
    "curate-robot-episodes", "align-robot-camera-frames", "curate-humanoid-episodes"
)


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_robot_negative_controls(engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("robot_checks.py"),
        tmp_path / engine,
        ["av==18.0.0", "numpy==2.5.4"],
        300,
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"
