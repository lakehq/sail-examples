"""Each example's own entry point runs the way a reader would run it: `uv run main.py`."""

import os
import subprocess

import pytest
from catalog import discover_examples

EXAMPLES = [e for e in discover_examples() if not e.meta.get("needs_gpu")]


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_example_entry_point(example):
    key = example.meta.get("needs_key")
    if key and not os.environ.get(key):
        pytest.skip(f"needs {key}")
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    proc = subprocess.run(
        ["uv", "run", "--quiet", "--script", str(example.path)],
        cwd=example.path.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=example.meta.get("timeout", 120) + 180,
        check=False,
    )
    assert proc.returncode == 0, f"exit {proc.returncode}:\n{proc.stderr[-2000:]}"
    assert proc.stdout.strip(), "the entry point printed nothing"
