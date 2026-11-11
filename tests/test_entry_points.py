"""Each example's own entry point runs the way a reader would run it: `uv run main.py`."""

import json
import os
import subprocess

import pytest
from catalog import ROOT, discover_examples
from harness import engine_deps, release_key, script_command
from output_comparison import compare_output
from output_presentation import variant_script, variant_sources

EXAMPLES = [e for e in discover_examples() if not e.meta.get("needs_gpu")]
CASES = [
    (example, variant) for example in EXAMPLES for variant in ["default", *variant_sources(example.path)]
]


@pytest.mark.parametrize(
    "example,variant",
    CASES,
    ids=[example.id + (f"-{variant}" if variant != "default" else "") for example, variant in CASES],
)
def test_example_entry_point(example, variant):
    key = example.meta.get("needs_key")
    if key and not os.environ.get(key):
        pytest.skip(f"needs {key}")
    sail_version = next(
        dep.removeprefix("pysail==") for dep in engine_deps("sail") if dep.startswith("pysail==")
    )
    release_key(sail_version)
    report = ROOT / ".reports" / "output-comparisons" / example.path.parent.relative_to(ROOT)
    if variant != "default":
        report = report / variant
    report.mkdir(parents=True, exist_ok=True)
    for name in ("expected.txt", "actual.txt", "output.diff", "stderr.txt"):
        (report / name).unlink(missing_ok=True)
    (report / "result.json").write_text(json.dumps({"sail": sail_version, "status": "execution-incomplete"}))
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    with variant_script(example.path, variant) as script:
        proc = subprocess.run(
            script_command(script),
            cwd=script.parent,
            env=env,
            capture_output=True,
            text=True,
            timeout=example.meta.get("timeout", 120) + 180,
            check=False,
        )
    (report / "actual.txt").write_text(proc.stdout)
    (report / "stderr.txt").write_text(proc.stderr)
    assert proc.returncode == 0, f"exit {proc.returncode}:\n{proc.stderr[-2000:]}"
    assert proc.stdout.strip(), "the entry point printed nothing"
    result = compare_output(example.path, proc.stdout, sail_version, report, variant)
    difference = (report / "output.diff").read_text()
    preview = "\n".join(difference.splitlines()[:80])
    source_note = (
        " The script also differs from the reviewed capture." if result.get("source_changed") else ""
    )
    assert result["status"] != "changed", (
        f"Console output changed.{source_note} Review {report / 'output.diff'} before accepting a new capture.\n"
        + preview
    )
