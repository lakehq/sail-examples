"""Compare console output with reviewed captures without updating their history."""

import difflib
import hashlib
import json
import platform
import re
from pathlib import Path

from harness import capture_key, release_key


def reviewed_capture(history: Path, sail_version: str, variant: str = "default"):
    target = release_key(sail_version)
    if not history.exists():
        return None
    document = json.loads(history.read_text())
    if variant != "default":
        document = document.get("variants", {}).get(variant, {"snapshots": []})
    candidates = [
        (capture_key(run), snapshot["output"], run)
        for snapshot in document["snapshots"]
        for run in snapshot["runs"]
        if release_key(run["sail"]) <= target
    ]
    if not candidates:
        return None
    _, output, run = max(candidates, key=lambda item: item[0])
    return {"output": output, "run": run}


def comparable_output(text: str) -> str:
    text = text.replace("\r\n", "\n")
    # Pytest's success-summary duration varies independently of the example's results.
    return re.sub(r"(?m)^(\d+ passed) in \d+(?:\.\d+)?s(?=\n|\Z)", r"\1 in <elapsed>s", text)


def compare_output(
    script: Path, actual: str, sail_version: str, report: Path, variant: str = "default"
) -> dict:
    from output_presentation import variant_sources

    baseline = reviewed_capture(script.with_name("outputs.json"), sail_version, variant)
    source = script.read_bytes() if variant == "default" else variant_sources(script)[variant].encode()
    result = {
        "sail": sail_version,
        "python": platform.python_version(),
        "script_sha256": hashlib.sha256(source).hexdigest(),
        "variant": variant,
        "status": "missing-baseline",
        "normalization": ["CRLF line endings", "pytest success-summary elapsed time"],
    }
    report.mkdir(parents=True, exist_ok=True)
    (report / "actual.txt").write_text(actual)
    expected = ""
    difference = ""
    if baseline is not None:
        expected = baseline["output"]
        result["baseline"] = baseline["run"]
        result["source_changed"] = baseline["run"]["script_sha256"] != result["script_sha256"]
        before = comparable_output(expected)
        after = comparable_output(actual)
        result["status"] = "matched" if before == after else "changed"
        lines = difflib.unified_diff(
            before.splitlines(keepends=True),
            after.splitlines(keepends=True),
            fromfile=f"reviewed output (Sail {baseline['run']['sail']})",
            tofile=f"current output (Sail {sail_version})",
        )
        difference = "".join(
            line if line.endswith("\n") else line + "\n\\ No newline at end of file\n" for line in lines
        )
    (report / "expected.txt").write_text(expected)
    (report / "output.diff").write_text(difference)
    (report / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result
