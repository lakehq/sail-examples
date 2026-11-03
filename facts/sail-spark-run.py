FACT = {
    "id": "sail-spark-run",
    "category": "run-and-connect",
    "text": """`sail spark run -f script.py` (or `-f -` to read the script from stdin) starts a temporary
        Sail server, runs the script with a ready `spark` session, and stops the server. A
        `WARN ... aborting N task(s)` line at exit is harmless.""",
    "engines": ["sail"],
    "order": 2,
}

import subprocess
import sys
import tempfile
from pathlib import Path


def check(spark, engine):
    script = Path(tempfile.mkdtemp()) / "job.py"
    script.write_text("print('rows', spark.range(5).count())\n")
    sail = Path(sys.executable).parent / "sail"
    proc = subprocess.run(
        [str(sail), "spark", "run", "-f", str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, f"exit {proc.returncode}: {proc.stderr[-500:]}"
    assert "rows 5" in proc.stdout, f"unexpected output: {proc.stdout[-300:]}"
    stdin = subprocess.run(
        [str(sail), "spark", "run", "-f", "-"],
        input=script.read_text(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert stdin.returncode == 0 and "rows 5" in stdin.stdout, f"-f - failed: {stdin.stderr[-300:]}"
    return {"exit": proc.returncode, "warn_at_exit": "aborting" in proc.stderr}
