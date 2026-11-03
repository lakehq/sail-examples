"""Launch the runner for one target on one engine, in that engine's uv environment.

The Sail engine's dependencies come from `engines.toml`. To check another Sail
release without editing the pins, set SAIL_EXAMPLES_SAIL_DEPS to a JSON list of
requirements, for example '["pysail==0.7.3", "pyspark-client==4.1.3", "pyarrow==26.0.0", "pandas==3.0.6"]'.
"""

import json
import os
import shutil
import signal
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = Path(__file__).resolve().parent / "runner.py"
ENGINES = tomllib.loads((Path(__file__).resolve().parent / "engines.toml").read_text())
PYTHON = "3.12"
SAIL_DEPS_OVERRIDE = "SAIL_EXAMPLES_SAIL_DEPS"
# JSON Spark configuration that the runner applies to Spark sessions.
SPARK_CONF_VARIABLE = "SAIL_EXAMPLES_SPARK_CONF"


def engine_deps(engine: str) -> list[str]:
    if engine == "sail" and os.environ.get(SAIL_DEPS_OVERRIDE):
        deps = json.loads(os.environ[SAIL_DEPS_OVERRIDE])
        if not isinstance(deps, list) or not any(str(d).startswith("pysail") for d in deps):
            raise ValueError(f"{SAIL_DEPS_OVERRIDE} must be a JSON list that includes a pysail requirement")
        return [str(d) for d in deps]
    return list(ENGINES[engine]["deps"])


def spark_conf(extras: list[str] | None) -> dict[str, str]:
    """Spark configuration for the named extras in engines.toml (for example ["delta"])."""
    conf: dict[str, str] = {}
    for name in extras or []:
        conf.update(ENGINES["spark"]["extras"][name]["conf"])
    return conf


def java_home() -> str | None:
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    if shutil.which("/usr/libexec/java_home"):
        found = subprocess.run(
            ["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True, check=False
        )
        if found.returncode == 0:
            return found.stdout.strip()
    return None


def run(
    engine: str,
    kind: str,
    target: Path,
    out: Path,
    extra_deps: list[str],
    timeout: int,
    env_vars: dict[str, str] | None = None,
    spark_extras: list[str] | None = None,
) -> dict:
    """Run `target` (kind "example" or "fact") on `engine`; return the runner's meta.json.

    `env_vars` are set before the engine starts; `{tmp}` in a value becomes a fresh directory.
    `spark_extras` names entries of engines.toml [spark.extras] to configure Spark with (Sail ignores them).
    The run gets its own process group, so a timeout kills uv, the runner and any JVM it started.
    """
    command = ["uv", "run", "--quiet", "--no-project", "--isolated", "--python", PYTHON]
    for dep in engine_deps(engine) + extra_deps:
        command += ["--with", dep]
    command += ["python", str(RUNNER), "--engine", engine, f"--{kind}", str(target), "--out", str(out)]

    out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    scratch = out / "scratch"
    scratch.mkdir(exist_ok=True)
    env["TMPDIR"] = str(scratch)  # keep each run's scratch files inside its own output directory
    if engine == "spark" and java_home():
        env["JAVA_HOME"] = java_home()
    env.pop(SPARK_CONF_VARIABLE, None)  # only this run's extras, never a value inherited from the caller
    if engine == "spark" and spark_extras:
        env[SPARK_CONF_VARIABLE] = json.dumps(spark_conf(spark_extras))
    for key, value in (env_vars or {}).items():
        if "{tmp}" in value:
            (out / "tmp").mkdir(exist_ok=True)
            value = value.replace("{tmp}", str(out / "tmp"))
        env[key] = value

    proc = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:  # the group exited between the timeout and the kill
            pass
        proc.communicate()
        return {"engine": engine, "status": "timeout", "message": f"exceeded {timeout} s"}
    meta_path = out / "meta.json"
    if not meta_path.exists():
        text = stderr or stdout
        message = text if len(text) <= 6000 else text[:3000] + "\n...\n" + text[-3000:]
        return {"engine": engine, "status": "error", "message": message}
    return json.loads(meta_path.read_text())
