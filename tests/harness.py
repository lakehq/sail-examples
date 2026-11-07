"""Launch the runner for one target on one engine, in that engine's uv environment.

The Sail engine's dependencies come from `engines.toml`. To check another Sail
release without editing the pins, set SAIL_EXAMPLES_SAIL_DEPS to a JSON list of
requirements, for example '["pysail==0.7.3", "pyspark-client==4.1.3", "pyarrow==26.0.0", "pandas==3.0.6"]'.
"""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNNER = Path(__file__).resolve().parent / "runner.py"
ENGINES = tomllib.loads((Path(__file__).resolve().parent / "engines.toml").read_text())
PYTHON = (ROOT / ".python-version").read_text().strip()
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


def script_command(script: Path, *arguments: str) -> list[str]:
    """Keep reader commands pinned unless a release check explicitly replaces the Sail engine."""
    if not os.environ.get(SAIL_DEPS_OVERRIDE):
        return ["uv", "run", "--quiet", "--python", PYTHON, "--script", str(script), *arguments]

    from catalog import script_dependencies

    command = ["uv", "run", "--quiet", "--no-project", "--isolated", "--python", PYTHON]
    for dependency in engine_deps("sail") + script_dependencies(script):
        command += ["--with", dependency]
    return [*command, "python", str(script), *arguments]


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
    """Run an example, fact or entry-point capture; return the runner's meta.json.

    `env_vars` are set before the engine starts; `{tmp}` in a value becomes a fresh directory.
    `spark_extras` names entries of engines.toml [spark.extras] to configure Spark with (Sail ignores them).
    The run gets its own process group, so a timeout kills uv, the runner and any JVM it started.
    """
    command = ["uv", "run", "--quiet", "--no-project", "--isolated", "--python", PYTHON]
    for dep in engine_deps(engine) + extra_deps:
        command += ["--with", dep]
    command += ["python", str(RUNNER), "--engine", engine, f"--{kind}", str(target), "--out", str(out)]

    out.mkdir(parents=True, exist_ok=True)
    meta_path = out / "meta.json"
    meta_path.unlink(missing_ok=True)
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
    if not meta_path.exists():
        text = stderr or stdout
        message = text if len(text) <= 6000 else text[:3000] + "\n...\n" + text[-3000:]
        return {"engine": engine, "status": "error", "message": message}
    return json.loads(meta_path.read_text())


def release_key(version: str) -> tuple[int, ...]:
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError(f"Use a stable Sail release such as 0.7.3, got {version!r}")
    return tuple(int(part) for part in version.split("."))


def capture_key(run: dict) -> tuple[tuple[int, ...], int]:
    revision = run.get("revision", 1)
    if type(revision) is not int or revision < 1:
        raise ValueError("Capture revision must be a positive integer")
    return release_key(run["sail"]), revision


def record_output(
    path: Path, capture: dict, variant: str = "default", *, revision_reason: str | None = None
) -> None:
    """Append reviewed revisions without replacing earlier captures."""
    from output_comparison import comparable_output

    if revision_reason is not None and not revision_reason.strip():
        raise ValueError("Give a reason for accepting a revised capture")
    document = json.loads(path.read_text()) if path.exists() else {"snapshots": []}
    history = document
    if variant != "default":
        history = document.setdefault("variants", {}).setdefault(variant, {"snapshots": []})
    snapshots = history["snapshots"]
    captures = [
        {"output": snapshot["output"], "run": run} for snapshot in snapshots for run in snapshot["runs"]
    ]
    version = capture["run"]["sail"]
    release_key(version)
    same_version = [item for item in captures if item["run"]["sail"] == version]
    capture = {"output": capture["output"], "run": dict(capture["run"])}
    if same_version:
        previous = max(same_version, key=lambda item: capture_key(item["run"]))
        same_output = comparable_output(previous["output"]) == comparable_output(capture["output"])
        same_source = previous["run"]["script_sha256"] == capture["run"]["script_sha256"]
        if same_output and same_source:
            if version in history.get("failed_versions", []):
                history["failed_versions"].remove(version)
                save_output_history(path, document)
            return
        if revision_reason is None:
            raise ValueError(
                f"Sail {version} already has a different capture in {path}; "
                "review the output change, then use --accept-revision with a reason to preserve it as a new revision"
            )
        capture["run"]["revision"] = capture_key(previous["run"])[1] + 1
        capture["run"]["revision_reason"] = revision_reason.strip()
    captures.append(capture)
    captures.sort(key=lambda item: capture_key(item["run"]))
    grouped = []
    for item in captures:
        if grouped and grouped[-1]["output"] == item["output"]:
            grouped[-1]["runs"].append(item["run"])
        else:
            grouped.append({"output": item["output"], "runs": [item["run"]]})
    history["snapshots"] = grouped
    history["failed_versions"] = [item for item in history.get("failed_versions", []) if item != version]
    save_output_history(path, document)


def record_output_failure(path: Path, version: str, variant: str = "default") -> None:
    release_key(version)
    document = json.loads(path.read_text()) if path.exists() else {"snapshots": []}
    history = document
    if variant != "default":
        history = document.setdefault("variants", {}).setdefault(variant, {"snapshots": []})
    history["failed_versions"] = sorted(set(history.get("failed_versions", []) + [version]), key=release_key)
    save_output_history(path, document)


def save_output_history(path: Path, history: dict) -> None:
    for section in [history, *history.get("variants", {}).values()]:
        if not section.get("failed_versions"):
            section.pop("failed_versions", None)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(history, indent=2) + "\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> None:
    from catalog import discover_examples
    from output_presentation import update_readme_outputs, variant_script, variant_sources

    parser = argparse.ArgumentParser(description="Capture Sail output and generate README output sections")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--capture-output", help="Example ID")
    action.add_argument(
        "--render-readmes", action="store_true", help="Render accepted captures without running examples"
    )
    parser.add_argument("--check", action="store_true", help="Check README output sections without writing")
    parser.add_argument("--sail-version")
    parser.add_argument(
        "--accept-revision",
        metavar="REASON",
        help="Accept a reviewed change on an already captured Sail release",
    )
    parser.add_argument(
        "--variant", default="default", help="Named README diff, or default for the unchanged script"
    )
    args = parser.parse_args()
    examples = {example.id: example for example in discover_examples()}
    if args.render_readmes:
        if args.sail_version or args.variant != "default" or args.accept_revision is not None:
            parser.error("--sail-version, --variant and --accept-revision apply only to --capture-output")
        changed = [
            example.id
            for example in examples.values()
            if update_readme_outputs(example.path, check=args.check)
        ]
        if args.check and changed:
            raise SystemExit(
                "README output sections are stale: "
                + ", ".join(changed)
                + ". Run uv run python tests/harness.py --render-readmes."
            )
        print(f"{'Checked' if args.check else 'Updated'} README output sections ({len(changed)} changed)")
        return
    if args.check or not args.sail_version:
        parser.error("--capture-output requires --sail-version; --check applies only to --render-readmes")
    release_key(args.sail_version)
    if args.accept_revision is not None and not args.accept_revision.strip():
        parser.error("--accept-revision requires a nonempty review reason")
    if args.capture_output not in examples:
        parser.error(f"Unknown example: {args.capture_output}")
    example = examples[args.capture_output]
    if args.variant != "default" and args.variant not in variant_sources(example.path):
        parser.error(f"Unknown output variant: {args.variant}")
    if example.meta.get("needs_key"):
        parser.error("Credential-dependent examples need a reviewed public dataset before recording output")
    deps = [
        f"pysail=={args.sail_version}" if dependency.startswith("pysail==") else dependency
        for dependency in ENGINES["sail"]["deps"]
    ]
    os.environ[SAIL_DEPS_OVERRIDE] = json.dumps(deps)
    out = ROOT / ".reports" / "output-captures" / example.id / args.sail_version
    if args.variant != "default":
        out = out / args.variant
    with variant_script(example.path, args.variant) as script:
        meta = run(
            "sail", "entry-point", script, out, example.extra_deps, example.meta.get("timeout", 120) + 180
        )
    if meta["status"] != "ok":
        record_output_failure(example.path.with_name("outputs.json"), args.sail_version, args.variant)
        update_readme_outputs(example.path)
        raise SystemExit(f"No output recorded: {meta.get('message', meta['status'])}")
    capture = meta["capture"]
    if capture["run"]["sail"] != args.sail_version:
        raise SystemExit("Resolved Sail version differs from the requested version; no output recorded")
    destination = example.path.with_name("outputs.json")
    record_output(destination, capture, args.variant, revision_reason=args.accept_revision)
    update_readme_outputs(example.path)
    print(
        f"Recorded Sail {args.sail_version} output ({args.variant}) in {destination} and updated its README"
    )


if __name__ == "__main__":
    main()
