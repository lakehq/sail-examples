"""Run one example or one fact check on one engine, in that engine's environment.

The harness calls this script through `uv run` with the engine's dependencies.
It must only import the standard library, pyspark, pyarrow and the engine
package, because it runs inside the engine environment, not the dev one.

    python runner.py --engine sail  --example quickstart/hello-sail/main.py --out /tmp/x
    python runner.py --engine spark --fact facts/cache-is-a-no-op.py --out /tmp/y
"""

import argparse
import contextlib
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import os
import platform
import runpy
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

_SERVER = None  # keeps the in-process Sail server alive for the whole run


def make_session(engine: str):
    global _SERVER
    from pyspark.sql import SparkSession

    if engine == "sail":
        from pysail.spark import SparkConnectServer

        _SERVER = SparkConnectServer("127.0.0.1", 0)
        _SERVER.start(background=True)
        _, port = _SERVER.listening_address
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    elif engine == "spark":
        builder = SparkSession.builder.remote("local[4]")
        # Extras such as Delta Lake, chosen by the harness (tests/engines.toml [spark.extras]).
        for key, value in json.loads(os.environ.get("SAIL_EXAMPLES_SPARK_CONF", "{}")).items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
    else:
        raise ValueError(f"unknown engine {engine!r}")
    # Examples must not depend on the host's time zone.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def engine_versions(engine: str) -> dict:
    import pyspark

    versions = {"python": platform.python_version(), "pyspark": pyspark.__version__}
    if engine == "sail":
        import pysail

        versions["pysail"] = getattr(pysail, "__version__", "unknown")
    return versions


def load_module(path: Path):
    spec = importlib.util.spec_from_file_location(f"_target_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    spec.loader.exec_module(module)
    return module


def write_results(result, out: Path) -> list[str]:
    import pyarrow.parquet as pq

    frames = result if isinstance(result, dict) else {"result": result}
    names = []
    for name, frame in frames.items():
        pq.write_table(frame.toArrow(), out / f"{name}.parquet")
        names.append(name)
    return names


def write_references(reference: dict, out: Path) -> list[str]:
    """Write an example's independent reference outputs (pyarrow tables or lists of row dicts)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    for name, value in reference.items():
        table = value if isinstance(value, pa.Table) else pa.Table.from_pylist(value)
        pq.write_table(table, out / f"reference-{name}.parquet")
    return list(reference)


def capture_entry_point(path: Path) -> dict:
    """Capture the reader's command, keeping diagnostics out of the displayed output."""
    stdout = io.StringIO()
    script_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    previous_argv = sys.argv
    sys.argv = [str(path)]
    sys.path.insert(0, str(path.parent))
    try:
        with contextlib.redirect_stdout(stdout):
            try:
                runpy.run_path(str(path), run_name="__main__")
            except SystemExit as error:
                if error.code not in (None, 0):
                    raise RuntimeError(f"Entry point exited with {error.code}") from error
    finally:
        sys.argv = previous_argv
    output = stdout.getvalue()
    if not output.strip():
        raise ValueError("The entry point printed no output; no snapshot was recorded")
    return {
        "output": output,
        "run": {
            "sail": importlib.metadata.version("pysail"),
            "python": platform.python_version(),
            "captured_at": datetime.now(UTC).isoformat(),
            "script_sha256": script_hash,
            "packages": dict(
                sorted((dist.metadata["Name"], dist.version) for dist in importlib.metadata.distributions())
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, choices=["sail", "spark"])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--example", type=Path)
    target.add_argument("--fact", type=Path)
    target.add_argument("--entry-point", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    meta = {"engine": args.engine, "status": "error", "versions": engine_versions(args.engine)}
    started = time.monotonic()
    spark = None
    try:
        path = (args.example or args.fact or args.entry_point).resolve()
        os.chdir(path.parent)
        if args.entry_point:
            if args.engine != "sail":
                raise ValueError("Output snapshots require the Sail engine")
            meta["capture"] = capture_entry_point(path)
        else:
            module = load_module(path)
            spark = make_session(args.engine)
            if args.example:
                meta["outputs"] = write_results(module.run(spark), args.out)
                if hasattr(module, "reference"):
                    meta["references"] = write_references(module.reference(), args.out)
            else:
                meta["observed"] = module.check(spark, args.engine)
        meta["status"] = "ok"
    except AssertionError as error:
        meta["status"] = "failed"
        meta["message"] = str(error)
    except Exception:  # noqa: BLE001 - every error is reported back to the harness
        text = traceback.format_exc()
        # Keep the head (where JVM errors put the cause) and the tail (the Python frames).
        meta["message"] = text if len(text) <= 6000 else text[:3000] + "\n...\n" + text[-3000:]
    finally:
        meta["seconds"] = round(time.monotonic() - started, 2)
        if spark is not None:
            spark.stop()
        if _SERVER is not None:
            _SERVER.stop()
    (args.out / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
    return 0 if meta["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
