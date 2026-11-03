"""Every example runs on Sail and, unless it is Sail-only, on Apache Spark; results must match."""

import os

import pyarrow.parquet as pq
import pytest
from catalog import discover_examples
from compare import Tolerance, compare_tables
from harness import run
from quality_checks import QUALITY_EXAMPLES, check_quality

EXAMPLES = discover_examples()


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_example(example, tmp_path):
    if example.meta.get("needs_gpu"):
        pytest.skip("needs a GPU")
    key = example.meta.get("needs_key")
    if key and not os.environ.get(key):
        pytest.skip(f"needs {key}")

    timeout = example.meta.get("timeout", 120) + 180  # headroom for environment setup
    tol = Tolerance(rel=float(example.meta.get("float_tolerance", 1e-9)))
    ordered = bool(example.meta.get("ordered"))
    results = {}
    for engine in example.engines:
        extras = example.meta.get("spark_extras")
        meta = run(
            engine, "example", example.path, tmp_path / engine, example.extra_deps, timeout, None, extras
        )
        assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"
        results[engine] = meta
        if example.id in QUALITY_EXAMPLES:
            check_quality(
                example.id,
                {name: pq.read_table(tmp_path / engine / f"{name}.parquet") for name in meta["outputs"]},
            )
        if example.engines != ("sail",) and meta.get("references"):
            assert set(meta["references"]) == set(meta["outputs"]), "reference output names differ"
            for name in meta["references"]:
                actual = pq.read_table(tmp_path / engine / f"{name}.parquet")
                expected = pq.read_table(tmp_path / engine / f"reference-{name}.parquet")
                problems = compare_tables(actual, expected, ordered=ordered, tol=tol)
                assert not problems, f"{engine} {name}: independent reference differs:\n" + "\n".join(
                    problems
                )

    if example.engines == ("sail",):
        # A Sail-only example has no Spark result to match, so it must match an independent reference.
        references = results["sail"].get("references") or []
        assert references, "a Sail-only example must define reference()"
        for name in references:
            sail = pq.read_table(tmp_path / "sail" / f"{name}.parquet")
            expected = pq.read_table(tmp_path / "sail" / f"reference-{name}.parquet")
            problems = compare_tables(sail, expected, ordered=ordered, tol=tol)
            assert not problems, f"{name}: Sail and the reference differ:\n" + "\n".join(problems)
        return
    for name in results["sail"]["outputs"]:
        sail = pq.read_table(tmp_path / "sail" / f"{name}.parquet")
        spark = pq.read_table(tmp_path / "spark" / f"{name}.parquet")
        problems = compare_tables(sail, spark, ordered=ordered, tol=tol)
        assert not problems, f"{name}: Sail and Spark differ:\n" + "\n".join(problems)
