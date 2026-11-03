"""Every fact about Sail is re-checked on every engine it names.

A fact's `check(spark, engine)` raises AssertionError when the fact no longer
holds, for example after a Sail release adds a missing feature. The fact's
text must then be updated before CI goes green again.

Set SAIL_EXAMPLES_FACTS_DIR to check facts from another directory.
"""

import os
from pathlib import Path

import pytest
from catalog import ROOT, discover_facts
from harness import run

FACTS = discover_facts(Path(os.environ.get("SAIL_EXAMPLES_FACTS_DIR", ROOT / "facts")))


@pytest.mark.parametrize(
    ("fact", "engine"),
    [(fact, engine) for fact in FACTS for engine in fact.engines],
    ids=[f"{fact.id}[{engine}]" for fact in FACTS for engine in fact.engines],
)
def test_fact(fact, engine, tmp_path):
    timeout = fact.meta.get("timeout", 60) + 180
    extras = fact.meta.get("spark_extras")
    meta = run(engine, "fact", fact.path, tmp_path, fact.extra_deps, timeout, fact.meta.get("env"), extras)
    assert meta["status"] == "ok", f"{fact.id} on {engine} [{meta['status']}]: {meta.get('message')}"
