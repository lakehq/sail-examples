"""Small independent controls for ordered events and investigable rejected rows."""

from pathlib import Path

import pytest
from harness import run


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            case,
            marks=pytest.mark.examples("taxi-etl-job" if case == "rejects" else "sessions-funnels-retention"),
        )
        for case in ("funnel", "retention", "rejects", "deferred")
    ],
)
@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_analytics_and_etl_contracts(case, engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("analytics_etl_checks.py"),
        tmp_path / engine,
        [],
        300,
        {"SAIL_EXAMPLES_TABULAR_CASE": case},
    )
    assert meta["status"] == "ok", f"{engine} {case} [{meta['status']}]: {meta.get('message')}"
