"""Literal ledger expectations and dual-engine reconciliation controls."""

from collections import Counter
from pathlib import Path

import pytest
from harness import run
from payment_reconciliation_checks import adversarial, example, literal_default

pytestmark = pytest.mark.examples("reconcile-orders-payments")


def test_python_ledger_literal_totals():
    module = example()
    literal_default(module.reference_rows(*module.fixture()))


def test_python_ledger_quarantines_conflicting_parent_ids():
    module = example()
    result = module.reference_rows(*adversarial(module))
    assert len(result["orders"]) == 1
    order = result["orders"][0]
    assert (order["captured_minor"], order["refunded_minor"], order["net_minor"]) == (1507, 500, 1007)
    assert Counter(r["reason"] for r in result["exceptions"])["conflicting_id"] == 4


def test_python_ledger_empty_is_explicit():
    module = example()
    assert module.reference_rows([], [], []) == {name: [] for name in module.OUTPUT_SCHEMAS}


def test_python_ledger_conflict_order_does_not_depend_on_arrival():
    module = example()
    snapshots = adversarial(module)
    expected = module.reference_rows(*snapshots)
    reversed_rows = [list(reversed(rows)) for rows in snapshots]
    assert module.reference_rows(*reversed_rows) == expected


@pytest.mark.parametrize("case", ["contract", "invalid", "empty_skew"])
@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_payment_reconciliation_contracts(case, engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("payment_reconciliation_checks.py"),
        tmp_path / engine,
        [],
        300,
        {"SAIL_EXAMPLES_PAYMENTS_CASE": case},
    )
    assert meta["status"] == "ok", f"{engine} {case} [{meta['status']}]: {meta.get('message')}"
