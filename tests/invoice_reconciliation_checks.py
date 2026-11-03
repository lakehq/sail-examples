"""Independent source fields and hand-specified reconciliation outcomes, on each engine."""

import importlib.util
import json
import tempfile
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "pipelines/reconcile-invoice-documents/main.py"
    spec = importlib.util.spec_from_file_location("invoice_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(spark, engine):
    module = load_example()
    gold = json.loads((module.HERE / "fixtures/source-gold.json").read_text())["rows"]
    with tempfile.TemporaryDirectory(prefix="invoice-check-") as directory:
        root = Path(directory)
        outputs = module.run(spark, root)
        documents = [row.asDict() for row in outputs["documents"].collect()]
        expected = {
            "doc-acme": "duplicate_invoice",
            "doc-acme-alias": "duplicate_invoice",
            "doc-northwind": "amount_mismatch",
            "doc-pennine": "missing_po",
            "doc-mersey": "match",
            "doc-corrupt": "extraction_error",
            "doc-blank": "extraction_error",
        }
        assert len(documents) == 7 and {row["document_id"]: row["status"] for row in documents} == expected
        by_source = {row["source_id"]: row for row in documents}
        for answer in gold:
            actual = by_source[answer["source_id"]]
            assert {key: actual[key] for key in answer} == answer, (engine, answer["source_id"], actual)
            assert actual["extraction_status"] == "ok" and actual["invoice_evidence"] == answer["invoice_id"]
            assert actual["total_evidence"], "missing OCR TOTAL evidence"
        structured = [row.asDict() for row in outputs["structured"].collect()]
        assert len(structured) == 6 and {row["document_id"]: row["status"] for row in structured} == {
            "structured-match": "match",
            "structured-duplicate-a": "duplicate_invoice",
            "structured-duplicate-b": "duplicate_invoice",
            "structured-missing-po": "missing_po",
            "structured-wrong-amount": "amount_mismatch",
            "structured-invalid": "extraction_error",
        }
        assert sum(row["gross_minor"] for row in documents if row["status"] == "match") == 1499034
        old_pointer = (root / "CURRENT").read_bytes()
        old_generation = module.current_run(root)
        try:
            module.run(spark, root, fail_before_publish=True)
        except RuntimeError as error:
            assert "before CURRENT" in str(error)
        else:
            raise AssertionError("injected prepublication failure was not observed")
        assert (root / "CURRENT").read_bytes() == old_pointer and old_generation.is_dir()
        assert spark.read.parquet(str(old_generation / "documents.parquet")).count() == 7

        # Independent canonical inputs test rejection and join behavior without any OCR call.
        def row(
            name, supplier, observed, invoice, po="PO-STRUCT-USD", currency="USD", gross=5000, status="ok"
        ):
            return {
                **dict.fromkeys(module.FIELDS),
                "document_id": name,
                "source_id": "authored-test",
                "supplier_id": supplier,
                "observed_supplier_id": observed,
                "invoice_id": invoice,
                "po_id": po,
                "currency": currency,
                "gross_minor": gross,
                "extraction_status": status,
            }

        cases = [
            row("good", "S-ACME", "S-ACME", "CHECK-001"),
            row("null-invoice-a", "S-ACME", "S-ACME", None),
            row("null-invoice-b", "S-ACME", "S-ACME", None),
            row("null-gross", "S-ACME", "S-ACME", "CHECK-002", gross=None),
            row("null-supplier", "S-ACME", None, "CHECK-003"),
            row("unknown", "S-UNKNOWN", "S-UNKNOWN", "CHECK-004"),
            row("supplier", "S-ACME", "S-MERSEY", "CHECK-005"),
            row("po-supplier", "S-MERSEY", "S-MERSEY", "CHECK-006"),
            row("currency", "S-ACME", "S-ACME", "CHECK-007", currency="GBP"),
            row("no-po", "S-ACME", "S-ACME", "CHECK-008", po=None),
            row("amount", "S-ACME", "S-ACME", "CHECK-009", gross=4999),
            row("duplicate-a", "S-ACME", "S-ACME", "CHECK-DUP"),
            row("duplicate-b", "S-ACME", "S-ACME", "CHECK-DUP"),
            row("error-priority", "S-ACME", "S-ACME", "CHECK-DUP", status="error"),
        ]
        report = module.reconcile(spark, spark.createDataFrame(cases, module.SCHEMA))
        states = {value.document_id: value.status for value in report.collect()}
        assert len(states) == len(cases) and states == {
            "good": "match",
            "null-invoice-a": "extraction_error",
            "null-invoice-b": "extraction_error",
            "null-gross": "extraction_error",
            "null-supplier": "extraction_error",
            "unknown": "unknown_supplier",
            "supplier": "supplier_mismatch",
            "po-supplier": "po_supplier_mismatch",
            "currency": "currency_mismatch",
            "no-po": "missing_po",
            "amount": "amount_mismatch",
            "duplicate-a": "duplicate_invoice",
            "duplicate-b": "duplicate_invoice",
            "error-priority": "extraction_error",
        }, states
        intake = {
            "document_id": "missing-invoice-number",
            "source_id": "authored-test",
            "supplier_id": "S-ACME",
            "po_id": "PO-ACME",
            "currency": "USD",
        }
        try:
            fields = module.parse_invoice(
                ["Acme Industrial Supply Co", "Invoice #:", "TOTAL", "$372.49"], "USD"
            )
        except module.DocumentError as error:
            malformed = module.canonical_row(intake, error=str(error))
        else:
            raise AssertionError(f"missing invoice number accepted as {fields[1]}")
        rejected = module.reconcile(spark, spark.createDataFrame([malformed], module.SCHEMA)).first()
        assert rejected.status == "extraction_error" and rejected.invoice_id is None
        for orders in (module.PURCHASE_ORDERS + [module.PURCHASE_ORDERS[0]],):
            try:
                module.reconcile(spark, spark.createDataFrame(cases, module.SCHEMA), purchase_orders=orders)
            except ValueError as error:
                assert "unique" in str(error)
            else:
                raise AssertionError("duplicate PO dimension fanned out report rows")
    return {
        "engine": engine,
        "document_inputs": 7,
        "source_gold": 4,
        "structured_inputs": 6,
        "adversarial_inputs": 14,
        "old_report_survived_failure": True,
    }
