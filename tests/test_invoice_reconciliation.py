"""Invoice parsing, cache and publication boundaries, plus scheduled real-engine checks."""

import importlib.util
import json
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run

pytestmark = pytest.mark.examples("reconcile-invoice-documents")

EXAMPLE = ROOT / "pipelines/reconcile-invoice-documents"
LINES = {
    "clean-us-clean-simple": [
        "Acme Industrial Supply Co",
        "INVOICE",
        "Invoice #:",
        "ACM-100145",
        "TOTAL",
        "$372.49",
    ],
    "clean-us-with-shipping": [
        "INVOICE",
        "Northwind Logistics LLC",
        "Invoice #:",
        "NWL-20240318-002",
        "TOTAL",
        "$1,599.43",
    ],
    "clean-gb-serif": [
        "Pennine Tyre & Parts Limited",
        "TAX INVOICE",
        "Invoice #:",
        "IN0049923",
        "TOTAL",
        "£21,613.90",
    ],
    "clean-gb-mono-plain": [
        "Mersey",
        "Office Supplies Ltd",
        "INVOICE",
        "Invoice Number:",
        "IN0003904",
        "TOTAL",
        "£14,990.34",
    ],
}


@pytest.fixture
def example():
    spec = importlib.util.spec_from_file_location("invoice_example", EXAMPLE / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "gold",
    json.loads((EXAMPLE / "fixtures/source-gold.json").read_text())["rows"],
    ids=lambda row: row["source_id"],
)
def test_parser_agrees_with_independent_source_gold(example, gold):
    supplier, invoice, gross, invoice_evidence, total_evidence = example.parse_invoice(
        LINES[gold["source_id"]], gold["currency"]
    )
    assert (supplier, invoice, gross) == (
        gold["observed_supplier_id"],
        gold["invoice_id"],
        gold["gross_minor"],
    )
    assert invoice_evidence == gold["invoice_id"] and total_evidence == LINES[gold["source_id"]][-1]


@pytest.mark.parametrize("amount", ["1,59.43", "NaN", "1.001", "-1.00", "0.00", "1e3", "1.0", "1,0000.00"])
def test_amounts_never_round_or_accept_malformed_gross_values(example, amount):
    with pytest.raises(example.DocumentError):
        example.minor_units(amount, "USD")


@pytest.mark.parametrize(
    "mutation",
    [
        "two_totals",
        "two_invoice_labels",
        "two_headers",
        "header_after_eight",
        "wrong_symbol",
        "unsupported_currency",
        "missing_total",
        "bad_invoice",
    ],
)
def test_ambiguous_or_unsupported_extraction_is_rejected(example, mutation):
    lines, currency = list(LINES["clean-us-clean-simple"]), "USD"
    if mutation == "two_totals":
        lines += ["TOTAL", "$372.49"]
    elif mutation == "two_invoice_labels":
        lines += ["Invoice #:", "ACM-100145"]
    elif mutation == "two_headers":
        lines.insert(0, "Northwind Logistics LLC")
    elif mutation == "header_after_eight":
        lines = ["other"] * 8 + lines
    elif mutation == "wrong_symbol":
        lines[-1] = "£372.49"
    elif mutation == "unsupported_currency":
        currency = "CAD"
    elif mutation == "missing_total":
        lines[-2] = "Subtotal"
    elif mutation == "bad_invoice":
        lines[3] = "ACM 100145"
    with pytest.raises(example.DocumentError):
        example.parse_invoice(lines, currency)


def test_blank_input_is_an_explicit_failure(example):
    with pytest.raises(example.DocumentError, match="blank"):
        example.parse_invoice([], "USD")


@pytest.mark.parametrize("label", ["Invoice #:", "Invoice Number:"])
def test_missing_invoice_value_cannot_reuse_total_label(example, label):
    with pytest.raises(example.DocumentError, match="invoice_or_total_label_missing_or_ambiguous"):
        example.parse_invoice(["Acme Industrial Supply Co", label, "TOTAL", "$372.49"], "USD")


def test_cache_shares_source_bytes_between_aliases_and_rejects_corrupt_entries(example, tmp_path):
    content, calls = b"test bytes, not opened as a PDF by this boundary test", []
    first, alias = tmp_path / "first.pdf", tmp_path / "alias.pdf"
    first.write_bytes(content)
    alias.write_bytes(content)

    def recognize(data, engine):
        calls.append(data)
        return LINES["clean-us-clean-simple"]

    cache, source_sha = tmp_path / "cache", example.digest(content)
    assert example.cached_lines(first, source_sha, cache, None, recognize) == LINES["clean-us-clean-simple"]
    assert example.cached_lines(alias, source_sha, cache, None, recognize) == LINES["clean-us-clean-simple"]
    assert len(calls) == 1
    entry = next(cache.glob("*.json"))
    record = json.loads(entry.read_text())
    record["lines"][-1] = "$0.01"
    entry.write_text(json.dumps(record))
    assert example.cached_lines(first, source_sha, cache, None, recognize)[-1] == "$372.49"
    assert len(calls) == 2


def test_source_and_script_changes_do_not_reuse_cached_evidence(example, tmp_path, monkeypatch):
    path, cache = tmp_path / "input.pdf", tmp_path / "cache"
    path.write_bytes(b"original")
    calls = []

    def recognize(*args):
        calls.append(True)
        return []

    source_sha = example.digest(path.read_bytes())
    example.cached_lines(path, source_sha, cache, None, recognize)
    monkeypatch.setattr(example, "SCRIPT_ID", "0" * 64)
    example.cached_lines(path, source_sha, cache, None, recognize)
    assert len(calls) == 2
    path.write_bytes(b"changed")
    with pytest.raises(example.DocumentError, match="source_sha256"):
        example.cached_lines(path, source_sha, cache, None, recognize)
    assert len(calls) == 2


def test_pdf_byte_limit_precedes_native_parsing(example, tmp_path):
    path = tmp_path / "large.pdf"
    path.write_bytes(b"x" * (example.MAX_PDF_BYTES + 1))
    with pytest.raises(example.DocumentError, match="pdf_byte_limit"):
        example.read_pdf_bytes(path, example.digest(path.read_bytes()))


@pytest.mark.parametrize(
    "pages,size,error", [(2, (612, 792), "single_page"), (1, (4000, 4000), "render_pixel")]
)
def test_page_limits_precede_bitmap_allocation_and_close_under_lock(example, monkeypatch, pages, size, error):
    calls = []
    lock = example.process_lock("pdfium")

    class Page:
        def get_size(self):
            assert lock.locked()
            return size

        def render(self, **kwargs):
            pytest.fail("an unsupported document allocated a bitmap")

        def close(self):
            assert lock.locked()
            calls.append("page-closed")

    class Document:
        def __init__(self, content):
            assert lock.locked()

        def __len__(self):
            assert lock.locked()
            return pages

        def __getitem__(self, number):
            assert lock.locked()
            return Page()

        def close(self):
            assert lock.locked()
            calls.append("document-closed")

    monkeypatch.setitem(sys.modules, "pypdfium2", types.SimpleNamespace(PdfDocument=Document))
    monkeypatch.setitem(sys.modules, "numpy", types.SimpleNamespace())
    with pytest.raises(example.DocumentError, match=error):
        example.recognize_pdf(b"fixture", None)
    assert calls == (["document-closed"] if pages != 1 else ["page-closed", "document-closed"])


def test_same_cache_key_has_one_writer_and_one_ocr_call(example, tmp_path):
    path, cache = tmp_path / "input.pdf", tmp_path / "cache"
    path.write_bytes(b"fixture")
    entered, release = threading.Event(), threading.Event()
    calls = []

    def recognize(*args):
        calls.append(True)
        entered.set()
        assert release.wait(5)
        return LINES["clean-us-clean-simple"]

    def read():
        return example.cached_lines(path, example.digest(b"fixture"), cache, None, recognize)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(read)
        assert entered.wait(5)
        second = pool.submit(read)
        try:
            with pytest.raises(TimeoutError):
                second.result(timeout=0.05)
        finally:
            release.set()
        assert first.result() == second.result() == LINES["clean-us-clean-simple"]
    assert len(calls) == 1


def test_cache_bounds_lines_before_writing(example, tmp_path):
    path = tmp_path / "input.pdf"
    path.write_bytes(b"fixture")
    with pytest.raises(example.DocumentError, match="ocr_output_limit"):
        example.cached_lines(
            path, example.digest(b"fixture"), tmp_path / "cache", None, lambda *args: ["x" * 2049]
        )
    assert not list((tmp_path / "cache").glob("*.json"))


def test_cache_symlinks_cannot_replace_other_files(example, tmp_path):
    path, cache = tmp_path / "input.pdf", tmp_path / "cache"
    path.write_bytes(b"fixture")
    example.cached_lines(path, example.digest(b"fixture"), cache, None, lambda *args: [])
    entry = next(cache.glob("*.json"))
    outside = tmp_path / "sentinel"
    outside.write_text("unchanged")
    entry.unlink()
    entry.symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        example.cached_lines(path, example.digest(b"fixture"), cache, None, lambda *args: [])
    assert outside.read_text() == "unchanged"


def test_duplicate_dimension_keys_are_rejected(example):
    for rows, label in ((example.SUPPLIERS, "supplier"), (example.PURCHASE_ORDERS, "purchase order")):
        with pytest.raises(ValueError, match="unique"):
            example.unique_dimension(rows + [rows[0]], 0, label)


@pytest.mark.parametrize(
    "field,value",
    [
        (1, None),
        (1, "S-UNKNOWN"),
        (2, None),
        (2, "JPY"),
        (3, None),
        (3, True),
        (3, 0),
        (3, -1),
        (3, 9_000_000_000_000_001),
        (3, 50.0),
        (3, "5000"),
    ],
)
def test_invalid_po_dimension_values_cannot_fall_through_null_comparisons(example, field, value):
    order = list(example.PURCHASE_ORDERS[0])
    order[field] = value
    with pytest.raises(ValueError, match="purchase order requires"):
        example.validate_dimensions(example.SUPPLIERS, [order])


@pytest.mark.parametrize("name", [None, "", "  "])
def test_supplier_dimension_requires_real_nonblank_names(example, name):
    with pytest.raises(ValueError, match="supplier names"):
        example.validate_dimensions([("S-ACME", name)], [])


def test_failed_publication_keeps_previous_generation_and_allows_retry(example, tmp_path):
    def build(directory):
        (directory / "report.txt").write_text("complete")
        return {"rows": 7}

    original = example.publish(tmp_path, build)
    pointer = (tmp_path / "CURRENT").read_bytes()
    with pytest.raises(RuntimeError, match="before CURRENT"):
        example.publish(tmp_path, build, fail_before_publish=True)
    assert (tmp_path / "CURRENT").read_bytes() == pointer
    assert example.current_run(tmp_path) == original and (original / "report.txt").read_text() == "complete"
    retried = example.publish(tmp_path, build)
    assert example.current_run(tmp_path) == retried and retried != original and original.is_dir()


def test_new_pointer_never_names_a_deleted_generation_after_late_filesystem_error(
    example, tmp_path, monkeypatch
):
    original_write = example.atomic_json

    def fail_after_replace(path, value):
        original_write(path, value)
        if Path(path).name == "CURRENT":
            raise OSError("injected post-replacement sync failure")

    monkeypatch.setattr(example, "atomic_json", fail_after_replace)
    with pytest.raises(OSError, match="sync failure"):
        example.publish(tmp_path, lambda directory: {"complete": True})
    assert example.current_run(tmp_path).is_dir()


def test_answer_key_is_separate_from_four_pdf_inference_inputs(example):
    sources = json.loads((EXAMPLE / "fixtures/sources.json").read_text())
    assert len(sources) == 4 and all(row["file"].endswith(".pdf") for row in sources)
    assert all(set(row) == {"source_id", "file", "sha256"} for row in sources)
    assert "source-gold.json" not in (EXAMPLE / "main.py").read_text()
    assert example.REVISION == "446bab08662790cb39457792243bff5704373b95"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_invoice_engine_ocr_gold_and_reconciliation_outcomes(engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("invoice_reconciliation_checks.py"),
        tmp_path / engine,
        script_dependencies(EXAMPLE / "main.py"),
        360,
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"
