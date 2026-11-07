# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "rapidocr==3.10.0",
#   "onnxruntime==1.31.1",
#   "pypdfium2==5.14.0",
#   "pillow==13.0.0",
#   "opencv-python-headless==5.0.0.93",
#   "shapely==2.2.0",
#   "pyclipper==1.4.0",
#   "huggingface-hub==0.36.0",
# ]
# ///
"""Read four controlled-layout invoices with OCR, then reconcile their gross totals."""

import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import sys
import threading
import types
import uuid
from collections import Counter
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT_ID = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
DATASET = "drew-ipp/invoice-extraction-benchmark"
REVISION = "446bab08662790cb39457792243bff5704373b95"
MAX_PDF_BYTES = 5_000_000
MAX_RENDER_PIXELS = 2_000_000
MAX_LINES = 512
MAX_LINE_CHARS = 2048
MAX_CACHE_BYTES = 1_100_000
# Keep small field labels legible while staying within the render-pixel limit.
RENDER_SCALE = 1.75
OCR_PARAMS = {
    "EngineConfig.onnxruntime.intra_op_num_threads": 1,
    "EngineConfig.onnxruntime.inter_op_num_threads": 1,
    "Global.text_score": 0.5,
    "Global.log_level": "error",
}
SCHEMA = (
    "document_id string, source_id string, supplier_id string, po_id string, currency string, "
    "observed_supplier_id string, invoice_id string, gross_minor long, invoice_evidence string, "
    "total_evidence string, extraction_status string, error string"
)
FIELDS = [field.strip().split()[0] for field in SCHEMA.split(",")]
SUPPLIERS = [
    ("S-ACME", "Acme Industrial Supply Co"),
    ("S-NORTHWIND", "Northwind Logistics LLC"),
    ("S-PENNINE", "Pennine Tyre & Parts Limited"),
    ("S-MERSEY", "Mersey Office Supplies Ltd"),
]
# These are authored purchasing/intake fixtures, not fields supplied to OCR.
PURCHASE_ORDERS = [
    ("PO-ACME", "S-ACME", "USD", 37249),
    ("PO-NORTHWIND", "S-NORTHWIND", "USD", 159944),  # deliberately one cent too high
    ("PO-MERSEY", "S-MERSEY", "GBP", 1499034),
    ("PO-STRUCT-USD", "S-ACME", "USD", 5000),
    ("PO-STRUCT-GBP", "S-MERSEY", "GBP", 9000),
]


class DocumentError(ValueError):
    """An input cannot meet this example's deliberately narrow extraction contract."""


def digest(value):
    return hashlib.sha256(value).hexdigest()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def atomic_json(path, value):
    path = Path(path)
    if path.is_symlink():
        raise ValueError("JSON destination must not be a symlink")
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(json_bytes(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def process_lock(name):
    registry = sys.modules.setdefault("_process_locks", types.ModuleType("_process_locks"))
    return registry.__dict__.setdefault(name, threading.Lock())


def validate_lines(lines):
    if (
        not isinstance(lines, list)
        or len(lines) > MAX_LINES
        or any(not isinstance(line, str) or len(line) > MAX_LINE_CHARS for line in lines)
    ):
        raise DocumentError("ocr_output_limit")
    lines = [line.strip() for line in lines if line.strip()]
    if len(json_bytes(lines)) > 1_000_000:
        raise DocumentError("ocr_output_limit")
    return lines


def minor_units(amount, currency):
    """Exact positive gross amounts for the two explicitly supported ISO currencies."""
    if currency not in {"USD", "GBP"}:
        raise DocumentError("unsupported_currency")
    if not isinstance(amount, str) or not re.fullmatch(
        r"(?:[0-9]+|[1-9][0-9]{0,2}(?:,[0-9]{3})+)\.[0-9]{2}", amount
    ):
        raise DocumentError("invalid_gross_amount")
    result = int(Decimal(amount.replace(",", "")) * 100)
    if not 0 < result <= 9_000_000_000_000_000:
        raise DocumentError("gross_amount_out_of_range")
    return result


def parse_invoice(lines, currency, suppliers=SUPPLIERS):
    """Read one unambiguous header, invoice label/value and TOTAL label/value."""
    lines = validate_lines(lines)
    if not lines:
        raise DocumentError("blank")
    normalized = lambda text: " ".join(text.casefold().split())
    header = lines[:8]
    matches = []
    for supplier_id, name in suppliers:
        for start in range(len(header)):
            for length in (1, 2):
                if normalized(" ".join(header[start : start + length])) == normalized(name):
                    matches.append((supplier_id, start))
    # A repeated header or two supplier names is ambiguous, even when one is expected.
    if len(set(matches)) != 1:
        raise DocumentError("supplier_header_missing_or_ambiguous")
    labels = [
        index for index, line in enumerate(lines) if line.casefold() in {"invoice #:", "invoice number:"}
    ]
    totals = [index for index, line in enumerate(lines) if line.casefold() == "total"]
    if len(labels) != 1 or len(totals) != 1 or max(labels[0], totals[0]) + 1 >= len(lines):
        raise DocumentError("invoice_or_total_label_missing_or_ambiguous")
    label_positions = {labels[0], totals[0]}
    if any(position + 1 in label_positions for position in label_positions):
        raise DocumentError("invoice_or_total_label_missing_or_ambiguous")
    invoice_id, amount = lines[labels[0] + 1], lines[totals[0] + 1]
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9/-]{2,63}", invoice_id):
        raise DocumentError("invalid_invoice_id")
    if currency not in {"USD", "GBP"}:
        raise DocumentError("unsupported_currency")
    symbol = {"USD": "$", "GBP": "£"}[currency]
    if not amount.startswith(symbol):
        raise DocumentError("currency_symbol_mismatch")
    return matches[0][0], invoice_id, minor_units(amount[len(symbol) :], currency), invoice_id, amount


def read_pdf_bytes(path, expected_sha):
    with Path(path).open("rb") as stream:
        content = stream.read(MAX_PDF_BYTES + 1)
    if len(content) > MAX_PDF_BYTES:
        raise DocumentError("pdf_byte_limit")
    if digest(content) != expected_sha:
        raise DocumentError("source_sha256_mismatch")
    return content


def recognize_pdf(content, engine):
    import numpy as np
    import pypdfium2 as pdfium

    # Every PDFium operation, including close, shares the existing examples' process lock.
    with process_lock("pdfium"):
        document = pdfium.PdfDocument(content)
        try:
            if len(document) != 1:
                raise DocumentError("single_page_required")
            page = document[0]
            try:
                width, height = page.get_size()
                if not all(math.isfinite(value) and value > 0 for value in (width, height)):
                    raise DocumentError("invalid_page_size")
                if math.ceil(width * RENDER_SCALE) * math.ceil(height * RENDER_SCALE) > MAX_RENDER_PIXELS:
                    raise DocumentError("render_pixel_limit")
                bitmap = page.render(scale=RENDER_SCALE)
                try:
                    image = bitmap.to_pil().convert("RGB")
                finally:
                    bitmap.close()
            finally:
                page.close()
        finally:
            document.close()
    result = engine(np.asarray(image))
    return validate_lines(list(result.txts or ()))


def cached_lines(path, expected_sha, cache, engine, recognize=recognize_pdf):
    """A local cache of OCR lines; intake/PO fields and source gold are never cached inputs."""
    content = read_pdf_bytes(path, expected_sha)  # a cache hit still verifies the current source bytes
    cache = Path(cache)
    if cache.is_symlink():
        raise ValueError("cache directory must not be a symlink")
    cache.mkdir(parents=True, exist_ok=True)
    identity = {"source_sha256": expected_sha, "script_sha256": SCRIPT_ID, "ocr_config": OCR_PARAMS}
    key = digest(json_bytes(identity))
    target, lock = cache / (key + ".json"), cache / (key + ".lock")
    if target.is_symlink() or lock.is_symlink():
        raise ValueError("cache entries must not be symlinks")
    with lock.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        if target.is_file() and target.stat().st_size <= MAX_CACHE_BYTES:
            try:
                record = json.loads(target.read_text())
                lines = validate_lines(record["lines"])
                if record["identity"] == identity and record["lines_sha256"] == digest(json_bytes(lines)):
                    return lines
            except (ValueError, KeyError, TypeError):
                pass  # an invalid local cache is recomputed, never accepted as extracted evidence
        lines = validate_lines(recognize(content, engine))
        atomic_json(target, {"identity": identity, "lines": lines, "lines_sha256": digest(json_bytes(lines))})
        return lines


def canonical_row(intake, *, fields=None, error=None):
    row = {name: intake.get(name) for name in FIELDS}
    row.update(
        observed_supplier_id=None,
        invoice_id=None,
        gross_minor=None,
        invoice_evidence=None,
        total_evidence=None,
        extraction_status="error" if error else "ok",
        error=error,
    )
    if fields is not None:
        row.update(
            zip(
                ("observed_supplier_id", "invoice_id", "gross_minor", "invoice_evidence", "total_evidence"),
                fields,
                strict=True,
            )
        )
    return row


def ocr_documents(batches):
    import pyarrow as pa
    from rapidocr import RapidOCR

    engine = RapidOCR(params=OCR_PARAMS)  # once per partition, outside both batch and document loops
    schema = pa.schema([(name, pa.int64() if name == "gross_minor" else pa.string()) for name in FIELDS])
    for batch in batches:
        for intake in batch.to_pylist():
            try:
                lines = cached_lines(intake["path"], intake["sha256"], intake["cache"], engine)
                row = canonical_row(intake, fields=parse_invoice(lines, intake["currency"]))
            except DocumentError as error:
                row = canonical_row(intake, error=str(error))
            except Exception as error:  # noqa: BLE001 - one explicit failure row per individual input
                row = canonical_row(intake, error="read_or_ocr_failed:" + type(error).__name__)
            yield pa.RecordBatch.from_pylist([row], schema=schema)


def unique_dimension(rows, index, label):
    keys = [row[index] for row in rows]
    if any(not isinstance(key, str) or not key.strip() for key in keys) or len(keys) != len(set(keys)):
        raise ValueError(label + " keys must be nonempty and unique")


def validate_dimensions(suppliers, purchase_orders):
    unique_dimension(suppliers, 0, "supplier")
    unique_dimension(purchase_orders, 0, "purchase order")
    if any(len(row) != 2 or not isinstance(row[1], str) or not row[1].strip() for row in suppliers):
        raise ValueError("supplier names must be nonblank strings")
    supplier_ids = {row[0] for row in suppliers}
    if any(
        len(row) != 4
        or row[1] not in supplier_ids
        or row[2] not in {"USD", "GBP"}
        or type(row[3]) is not int
        or not 0 < row[3] <= 9_000_000_000_000_000
        for row in purchase_orders
    ):
        raise ValueError(
            "purchase order requires a known supplier, supported currency and positive integer gross"
        )


def reconcile(spark, invoices, suppliers=SUPPLIERS, purchase_orders=PURCHASE_ORDERS):
    """The same relational checks for OCR-derived and already structured canonical rows."""
    from pyspark.sql import functions as F

    validate_dimensions(suppliers, purchase_orders)
    known = spark.createDataFrame(
        [(row[0], True) for row in suppliers], "supplier_id string, known_supplier boolean"
    )
    orders = spark.createDataFrame(
        purchase_orders, "po_id string, po_supplier_id string, po_currency string, expected_gross_minor long"
    )
    duplicates = (
        invoices.where(
            "extraction_status = 'ok' AND invoice_id IS NOT NULL AND observed_supplier_id IS NOT NULL"
        )
        .groupBy("observed_supplier_id", "invoice_id")
        .count()
        .withColumnRenamed("count", "invoice_copies")
    )
    joined = (
        invoices.join(duplicates, ["observed_supplier_id", "invoice_id"], "left")
        .join(known, "supplier_id", "left")
        .join(orders, "po_id", "left")
    )
    invalid = (
        (F.col("extraction_status") != "ok")
        | F.col("extraction_status").isNull()
        | F.col("invoice_id").isNull()
        | F.col("observed_supplier_id").isNull()
        | (F.length(F.trim(F.col("invoice_id"))) == 0)
        | (F.length(F.trim(F.col("observed_supplier_id"))) == 0)
        | F.col("gross_minor").isNull()
        | (F.col("gross_minor") <= 0)
        | (F.col("gross_minor") > 9_000_000_000_000_000)
        | F.col("currency").isNull()
        | (~F.col("currency").isin("USD", "GBP"))
    )
    status = (
        F.when(invalid, "extraction_error")
        .when(F.col("invoice_copies") > 1, "duplicate_invoice")
        .when(F.col("known_supplier").isNull(), "unknown_supplier")
        .when(F.col("observed_supplier_id") != F.col("supplier_id"), "supplier_mismatch")
        .when(F.col("po_supplier_id").isNull(), "missing_po")
        .when(F.col("po_supplier_id") != F.col("supplier_id"), "po_supplier_mismatch")
        .when(F.col("po_currency") != F.col("currency"), "currency_mismatch")
        .when(F.col("gross_minor") != F.col("expected_gross_minor"), "amount_mismatch")
        .otherwise("match")
    )
    return joined.withColumn("status", status).select(*FIELDS, "status").orderBy("document_id")


def blank_pdf():
    """A small authored blank PDF, independent of the four downloaded source documents."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        b"<< /Length 0 >>\nstream\n\nendstream",
    ]
    content, offsets = b"%PDF-1.4\n", [0]
    for index, body in enumerate(objects, 1):
        offsets.append(len(content))
        content += f"{index} 0 obj\n".encode() + body + b"\nendobj\n"
    start = len(content)
    content += b"xref\n0 5\n0000000000 65535 f \n"
    content += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return content + f"trailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n".encode()


def document_intake(root):
    from huggingface_hub import hf_hub_download

    sources = json.loads((HERE / "fixtures/sources.json").read_text())
    contexts = [
        ("doc-acme", "S-ACME", "PO-ACME", "USD"),
        ("doc-northwind", "S-NORTHWIND", "PO-NORTHWIND", "USD"),
        ("doc-pennine", "S-PENNINE", "PO-MISSING", "GBP"),
        ("doc-mersey", "S-MERSEY", "PO-MERSEY", "GBP"),
    ]
    records = []
    for source, (document_id, supplier_id, po_id, currency) in zip(sources, contexts, strict=True):
        path = hf_hub_download(
            DATASET, "corpus/v1/" + source["file"], repo_type="dataset", revision=REVISION, token=False
        )
        records.append(
            {
                "document_id": document_id,
                "source_id": source["source_id"],
                "supplier_id": supplier_id,
                "po_id": po_id,
                "currency": currency,
                "path": str(path),
                "sha256": source["sha256"],
                "cache": str(root / "ocr-cache"),
            }
        )
    records.append({**records[0], "document_id": "doc-acme-alias"})
    inputs = root / "inputs"
    inputs.mkdir(exist_ok=True)
    for name, content in (("corrupt", b"This is not a PDF.\n"), ("blank", blank_pdf())):
        path = inputs / (name + ".pdf")
        path.write_bytes(content)
        records.append(
            {
                "document_id": "doc-" + name,
                "source_id": "authored-" + name,
                "supplier_id": "S-ACME",
                "po_id": "PO-ACME",
                "currency": "USD",
                "path": str(path),
                "sha256": digest(content),
                "cache": str(root / "ocr-cache"),
            }
        )
    return records


def structured_intake():
    examples = [
        ("structured-match", "S-ACME", "PO-STRUCT-USD", "USD", "STRUCT-001", "50.00"),
        ("structured-duplicate-a", "S-MERSEY", "PO-STRUCT-GBP", "GBP", "STRUCT-002", "90.00"),
        ("structured-duplicate-b", "S-MERSEY", "PO-STRUCT-GBP", "GBP", "STRUCT-002", "90.00"),
        ("structured-missing-po", "S-ACME", None, "USD", "STRUCT-003", "70.00"),
        ("structured-wrong-amount", "S-ACME", "PO-STRUCT-USD", "USD", "STRUCT-004", "49.99"),
        ("structured-invalid", "S-ACME", "PO-STRUCT-USD", "USD", None, "50.00"),
    ]
    records = []
    for document_id, supplier_id, po_id, currency, invoice_id, gross in examples:
        intake = {
            "document_id": document_id,
            "source_id": "authored-structured",
            "supplier_id": supplier_id,
            "po_id": po_id,
            "currency": currency,
        }
        try:
            if not invoice_id:
                raise DocumentError("missing_invoice_id")
            row = canonical_row(
                intake,
                fields=(
                    supplier_id,
                    invoice_id,
                    minor_units(gross, currency),
                    invoice_id,
                    currency + " " + gross,
                ),
            )
        except DocumentError as error:
            row = canonical_row(intake, error=str(error))
        records.append(row)
    return records


def current_run(root):
    root = Path(root)
    if any(path.is_symlink() for path in (root, root / "CURRENT", root / "runs")):
        raise ValueError("report storage must not use symlinks")
    pointer = json.loads((root / "CURRENT").read_text())
    if not re.fullmatch(r"runs/[0-9a-f]{32}", pointer["run"]):
        raise ValueError("invalid report pointer")
    generation = root / pointer["run"]
    manifest = generation / "manifest.json"
    if (
        generation.is_symlink()
        or manifest.is_symlink()
        or digest(manifest.read_bytes()) != pointer["manifest_sha256"]
    ):
        raise ValueError("report manifest differs")
    return generation


def publish(root, build, *, fail_before_publish=False):
    """One local writer, new run paths, then atomic pointer replacement; old reports remain readable."""
    root = Path(root)
    if root.is_symlink():
        raise ValueError("report root must not be a symlink")
    root.mkdir(parents=True, exist_ok=True)
    for name in ("runs", ".writer.lock", "CURRENT"):
        if (root / name).is_symlink():
            raise ValueError("report storage must not use symlinks")
    (root / "runs").mkdir(exist_ok=True)
    with (root / ".writer.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        generation = root / "runs" / uuid.uuid4().hex
        generation.mkdir()
        publishing = False
        try:
            metadata = build(generation)
            atomic_json(
                generation / "manifest.json", {"schema_version": 1, "script_sha256": SCRIPT_ID, **metadata}
            )
            if fail_before_publish:
                raise RuntimeError("injected failure before CURRENT replacement")
            publishing = True
            atomic_json(
                root / "CURRENT",
                {
                    "run": str(generation.relative_to(root)),
                    "manifest_sha256": digest((generation / "manifest.json").read_bytes()),
                },
            )
        except BaseException:
            # Once pointer replacement starts, preserve the completed generation even if a later
            # filesystem error obscures whether replacement succeeded. CURRENT never names partial data.
            if not publishing:
                shutil.rmtree(generation)
            raise
    return generation


def require_coverage(frame, identifiers):
    actual = Counter(row.document_id for row in frame.select("document_id").collect())
    if actual != Counter(identifiers) or any(count != 1 for count in actual.values()):
        raise AssertionError("reconciliation must retain exactly one row for every unique input ID")


def run(spark, root=None, *, fail_before_publish=False):
    root = Path(root) if root is not None else HERE / ".data"

    def build(generation):
        intake = document_intake(root)
        ids = [row["document_id"] for row in intake]
        if len(ids) != len(set(ids)):
            raise ValueError("intake document IDs must be unique")
        inputs = spark.createDataFrame(
            intake,
            "document_id string, source_id string, supplier_id string, po_id string, currency string, "
            "path string, sha256 string, cache string",
        )
        ocr_path = str(generation / "ocr.parquet")
        inputs.repartition(2).mapInArrow(ocr_documents, SCHEMA).write.mode("error").parquet(ocr_path)
        # Both joins and subsequent actions consume stored OCR, never a recomputed model expression.
        canonical = spark.read.schema(SCHEMA).parquet(ocr_path)
        baseline = structured_intake()
        for name, frame, identifiers in (
            ("documents", canonical, ids),
            ("structured", spark.createDataFrame(baseline, SCHEMA), [row["document_id"] for row in baseline]),
        ):
            report = reconcile(spark, frame)
            report.write.mode("error").parquet(str(generation / (name + ".parquet")))
            require_coverage(spark.read.parquet(str(generation / (name + ".parquet"))), identifiers)
        return {"dataset_revision": REVISION, "documents": len(ids), "structured": len(baseline)}

    generation = publish(root, build, fail_before_publish=fail_before_publish)
    return {
        name: spark.read.parquet(str(generation / (name + ".parquet"))).orderBy("document_id")
        for name in ("documents", "structured")
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start()
    spark = None
    try:
        _, port = server.listening_address
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        for name, frame in run(spark).items():
            print(name)
            frame.select("document_id", "invoice_id", "currency", "gross_minor", "status", "error").show(
                truncate=False
            )
    finally:
        if spark is not None:
            spark.stop()
        server.stop()
