"""Check streaming page output and document cleanup with real PDF and OCR libraries."""

import importlib.util
import tempfile
from pathlib import Path

import pyarrow as pa


def check(spark, engine):
    import pypdfium2 as pdfium
    from pypdf import PdfWriter

    path = Path(__file__).resolve().parent.parent / "multimodal/ocr-scanned-pdfs/main.py"
    spec = importlib.util.spec_from_file_location("ocr_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory(prefix="ocr-stream-") as directory:
        root = Path(directory)
        paths = []
        for name in ("first.pdf", "second.pdf"):
            target = root / name
            with PdfWriter() as writer:
                for _ in range(33):
                    writer.add_blank_page(width=72, height=72)
                writer.write(target)
            paths.append(str(target))
        malformed = root / "malformed.pdf"
        malformed.write_bytes(b"not a PDF")
        paths.append(str(malformed))
        batch = pa.RecordBatch.from_pydict({"path": paths})
        batches = list(module.ocr_pages(iter([batch])))
        assert all(result.num_rows == 1 for result in batches), "output accumulates multiple pages"
        rows = [row for result in batches for row in result.to_pylist()]
        expected = {(name, page, "blank") for name in ("first.pdf", "second.pdf") for page in range(1, 34)}
        actual = {(row["file"], row["page"], row["status"]) for row in rows if row["page"] is not None}
        assert actual == expected and len(rows) == 67, rows
        assert len([row for row in rows if row["file"] == "malformed.pdf" and row["status"] == "error"]) == 1

        # Track a real document so cancelling after the first emitted page must close it.
        opened = []
        original = pdfium.PdfDocument

        def tracked_document(*args, **kwargs):
            document = original(*args, **kwargs)
            opened.append(document)
            return document

        pdfium.PdfDocument = tracked_document
        pages = None
        try:
            pages = module.ocr_pages(iter([pa.RecordBatch.from_pydict({"path": paths[:1]})]))
            first = next(pages)
            assert first.num_rows == 1 and first.to_pylist()[0]["page"] == 1
            assert len(opened) == 1 and opened[0].raw is not None
            pages.close()
            assert opened[0].raw is None, "document remained open after generator cancellation"
        finally:
            if pages is not None:
                pages.close()
            pdfium.PdfDocument = original

        # Exercise multiple documents in separate engine partitions with the same completeness oracle.
        frames = spark.createDataFrame([(p,) for p in paths], "path string").repartition(2)
        result = frames.mapInArrow(module.ocr_pages, module.SCHEMA).collect()
        observed = {(row.file, row.page, row.status) for row in result}
        assert observed == expected | {("malformed.pdf", None, "error")} and len(result) == 67
    return {"engine": engine, "documents": 3, "pages": 66, "error_rows": 1, "max_output_rows": 1}
