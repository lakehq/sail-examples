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
#   "onnxruntime==1.23.2",
#   "pypdfium2==5.14.0",
#   "pillow==13.0.0",
#   "opencv-python-headless==5.0.0.93",
#   "shapely==2.2.0",
#   "pyclipper==1.4.0",
# ]
# ///
"""Extract text from scanned PDFs with OCR inside the engine, one row per page.

The pattern: a table of PDF paths goes through `mapInArrow`, which renders each page to an image
with pypdfium2 and reads the text with RapidOCR (ONNX models that ship inside the package). Every
page gets an explicit status, so a blank page or an unreadable file is a row, not a silent gap.
SQL then scores the text against transcriptions of the pages. The scans are real: an 1896 book
page, a 1915 newspaper column, an 1892 fare table and an 1888 handbill.
"""

import atexit
import itertools
import json
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RENDER_SCALE = 2  # two pixels per PDF point, 144 dpi
MIN_LINE_SCORE = 0.5  # RapidOCR's default: it drops any line it reads with a lower score

SCANS = Path(__file__).resolve().parent / "scans"  # page scans; sources and rights in scans/CREDITS.md
SCAN_DPI = 144  # each scan's resolution in its PDF: rendering at RENDER_SCALE gives the scan's size
# Each PDF is a list of pages: a scan from scans/, or None for a blank page.
DOCUMENTS = {
    "fifty-famous-stories-1896.pdf": ["book-page-1896.jpg", None],
    "university-missourian-1915.pdf": ["newspaper-column-1915.jpg"],
    "hack-fares-1892.pdf": ["fare-table-1892.jpg"],
    "street-railway-handbill-1888.pdf": ["handbill-1888-lowres.jpg"],
}
UNREADABLE = "damaged.pdf"  # not a PDF at all, to show the error status
# Typographic quotes and dashes, mapped to plain ones before comparing text.
QUOTES, DOUBLE_QUOTES, DASHES = "[\u2018\u2019]", "[\u201c\u201d]", "[\u2010-\u2015]"

ARROW_SCHEMA = pa.schema(
    [
        ("file", pa.string()),
        ("page", pa.int32()),
        ("status", pa.string()),
        ("lines", pa.int32()),
        ("confidence", pa.float64()),
        ("text", pa.string()),
        ("error", pa.string()),
    ]
)
SCHEMA = "file string, page int, status string, lines int, confidence double, text string, error string"


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def make_pdfs(directory: Path) -> list[tuple[str, int, str]]:
    """Write image-only PDFs (no text layer, like a scanner makes); return (file, page, true text) per page."""
    from PIL import Image

    truth = json.loads((SCANS / "truth.json").read_text())
    expected = []
    for name, pages in DOCUMENTS.items():
        images = []
        for number, scan in enumerate(pages, start=1):
            if scan is None:  # a blank page, the size of the page before it
                images.append(Image.new("L", images[-1].size, 255))
                expected.append((name, number, ""))
            else:
                with Image.open(SCANS / scan) as image:
                    images.append(image.convert("L"))
                expected.append((name, number, truth[scan]))
        images[0].save(directory / name, save_all=True, append_images=images[1:], resolution=SCAN_DPI)
    (directory / UNREADABLE).write_bytes(b"this file was cut off during upload")
    return expected


def page_row(file, page, status, lines=0, confidence=None, text=None, error=None) -> dict:
    return {
        "file": file,
        "page": page,
        "status": status,
        "lines": lines,
        "confidence": confidence,
        "text": text,
        "error": error,
    }


def process_lock(name: str):
    """One lock per name for the whole Python process, shared by every task running in it.

    It lives in `sys.modules` because each task unpickles its own copy of this script's globals.
    """
    import sys
    import threading
    import types

    registry = sys.modules.setdefault("_process_locks", types.ModuleType("_process_locks"))
    return registry.__dict__.setdefault(name, threading.Lock())


def ocr_pages(batches):
    """mapInArrow function: one PDF in, one row per page out."""
    import numpy as np
    import pypdfium2 as pdfium
    from rapidocr import RapidOCR

    # The OCR models ship inside the rapidocr package, so there is nothing to download.
    # One thread per model: the engine runs partitions in parallel already.
    threads = {f"EngineConfig.onnxruntime.{k}_op_num_threads": 1 for k in ("intra", "inter")}
    engine = RapidOCR(params={**threads, "Global.text_score": MIN_LINE_SCORE, "Global.log_level": "error"})
    # PDFium must never run in two threads at once, and partitions can be threads of one process.
    pdfium_lock = process_lock("pdfium")

    def render(document, index):
        with pdfium_lock:
            # Close the page and bitmap here, under the lock, never later from garbage collection.
            page = document[index]
            try:
                bitmap = page.render(scale=RENDER_SCALE)
                try:
                    image = bitmap.to_pil().convert("RGB")  # a copy, independent of the bitmap
                finally:
                    bitmap.close()
            finally:
                page.close()
        return np.asarray(image)

    for batch in batches:
        for path in batch.column("path").to_pylist():
            file = Path(path).name
            # A failure becomes an `error` row with its reason, so one bad file or page never stops
            # the job and never disappears from the output.
            document, pages = None, 0
            try:
                with pdfium_lock:
                    document = pdfium.PdfDocument(path)
                    pages = len(document)
            except Exception as error:  # noqa: BLE001
                reason = f"{type(error).__name__}: {error}".replace(path, file)  # no machine-specific path
                if document is not None:
                    with pdfium_lock:
                        document.close()
                    document = None
                yield pa.RecordBatch.from_pylist(
                    [page_row(file, None, "error", error=reason)], schema=ARROW_SCHEMA
                )
            try:
                for number in range(1, pages + 1):
                    try:
                        # OCR runs outside the lock, in parallel.
                        result = engine(render(document, number - 1))
                        found = list(result.txts or ())
                        if found:
                            confidence = round(float(np.mean(result.scores)), 4)
                            row = page_row(file, number, "ok", len(found), confidence, "\n".join(found))
                        else:
                            row = page_row(file, number, "blank", text="")
                    except Exception as error:  # noqa: BLE001
                        row = page_row(file, number, "error", error=f"{type(error).__name__}: {error}")
                    # Emit each page before decoding the next; document length does not grow this batch.
                    yield pa.RecordBatch.from_pylist([row], schema=ARROW_SCHEMA)
            finally:
                if document is not None:
                    with pdfium_lock:
                        document.close()


def run(spark: SparkSession) -> dict[str, DataFrame]:
    directory = Path(temp_dir("scans-"))
    truth = spark.createDataFrame(make_pdfs(directory), "file string, page int, expected string")

    # A table of paths, one row per PDF. OCR every page, then save once: later queries read the text.
    pages_path = temp_dir("pages-")
    paths = sorted(str(p) for p in directory.iterdir())
    pdfs = spark.createDataFrame([(p,) for p in paths], "path string").repartition(2)
    pdfs.mapInArrow(ocr_pages, SCHEMA).write.mode("overwrite").parquet(pages_path)
    pages = spark.read.parquet(pages_path)

    # Explicit character classes match the same characters on every engine (`\\s` and `\\w` may not).
    def tidy(column):
        """Lowercase, plain quotes and dashes, single spaces."""
        text = F.regexp_replace(F.regexp_replace(F.lower(F.col(column)), QUOTES, "'"), DOUBLE_QUOTES, '"')
        text = F.regexp_replace(text, DASHES, "-")
        return F.trim(F.regexp_replace(text, r"[ \t\n\r]+", " "))

    def words(column):
        """The distinct words, ignoring punctuation."""
        letters = F.regexp_replace(tidy(column), r"[^a-z0-9' ]", " ")
        return F.array_distinct(F.filter(F.split(letters, " "), lambda w: w != ""))

    def sorted_words(column):
        """The tidied text with its words in sorted order, punctuation kept."""
        return F.array_join(F.array_sort(F.split(tidy(column), " ")), " ")

    scored = pages.join(truth, ["file", "page"], "left").select(
        "file",
        "page",
        "status",
        "lines",
        "confidence",
        "text",
        "error",
        # Character error rate: edits from the OCR text to the true text, per true character. It counts
        # reading order, so a table read in a different order scores worse than its words deserve.
        (
            F.levenshtein(tidy("text"), tidy("expected")) / F.greatest(F.length(tidy("expected")), F.lit(1))
        ).alias("character_error_rate"),
        # The same rate with the words of both texts sorted, so reading order no longer counts.
        (
            F.levenshtein(sorted_words("text"), sorted_words("expected"))
            / F.greatest(F.length(sorted_words("expected")), F.lit(1))
        ).alias("character_error_rate_sorted_words"),
        # Word recall: the share of the page's distinct words that OCR found, in any order (none for a
        # page without words).
        F.when(
            F.size(words("expected")) > 0,
            F.size(F.array_intersect(words("text"), words("expected"))) / F.size(words("expected")),
        ).alias("word_recall"),
    )
    summary = scored.groupBy("status").agg(
        F.count("*").alias("pages"),
        F.round(F.avg("character_error_rate"), 4).alias("mean_character_error_rate"),
        F.round(F.avg("word_recall"), 4).alias("mean_word_recall"),
    )
    return {"pages": scored.orderBy("file", "page"), "summary": summary.orderBy("status")}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["pages"].select(
        "file",
        "page",
        "status",
        "lines",
        "confidence",
        F.round("character_error_rate", 3).alias("cer"),
        F.round("character_error_rate_sorted_words", 3).alias("cer_sorted_words"),
        F.round("word_recall", 3).alias("word_recall"),
        "error",
    ).show(truncate=False)
    # The page with the highest character error rate: its first OCR lines beside the transcription's.
    worst = results["pages"].filter("status = 'ok'").orderBy(F.desc("character_error_rate")).first()
    scan = DOCUMENTS[worst["file"]][worst["page"] - 1]
    true_lines = json.loads((SCANS / "truth.json").read_text())[scan].splitlines()
    print(f"Highest character error rate, {worst['file']} page {worst['page']}:")
    print(f"  {'OCR':<52} TRANSCRIPTION")
    for ocr_line, true_line in itertools.zip_longest(
        worst["text"].splitlines()[:16], true_lines[:16], fillvalue=""
    ):
        print(f"  {ocr_line:<52} {true_line}")
    print()
    results["summary"].show()
    spark.stop()
    server.stop()
