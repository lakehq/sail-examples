"""Identical wrong model results must fail the maintenance harness."""

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import test_examples


@pytest.mark.parametrize(
    "example_id",
    ["search-images-by-text", "search-video-frames", "transcribe-audio", "ocr-scanned-pdfs", "pdf-search"],
)
def test_identical_wrong_results_fail_expected_answers(example_id, tmp_path, monkeypatch):
    if example_id == "search-images-by-text":
        tables = {
            "top3": [
                {"query_id": q, "rank": r, "file": f"wrong-{r}.jpg"} for q in range(5) for r in (1, 2, 3)
            ],
            "recall": [{"captions": 53, "recall_at_1": 52 / 53, "recall_at_5": 1.0}],
        }
    elif example_id == "search-video-frames":
        tables = {
            "scenes": [{"scene": i + 1, "start": i * 5, "end": (i + 1) * 5, "frames": 5} for i in range(6)],
            "search": [{"query_id": q, "rank": r, "start": 0, "end": 5} for q in range(4) for r in (1, 2)],
        }
    elif example_id == "transcribe-audio":
        tables = {
            "transcripts": [
                {"id": str(i), "transcript": "wrong", "reference_chars": 100, "edits": 40} for i in range(73)
            ],
            "summary": [{"clips": 73, "character_error_rate": 0.4}],
        }
    elif example_id == "ocr-scanned-pdfs":
        tables = {
            "pages": [
                {
                    "file": file,
                    "page": 1,
                    "status": "ok",
                    "character_error_rate": 1.0,
                    "character_error_rate_sorted_words": 1.0,
                    "word_recall": 0.0,
                }
                for file in [
                    "fifty-famous-stories-1896.pdf",
                    "university-missourian-1915.pdf",
                    "street-railway-handbill-1888.pdf",
                    "hack-fares-1892.pdf",
                ]
            ]
            + [
                {"file": "fifty-famous-stories-1896.pdf", "page": 2, "status": "blank"},
                {"file": "damaged.pdf", "page": None, "status": "error"},
            ]
        }
    else:
        tables = {"top3": [{"doc": "wrong-policy", "page": 1, "text": "an unrelated result"}] * 3}

    def fake_run(engine, kind, path, output, *args):
        output.mkdir()
        for name, rows in tables.items():
            pq.write_table(pa.Table.from_pylist(rows), output / f"{name}.parquet")
        return {"status": "ok", "outputs": list(tables)}

    monkeypatch.setattr(test_examples, "run", fake_run)
    example = SimpleNamespace(id=example_id, meta={}, engines=("sail", "spark"), path=tmp_path, extra_deps=[])
    # The same fabricated outputs would be returned on either engine. The harness
    # must reject their incorrect task answers before an equality pass can accept them.
    with pytest.raises(AssertionError):
        test_examples.test_example(example, tmp_path)
