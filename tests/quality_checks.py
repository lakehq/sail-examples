"""Expected answers for the fixed media fixtures, separate from engine equality."""

import math

QUALITY_EXAMPLES = {
    "search-images-by-text",
    "search-video-frames",
    "transcribe-audio",
    "ocr-scanned-pdfs",
    "pdf-search",
}


def check_quality(example_id, tables):
    """Fail when a documented fixture answer regresses identically on both engines."""
    if example_id not in QUALITY_EXAMPLES:
        return
    rows = {name: table.to_pylist() for name, table in tables.items()}
    if example_id == "search-images-by-text":
        top, recall = rows["top3"], rows["recall"]
        assert len(top) == 15
        assert {(r["query_id"], r["rank"]) for r in top} == {(q, r) for q in range(5) for r in (1, 2, 3)}
        assert len({(r["query_id"], r["file"]) for r in top}) == 15
        winners = {r["query_id"]: r["file"] for r in top if r["rank"] == 1}
        assert winners[0] == "dog-snow.jpg" and winners[3] == "street-night.jpg"
        assert len(recall) == 1 and recall[0]["captions"] == 53
        assert math.isclose(recall[0]["recall_at_1"], 52 / 53, abs_tol=1e-8)
        assert recall[0]["recall_at_5"] == 1
    elif example_id == "search-video-frames":
        scenes, search = rows["scenes"], rows["search"]
        assert len(scenes) == 6
        assert sorted((r["scene"], r["start"], r["end"], r["frames"]) for r in scenes) == [
            (i + 1, i * 5, (i + 1) * 5, 5) for i in range(6)
        ]
        assert len(search) == 8
        assert {(r["query_id"], r["rank"]) for r in search} == {(q, r) for q in range(4) for r in (1, 2)}
        assert {r["query_id"]: (r["start"], r["end"]) for r in search if r["rank"] == 1} == {
            0: (20, 25),
            1: (15, 20),
            2: (10, 15),
            3: (5, 10),
        }
    elif example_id == "transcribe-audio":
        transcripts, summary = rows["transcripts"], rows["summary"]
        assert len(transcripts) == 73 and len({r["id"] for r in transcripts}) == 73
        assert len(summary) == 1 and summary[0]["clips"] == 73
        assert all(isinstance(r["transcript"], str) and r["transcript"].strip() for r in transcripts)
        assert all(r["reference_chars"] > 0 and r["edits"] >= 0 for r in transcripts)
        expected = sum(r["edits"] for r in transcripts) / sum(r["reference_chars"] for r in transcripts)
        assert math.isclose(summary[0]["character_error_rate"], expected, abs_tol=1e-10)
        # The README's approximate 5% observation is enforced to the nearest whole percent.
        assert 0.045 <= expected < 0.055, f"default speech CER changed: {expected}"
    elif example_id == "ocr-scanned-pdfs":
        pages = rows["pages"]
        assert len(pages) == 6 and len({(r["file"], r["page"]) for r in pages}) == 6
        assert sorted(r["status"] for r in pages) == ["blank", "error", "ok", "ok", "ok", "ok"]
        expected = {
            "fifty-famous-stories-1896.pdf": (0, 0, 1),
            "university-missourian-1915.pdf": (0.032, 0.061, 0.950),
            "street-railway-handbill-1888.pdf": (0.044, 0.055, 0.932),
            "hack-fares-1892.pdf": (0.314, 0.020, 0.981),
        }
        actual = {r["file"]: r for r in pages if r["status"] == "ok"}
        assert set(actual) == set(expected)
        for file, scores in expected.items():
            for key, score in zip(
                ("character_error_rate", "character_error_rate_sorted_words", "word_recall"), scores
            ):
                observed = actual[file][key]
                assert observed is not None and math.isfinite(observed)
                assert abs(observed - score) < 0.00050001, f"OCR fixture changed: {file} {key}={observed}"
        assert actual["fifty-famous-stories-1896.pdf"]["character_error_rate"] == 0
    elif example_id == "pdf-search":
        top = rows["top3"]
        assert len(top) == 3
        assert top[0]["doc"] == "billing-policy" and top[0]["page"] == 2
        assert "duplicate charge" in top[0]["text"].lower()
