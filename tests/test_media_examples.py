"""Media transforms preserve complete outputs across document boundaries."""

from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_ocr_streams_pages_and_closes_documents(engine, tmp_path):
    dependencies = script_dependencies(ROOT / "multimodal/ocr-scanned-pdfs/main.py") + ["pypdf==6.21.1"]
    meta = run(
        engine, "fact", Path(__file__).with_name("ocr_checks.py"), tmp_path / engine, dependencies, 300
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"


@pytest.mark.parametrize("case,directory", [("video", "search-video-frames"), ("pdf", "pdf-search")])
@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_media_chunk_boundaries(case, directory, engine, tmp_path):
    dependencies = script_dependencies(ROOT / "multimodal" / directory / "main.py")
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("media_boundary_checks.py"),
        tmp_path / engine,
        dependencies,
        300,
        {"SAIL_EXAMPLES_BOUNDARY_CASE": case},
    )
    assert meta["status"] == "ok", f"{engine} {case} [{meta['status']}]: {meta.get('message')}"


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_repeated_captions_keep_their_photo_identity(engine, tmp_path):
    dependencies = script_dependencies(ROOT / "multimodal/search-images-by-text/main.py")
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("retrieval_checks.py"),
        tmp_path / engine,
        dependencies,
        120,
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"
