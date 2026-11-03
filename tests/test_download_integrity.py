"""Pinned taxi inputs are verified before use, including cached and partial files."""

import ast
import hashlib
import io
import os
import tempfile
import urllib.request
from pathlib import Path

import pytest
from catalog import ROOT

EXAMPLES = [
    "quickstart/query-files-with-sql",
    "analytics/explore-taxi-trips",
    "analytics/sql-in-a-python-app",
    "pipelines/taxi-etl-job",
    "pipelines/delta-table-lifecycle",
    "pipelines/iceberg-tables",
]
CONTENT = b"verified fixture bytes\n"
DIGEST = hashlib.sha256(CONTENT).hexdigest()


@pytest.fixture(params=["fixture.parquet", "trips/fixture.parquet"])
def input_name(request):
    return request.param


@pytest.fixture(params=EXAMPLES)
def download(request, tmp_path, input_name):
    # Compile the actual boundary function without importing engine/model packages.
    tree = ast.parse((ROOT / request.param / "main.py").read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "download")
    name, url = input_name, "https://example.com/fixture.parquet"
    files = {name: DIGEST if request.param.startswith("pipelines/") else (url, DIGEST)}
    if request.param == "pipelines/taxi-etl-job":
        files = {}
    namespace = {
        "Path": Path,
        "DATA": tmp_path,
        "FILES": files,
        "SOURCE": "https://example.com",
        "hashlib": hashlib,
        "os": os,
        "tempfile": tempfile,
        "urllib": urllib,
    }
    # Only the tracked example function is executed, with standard-library globals.
    exec(compile(ast.Module(body=[function], type_ignores=[]), request.param, "exec"), namespace)  # noqa: S102

    def call():
        args = (name, url, DIGEST) if request.param == "pipelines/taxi-etl-job" else (name,)
        return namespace["download"](*args)

    return call, tmp_path / name


def test_valid_cache_does_not_download(download, monkeypatch):
    call, path = download
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(CONTENT)

    def unexpected(*args, **kwargs):
        pytest.fail("valid cached input must not be downloaded")

    monkeypatch.setattr(urllib.request, "urlopen", unexpected)
    assert call() == path
    assert path.read_bytes() == CONTENT


def test_invalid_cache_is_rejected_without_replacement(download, monkeypatch):
    call, path = download
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"altered input")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: pytest.fail("must reject cached input"))
    with pytest.raises(ValueError, match="SHA-256 mismatch:"):
        call()
    assert path.read_bytes() == b"altered input"


def test_download_is_verified_before_promotion(download, monkeypatch):
    call, path = download
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: io.BytesIO(CONTENT))
    replace = os.replace

    def verified_replace(source, destination):
        assert not path.exists()
        assert source != path and Path(source).parent == path.parent
        assert Path(source).read_bytes() == CONTENT
        replace(source, destination)

    monkeypatch.setattr(os, "replace", verified_replace)
    assert call() == path
    assert path.read_bytes() == CONTENT
    assert list(path.parent.iterdir()) == [path]


def test_invalid_download_leaves_no_cache_or_temporary_file(download, monkeypatch):
    call, path = download
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"wrong bytes"))
    with pytest.raises(ValueError, match="SHA-256 mismatch:"):
        call()
    assert list(path.parent.iterdir()) == []


def test_interrupted_read_cleans_temporary_file(download, monkeypatch):
    call, path = download

    class Interrupted(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise OSError("network interrupted")
            return super().read(4)

    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: Interrupted(CONTENT))
    with pytest.raises(OSError, match="network interrupted"):
        call()
    assert list(path.parent.iterdir()) == []


def test_failed_promotion_preserves_other_partial_files(download, monkeypatch):
    call, path = download
    path.parent.mkdir(parents=True, exist_ok=True)
    unrelated = path.with_suffix(".parquet.partial")
    unrelated.write_bytes(b"another download")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: io.BytesIO(CONTENT))

    def fail(*args):
        raise OSError("promotion failed")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError, match="promotion failed"):
        call()
    assert list(path.parent.iterdir()) == [unrelated]
    assert unrelated.read_bytes() == b"another download"
