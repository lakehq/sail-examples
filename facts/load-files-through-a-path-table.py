FACT = {
    "id": "load-files-through-a-path-table",
    "category": "data-sources",
    "core": True,
    "text": """To process loose files (images, PDFs, audio, documents), build a DataFrame of file paths
        and read each file inside `mapInArrow` or a pandas UDF, returning only what later stages need
        (text, features, embeddings). This gives exactly one row per file and keeps large binaries out of
        the data passed between stages. Paths must be readable from where the UDF runs (local files on
        one machine, object storage on a cluster).""",
    "engines": ["sail", "spark"],
    "order": 1,
}

import hashlib
import tempfile
from pathlib import Path

import pyarrow as pa


def read_files(batches):
    for batch in batches:
        paths = batch.column("path").to_pylist()
        sizes, digests = [], []
        for path in paths:
            data = Path(path).read_bytes()
            sizes.append(len(data))
            digests.append(hashlib.sha256(data).hexdigest())
        yield pa.RecordBatch.from_pydict({"path": paths, "size": sizes, "sha256": digests})


def check(spark, engine):
    directory = Path(tempfile.mkdtemp())
    expected = {}
    for name, size in [("big.bin", 3 * 1024 * 1024), ("mid.bin", 600 * 1024), ("small.bin", 1024)]:
        data = bytes([len(name)]) * size
        (directory / name).write_bytes(data)
        expected[str(directory / name)] = (size, hashlib.sha256(data).hexdigest())

    paths = spark.createDataFrame([(p,) for p in sorted(expected)], "path string")
    rows = paths.mapInArrow(read_files, "path string, size long, sha256 string").collect()

    got = {r.path: (r.size, r.sha256) for r in rows}
    assert len(rows) == len(expected), f"expected one row per file ({len(expected)}), got {len(rows)}"
    assert got == expected, f"file contents differ: {got}"
    return {"rows": len(rows)}
