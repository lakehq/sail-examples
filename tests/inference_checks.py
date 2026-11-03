"""Real-model checks for oversized Arrow batches, tails and stable row identities."""

import contextlib
import importlib.util
import os
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parent.parent
CASES = {
    "text": ("ai/embed-and-search", "embed", "id", "id long, embedding array<float>"),
    "pdf-text": ("multimodal/pdf-search", "embed", "chunk_id", "chunk_id string, embedding array<float>"),
    "classification": ("multimodal/classify-images", "classify", "file", "SCHEMA"),
    "images": ("multimodal/search-images-by-text", "embed_images", "file", "IMAGE_COLUMNS"),
    "image-queries": ("multimodal/search-images-by-text", "embed_texts", "query_id", "TEXT_COLUMNS"),
    "video-queries": ("multimodal/search-video-frames", "embed_texts", "query_id", "TEXT_COLUMNS"),
}


def load_example(directory):
    spec = importlib.util.spec_from_file_location("inference_example", ROOT / directory / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextlib.contextmanager
def observe_model_calls(limit):
    import onnxruntime as ort

    original = ort.InferenceSession
    sizes, setups = [], []

    class ObservedSession:
        def __init__(self, *args, **kwargs):
            self.session = original(*args, **kwargs)
            setups.append(1)

        def run(self, output_names, feeds):
            size = next(iter(feeds.values())).shape[0]
            assert 0 < size <= limit, f"model call has {size} rows, limit {limit}"
            sizes.append(size)
            return self.session.run(output_names, feeds)

    ort.InferenceSession = ObservedSession
    try:
        yield sizes, setups
    finally:
        ort.InferenceSession = original


def compare_rows(expected, actual, key):
    import numpy as np

    assert len(actual) == len(expected)
    assert len({row[key] for row in actual}) == len(actual), "duplicate output ID"
    for left, right in zip(sorted(expected, key=lambda r: r[key]), sorted(actual, key=lambda r: r[key])):
        assert left[key] == right[key]
        if "embedding" in left:
            values = np.asarray(right["embedding"])
            assert np.isfinite(values).all()
            np.testing.assert_allclose(np.linalg.norm(values), 1, atol=1e-4)
            np.testing.assert_allclose(left["embedding"], values, rtol=1e-5, atol=1e-6)
        else:
            for row in (left, right):
                scores = np.asarray([entry["score"] for entry in row["top3"]])
                assert len(scores) == 3 and np.isfinite(scores).all()
                assert np.isfinite(row["score"]) and 0 <= row["score"] <= 1
                assert ((scores >= 0) & (scores <= 1)).all() and np.all(np.diff(scores) <= 0)
                assert row["label"] == row["top3"][0]["label"] and row["score"] == scores[0]
                assert scores.sum() <= 1 + 1e-6
            assert left["label"] == right["label"]
            assert [r["label"] for r in left["top3"]] == [r["label"] for r in right["top3"]]
            np.testing.assert_allclose(left["score"], right["score"], rtol=1e-5, atol=1e-6, equal_nan=False)
            np.testing.assert_allclose(
                [r["score"] for r in left["top3"]],
                [r["score"] for r in right["top3"]],
                rtol=1e-5,
                atol=1e-6,
                equal_nan=False,
            )


def check(spark, engine):
    case = os.environ["SAIL_EXAMPLES_INFERENCE_CASE"]
    directory, function_name, key, schema = CASES[case]
    module = load_example(directory)
    function = getattr(module, function_name)
    schema = getattr(module, schema, schema)
    with tempfile.TemporaryDirectory(prefix="bounded-inference-") as directory:
        if case in ("classification", "images"):
            originals = sorted((ROOT / "multimodal/classify-images/images").glob("*.jpg"))
            originals += sorted((ROOT / "multimodal/classify-images/images").glob("*.png"))
            assert originals
            paths = []
            for i in range(72):
                original = originals[i % len(originals)]
                path = Path(directory) / f"{i:03d}-{original.name}"
                shutil.copyfile(original, path)
                paths.append(str(path))
            batch = pa.RecordBatch.from_pydict({"path": paths})
            expected_ids = {Path(path).name for path in paths}
            input_schema = "path string"
        else:
            texts = [
                ""
                if i == 0
                else (f"Record {i}: a different document about cats, invoices and satellites. " * (i % 4 + 1))
                for i in range(72)
            ]
            identifiers = (
                [f"document:{i // 4}:{i}" for i in range(72)] if key == "chunk_id" else list(range(72))
            )
            id_type = pa.string() if key == "chunk_id" else pa.int64()
            batch = pa.RecordBatch.from_pydict({key: pa.array(identifiers, type=id_type), "text": texts})
            expected_ids = set(identifiers)
            input_schema = f"{key} {'string' if key == 'chunk_id' else 'long'}, text string"

        # Real ONNX sessions are observed, not replaced with model predictions.
        with observe_model_calls(module.INFERENCE_BATCH_SIZE) as (sizes, setups):
            outputs = list(
                function(iter([batch.slice(0, 0), batch.slice(0, 65), batch.slice(65), batch.slice(0, 0)]))
            )
            assert sizes == [16, 16, 16, 16, 1, 7], sizes
            assert len(setups) == 1, "model initialized more than once per invocation"
        assert [output.num_rows for output in outputs] == [16, 16, 16, 16, 1, 7]
        actual = [row for output in outputs for row in output.to_pylist()]
        assert {row[key] for row in actual} == expected_ids
        if case == "classification":
            labels = {
                "espresso.jpg": "espresso",
                "golden-retriever.jpg": "golden retriever",
                "monarch-butterfly.jpg": "monarch",
                "red-fox.jpg": "red fox",
                "siamese-cat.jpg": "Siamese cat",
                "strawberries.jpg": "strawberry",
                "tabby-cat.jpg": "tabby",
                "zebra.jpg": "zebra",
            }
            assert {path.name for path in originals} == set(labels)
            assert all(row["label"] == labels[row["file"].split("-", 1)[1]] for row in actual)

        # One setup for a stream of single-row batches supplies a batching-invariance reference.
        with observe_model_calls(1) as (sizes, setups):
            reference = [
                row
                for output in function(iter(batch.slice(i, 1) for i in range(72)))
                for row in output.to_pylist()
            ]
            assert len(sizes) == 72 and len(setups) == 1
        compare_rows(reference, actual, key)

        def bounded(batches):
            for output in function(batches):
                assert output.num_rows <= 16
                yield output

        data = spark.createDataFrame(batch.to_pylist(), input_schema).repartition(2)
        returned = [row.asDict(recursive=True) for row in data.mapInArrow(bounded, schema).collect()]
        compare_rows(reference, returned, key)
    return {"engine": engine, "case": case, "rows": 72, "max_model_rows": 16, "single_row_reference": True}
