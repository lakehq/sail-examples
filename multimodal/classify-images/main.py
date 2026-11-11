# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "onnxruntime==1.31.1",
#   "huggingface-hub==0.36.0",
#   "pillow==13.0.0",
# ]
# ///
"""Classify a folder of photos with an ImageNet model inside the engine.

The pattern: a table of image paths goes through `mapInArrow`, which loads the ONNX model once
per partition, decodes each image, runs the model on the whole batch, and returns the top three
labels per image. Only labels and scores leave the function, never pixels.
"""

import atexit
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_REPO = "Xenova/resnet-18"  # ResNet-18 trained on ImageNet-1k, exported to ONNX
MODEL_REVISION = "549a3007ab1a7a2a4d3af94c287ebbb27f8f65c5"  # pinned: every run uses the same weights
IMAGES = Path(__file__).resolve().parent / "images"
TOP_K = 3
INFERENCE_BATCH_SIZE = 16  # bound model tensors independently of Arrow input batch size
REVIEW_BELOW = 0.6  # predictions less confident than this go to a review table

PREDICTION = pa.struct([("label", pa.string()), ("score", pa.float32())])
ARROW_SCHEMA = pa.schema(
    [("file", pa.string()), ("label", pa.string()), ("score", pa.float32()), ("top3", pa.list_(PREDICTION))]
)
SCHEMA = "file string, label string, score float, top3 array<struct<label: string, score: float>>"


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def preprocess(path: str):
    """The model's own preprocessing: shortest side to 256, center crop 224, ImageNet normalization."""
    import numpy as np
    from PIL import Image, ImageOps

    with Image.open(path) as image:
        image = ImageOps.exif_transpose(image).convert("RGB")
    # The shorter side becomes exactly 256; the longer side is scaled and truncated, as transformers does.
    short, long = sorted(image.size)
    resized = (256, int(256 * long / short))
    image = image.resize(resized if image.width <= image.height else resized[::-1], Image.BICUBIC)
    left, top = (image.width - 224) // 2, (image.height - 224) // 2
    image = image.crop((left, top, left + 224, top + 224))
    pixels = np.asarray(image, dtype=np.float32) / 255.0
    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    return ((pixels - mean) / std).transpose(2, 0, 1)


def classify(batches):
    """mapInArrow function: everything before the loop runs once per partition."""
    import json

    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download

    # Download where the task runs (cached per machine), so this also works on cluster workers.
    model_path = hf_hub_download(MODEL_REPO, "onnx/model.onnx", revision=MODEL_REVISION)
    config_path = hf_hub_download(MODEL_REPO, "config.json", revision=MODEL_REVISION)
    # ImageNet labels list synonyms ("tabby, tabby cat"); keep the first name.
    id2label = json.loads(Path(config_path).read_text())["id2label"]
    labels = {int(i): name.split(",")[0] for i, name in id2label.items()}
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1  # the engine runs partitions in parallel already
    session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])

    for input_batch in batches:
        for start in range(0, input_batch.num_rows, INFERENCE_BATCH_SIZE):
            batch = input_batch.slice(start, INFERENCE_BATCH_SIZE)
            paths = batch.column("path").to_pylist()
            if not paths:
                continue
            pixels = np.stack([preprocess(path) for path in paths])
            (logits,) = session.run(["logits"], {"pixel_values": pixels})
            exp = np.exp(logits - logits.max(axis=1, keepdims=True))
            probabilities = exp / exp.sum(axis=1, keepdims=True)
            rows = {name: [] for name in ARROW_SCHEMA.names}
            for path, p in zip(paths, probabilities):
                best = np.argsort(-p)[:TOP_K]
                rows["file"].append(Path(path).name)
                rows["label"].append(labels[int(best[0])])
                rows["score"].append(float(p[best[0]]))
                rows["top3"].append([{"label": labels[int(i)], "score": float(p[i])} for i in best])
            yield pa.RecordBatch.from_pydict(rows, schema=ARROW_SCHEMA)


def run(spark: SparkSession) -> dict[str, DataFrame]:
    # A table of paths, one row per image. Each task opens only the files in its own rows.
    files = sorted(str(p) for p in IMAGES.iterdir() if p.suffix.lower() in {".jpg", ".jpeg", ".png"})
    paths = spark.createDataFrame([(p,) for p in files], "path string").repartition(2)

    # Classify, then save once: the queries below must not run the model again.
    predictions_path = temp_dir("predictions-")
    paths.mapInArrow(classify, SCHEMA).write.mode("overwrite").parquet(predictions_path)
    predictions = spark.read.parquet(predictions_path)

    # Low-confidence predictions, with the runner-up label, for a person to check.
    review = predictions.filter(F.col("score") < REVIEW_BELOW).select(
        "file",
        "label",
        "score",
        F.col("top3")[1]["label"].alias("runner_up"),
        F.col("top3")[1]["score"].alias("runner_up_score"),
    )
    return {"predictions": predictions.orderBy("file"), "review": review.orderBy("file")}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["predictions"].select("file", "label", F.round("score", 3).alias("score")).show(truncate=False)
    print("Needs review:")
    results["review"].select(
        "file",
        "label",
        F.round("score", 3).alias("score"),
        "runner_up",
        F.round("runner_up_score", 3).alias("runner_up_score"),
    ).show(truncate=False)
    spark.stop()
    server.stop()
