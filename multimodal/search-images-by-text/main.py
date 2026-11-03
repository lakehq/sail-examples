# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "onnxruntime==1.23.2",
#   "tokenizers==0.22.1",
#   "huggingface-hub==0.36.0",
#   "pillow==13.0.0",
# ]
# ///
"""Search a folder of photos with plain-language text, using CLIP inside the engine.

CLIP has two encoders that map images and text into the same vector space. Both run in
`mapInArrow`, the vectors are stored as ARRAY<FLOAT>, and `vector_cosine_similarity` ranks the
photos for each query. Each photo has a written caption, which gives a retrieval score in SQL.
"""

import atexit
import csv
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# CLIP ViT-B/32, with the image and text encoders exported to ONNX as two models.
VISION_REPO = "Qdrant/clip-ViT-B-32-vision"
VISION_REVISION = "e0c24ed0fa57fa3e4f97f30de74c51d944036ace"  # pinned: every run uses the same weights
TEXT_REPO = "Qdrant/clip-ViT-B-32-text"
TEXT_REVISION = "48ca1db27cb4063eb311ec2aa7f087a808112876"
CONTEXT_LENGTH = 77  # CLIP reads at most 77 tokens
INFERENCE_BATCH_SIZE = 16  # bound model tensors independently of Arrow input batch size

IMAGES = Path(__file__).resolve().parent / "images"  # sources and licenses: images/CREDITS.md
QUERIES = [
    "a dog playing in the snow",
    "a sailboat on the water",
    "a cat asleep",
    "a city street at night",
    "breakfast on a plate",
]

IMAGE_COLUMNS = "file string, embedding array<float>"
IMAGE_SCHEMA = pa.schema([("file", pa.string()), ("embedding", pa.list_(pa.float32()))])
TEXT_COLUMNS = "query_id long, embedding array<float>"
TEXT_SCHEMA = pa.schema([("query_id", pa.int64()), ("embedding", pa.list_(pa.float32()))])


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def photo_catalog() -> list[tuple[str, str, str]]:
    """(path, file, caption) for every photo, from images/captions.csv."""
    with open(IMAGES / "captions.csv", newline="") as f:
        return [(str(IMAGES / row["file"]), row["file"], row["caption"]) for row in csv.DictReader(f)]


def unit_rows(vectors):
    """Scale each row to length 1, so cosine similarity is a dot product."""
    import numpy as np

    return vectors / np.clip(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-12, None)


def embed_images(batches):
    """mapInArrow function: everything before the loop runs once per partition."""
    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from PIL import Image

    # Download where the task runs (cached per machine), so this also works on cluster workers.
    model_path = hf_hub_download(VISION_REPO, "model.onnx", revision=VISION_REVISION)
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1  # the engine runs partitions in parallel already
    session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
    mean = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
    std = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)

    def preprocess(path):
        # CLIP's preprocessing: shortest side to 224 (bicubic), center crop 224, normalize.
        with Image.open(path) as image:
            image = image.convert("RGB")
        # The shorter side becomes exactly 224; the longer side is scaled and truncated, as transformers does.
        short, long = sorted(image.size)
        resized = (224, int(224 * long / short))
        image = image.resize(resized if image.width <= image.height else resized[::-1], Image.BICUBIC)
        left, top = (image.width - 224) // 2, (image.height - 224) // 2
        image = image.crop((left, top, left + 224, top + 224))
        return ((np.asarray(image, dtype=np.float32) / 255.0 - mean) / std).transpose(2, 0, 1)

    for input_batch in batches:
        for start in range(0, input_batch.num_rows, INFERENCE_BATCH_SIZE):
            batch = input_batch.slice(start, INFERENCE_BATCH_SIZE)
            paths = batch.column("path").to_pylist()
            if not paths:
                continue
            (vectors,) = session.run(
                ["image_embeds"], {"pixel_values": np.stack([preprocess(p) for p in paths])}
            )
            yield pa.RecordBatch.from_pydict(
                {"file": [Path(p).name for p in paths], "embedding": unit_rows(vectors).tolist()},
                schema=IMAGE_SCHEMA,
            )


def embed_texts(batches):
    """mapInArrow function for the text encoder."""
    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    model_path = hf_hub_download(TEXT_REPO, "model.onnx", revision=TEXT_REVISION)
    tokenizer_path = hf_hub_download(TEXT_REPO, "tokenizer.json", revision=TEXT_REVISION)
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1
    session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
    tokenizer = Tokenizer.from_file(tokenizer_path)
    tokenizer.enable_truncation(CONTEXT_LENGTH)
    # Fixed-length padding makes a row's vector independent of the other rows in its batch.
    end_of_text = "<|endoftext|>"
    tokenizer.enable_padding(
        length=CONTEXT_LENGTH, pad_id=tokenizer.token_to_id(end_of_text), pad_token=end_of_text
    )

    for input_batch in batches:
        for start in range(0, input_batch.num_rows, INFERENCE_BATCH_SIZE):
            batch = input_batch.slice(start, INFERENCE_BATCH_SIZE)
            texts = batch.column("text").to_pylist()
            if not texts:
                continue
            encodings = tokenizer.encode_batch(texts)
            feeds = {
                "input_ids": np.array([e.ids for e in encodings], dtype=np.int64),
                "attention_mask": np.array([e.attention_mask for e in encodings], dtype=np.int64),
            }
            (vectors,) = session.run(["text_embeds"], feeds)
            yield pa.RecordBatch.from_pydict(
                {"query_id": batch.column("query_id"), "embedding": unit_rows(vectors).tolist()},
                schema=TEXT_SCHEMA,
            )


def caption_recall(ranked: DataFrame) -> DataFrame:
    """Score each caption against its own photo, even when captions repeat."""
    matches = ranked.filter(F.col("target_file").isNotNull() & (F.col("file") == F.col("target_file")))
    return matches.agg(
        F.count("*").alias("captions"),
        F.avg((F.col("rank") == 1).cast("double")).alias("recall_at_1"),
        F.avg((F.col("rank") <= 5).cast("double")).alias("recall_at_5"),
    )


def run(spark: SparkSession) -> dict[str, DataFrame]:
    catalog = photo_catalog()
    photos = spark.createDataFrame(catalog, "path string, file string, caption string")

    # Embed the photos once and save the vectors: every query below reads them back.
    vectors_path = temp_dir("image-vectors-")
    photo_paths = photos.select("path").repartition(2)
    photo_paths.mapInArrow(embed_images, IMAGE_COLUMNS).write.mode("overwrite").parquet(vectors_path)
    image_vectors = spark.read.parquet(vectors_path).withColumnRenamed("embedding", "image_vec")

    # Free-text queries, plus each photo's caption to score retrieval.
    query_rows = [(i, text, None) for i, text in enumerate(QUERIES)]
    query_rows.extend((i + len(QUERIES), caption, file) for i, (_, file, caption) in enumerate(catalog))
    queries = spark.createDataFrame(query_rows, "query_id long, text string, target_file string")
    query_vectors = queries.mapInArrow(embed_texts, TEXT_COLUMNS).withColumnRenamed("embedding", "text_vec")

    scored = query_vectors.crossJoin(image_vectors).select(
        "query_id",
        "file",
        F.expr("vector_cosine_similarity(text_vec, image_vec)").cast("double").alias("score"),
    )
    rank = F.row_number().over(Window.partitionBy("query_id").orderBy(F.desc("score"), "file"))
    ranked = scored.withColumn("rank", rank).join(queries, "query_id").join(photos.drop("path"), "file")

    top3 = ranked.filter((F.col("query_id") < len(QUERIES)) & (F.col("rank") <= 3)).select(
        "query_id", "text", "rank", "file", "score"
    )
    # A caption query is a hit at k when the photo it describes ranks in the top k.
    recall = caption_recall(ranked)
    return {"top3": top3.orderBy("query_id", "rank"), "recall": recall}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    for query_id, query in enumerate(QUERIES):
        print(query)
        for row in results["top3"].filter(F.col("query_id") == query_id).collect():
            print(f"  {row['rank']}. {row['score']:.3f}  {row['file']}")
    results["recall"].show()
    spark.stop()
    server.stop()
