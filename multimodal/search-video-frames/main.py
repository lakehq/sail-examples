# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "av==18.0.0",
#   "onnxruntime==1.23.2",
#   "tokenizers==0.22.1",
#   "huggingface-hub==0.36.0",
#   "pillow==13.0.0",
# ]
# ///
"""Find the scenes in videos that match a text query, with CLIP inside the engine.

The pattern: a table of video paths goes through `mapInArrow`, which decodes each video, keeps
one frame per second, and returns one CLIP vector per kept frame (one video in, many small rows
out). SQL window functions then split each video into scenes where consecutive frames stop
looking alike, and rank the scenes for each text query.
"""

import atexit
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

SCENE_CHANGE_BELOW = 0.8  # a new scene starts when a frame is less similar than this to the one before
QUERIES = ["a rocket launch at night", "a black cat", "a waterfall", "a tram on a snowy street"]
VIDEOS = Path(__file__).resolve().parent / "videos"  # sources and licenses: videos/CREDITS.md
FRAMES_PER_CHUNK = 32  # frames per model call and per output batch

FRAME_COLUMNS = "video string, second int, embedding array<float>"
FRAME_SCHEMA = pa.schema(
    [("video", pa.string()), ("second", pa.int32()), ("embedding", pa.list_(pa.float32()))]
)
TEXT_COLUMNS = "query_id long, embedding array<float>"
TEXT_SCHEMA = pa.schema([("query_id", pa.int64()), ("embedding", pa.list_(pa.float32()))])


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def unit_rows(vectors):
    """Scale each row to length 1, so cosine similarity is a dot product."""
    import numpy as np

    return vectors / np.clip(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-12, None)


def embed_frames(batches):
    """mapInArrow function: one video in, one row per kept frame out, in chunks."""
    import av
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

    def preprocess(image):
        # CLIP's preprocessing: shortest side to 224 (bicubic), center crop 224, normalize.
        # The shorter side becomes exactly 224; the longer side is scaled and truncated, as transformers does.
        short, long = sorted(image.size)
        resized = (224, int(224 * long / short))
        image = image.resize(resized if image.width <= image.height else resized[::-1], Image.BICUBIC)
        left, top = (image.width - 224) // 2, (image.height - 224) // 2
        image = image.crop((left, top, left + 224, top + 224))
        return ((np.asarray(image, dtype=np.float32) / 255.0 - mean) / std).transpose(2, 0, 1)

    def embedded(video, seconds, pixels):
        (vectors,) = session.run(["image_embeds"], {"pixel_values": np.stack(pixels)})
        rows = {"video": [video] * len(seconds), "second": seconds, "embedding": unit_rows(vectors).tolist()}
        return pa.RecordBatch.from_pydict(rows, schema=FRAME_SCHEMA)

    for batch in batches:
        for path in batch.column("path").to_pylist():
            video, seconds, pixels, next_second = Path(path).name, [], [], 0
            with av.open(path) as container:
                stream = container.streams.video[0]
                origin = stream.start_time or 0
                for frame in container.decode(stream):
                    if frame.pts is None:
                        continue
                    # Exact time since the stream started, as a fraction of seconds (no float rounding).
                    elapsed = (frame.pts - origin) * stream.time_base
                    if elapsed < next_second:
                        continue
                    # Keep the first frame at or after each whole second, labeled with its second.
                    second = int(elapsed)
                    seconds.append(second)
                    pixels.append(preprocess(frame.to_image()))
                    next_second = second + 1
                    # Embed and emit fixed-size chunks, so memory and batches stay small however long a
                    # video is.
                    if len(pixels) == FRAMES_PER_CHUNK:
                        yield embedded(video, seconds, pixels)
                        seconds, pixels = [], []
            if pixels:
                yield embedded(video, seconds, pixels)


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


def run(spark: SparkSession) -> dict[str, DataFrame]:
    # What each scene really shows, to check the results by eye.
    truth = spark.read.csv(
        str(VIDEOS / "scenes.csv"), header=True, schema="video string, start double, end double, shows string"
    )

    # A table of paths, one row per video. Sample and embed the frames, then save the vectors once.
    frames_path = temp_dir("frames-")
    paths = sorted(str(p) for p in VIDEOS.glob("*.mp4"))
    videos = spark.createDataFrame([(p,) for p in paths], "path string").repartition(2)
    videos.mapInArrow(embed_frames, FRAME_COLUMNS).write.mode("overwrite").parquet(frames_path)
    frames = spark.read.parquet(frames_path)

    # Scenes: a frame that looks unlike the one before it starts a new scene.
    in_order = Window.partitionBy("video").orderBy("second")
    previous = F.lag("embedding").over(in_order)
    similarity = F.call_function("vector_cosine_similarity", F.col("embedding"), previous)
    new_scene = F.when(previous.isNull() | (similarity < SCENE_CHANGE_BELOW), 1).otherwise(0)
    numbered = (
        frames.withColumn("similarity", similarity.cast("double"))
        .withColumn("new_scene", new_scene)
        .withColumn(
            "scene",
            F.sum("new_scene").over(in_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
    )
    scenes = numbered.groupBy("video", "scene").agg(
        F.min("second").alias("start"),
        (F.max("second") + 1).alias("end"),
        F.count("*").alias("frames"),
        # How alike consecutive sampled frames are: across the cut that starts the scene (null for the
        # first scene), and the lowest within the scene. Use these to choose SCENE_CHANGE_BELOW.
        F.max(F.when(F.col("new_scene") == 1, F.col("similarity"))).alias("similarity_at_cut"),
        F.min(F.when(F.col("new_scene") == 0, F.col("similarity"))).alias("lowest_similarity_within"),
    )

    # Search: a scene's score for a query is its best frame's score.
    queries = spark.createDataFrame(list(enumerate(QUERIES)), "query_id long, text string")
    query_vectors = queries.mapInArrow(embed_texts, TEXT_COLUMNS).withColumnRenamed("embedding", "text_vec")
    scored = query_vectors.crossJoin(numbered.withColumnRenamed("embedding", "frame_vec")).select(
        "query_id",
        "video",
        "scene",
        F.call_function("vector_cosine_similarity", F.col("text_vec"), F.col("frame_vec"))
        .cast("double")
        .alias("score"),
    )
    best = scored.groupBy("query_id", "video", "scene").agg(F.max("score").alias("score"))
    rank = F.row_number().over(Window.partitionBy("query_id").orderBy(F.desc("score"), "video", "scene"))
    # A found scene shows what the true scene that contains its first sampled second shows.
    shows = (
        scenes.alias("s")
        .join(
            truth.alias("t"),
            (F.col("s.video") == F.col("t.video"))
            & (F.col("s.start") >= F.col("t.start"))
            & (F.col("s.start") < F.col("t.end")),
            "left",
        )
        .select("s.video", "s.scene", "t.shows")
    )
    top2 = (
        best.withColumn("rank", rank)
        .filter("rank <= 2")
        .join(queries, "query_id")
        .join(scenes, ["video", "scene"])
        .join(shows, ["video", "scene"], "left")
        .select("query_id", "text", "rank", "video", "start", "end", "score", "shows")
    )
    return {"scenes": scenes.orderBy("video", "scene"), "search": top2.orderBy("query_id", "rank")}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["scenes"].select(
        "video",
        "scene",
        "start",
        "end",
        "frames",
        F.round("similarity_at_cut", 3).alias("similarity_at_cut"),
        F.round("lowest_similarity_within", 3).alias("lowest_similarity_within"),
    ).show()
    for row in results["search"].collect():
        print(
            f"{row['text']!r} #{row['rank']}: {row['video']} {row['start']}s to {row['end']}s"
            f"  score {row['score']:.3f}  (the scene shows: {row['shows']})"
        )
    spark.stop()
    server.stop()
