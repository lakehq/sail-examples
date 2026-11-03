# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "av==18.0.0",
#   "numpy==2.5.4",
# ]
# ///
"""Align sampled simulation camera frames with robot state and action records."""

import hashlib
import json
import math
import os
import tempfile
import urllib.request
from pathlib import Path

import pyarrow as pa
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

REVISION = "7628202a2180972f291ba1bc6723834921e72c19"
SOURCE = "https://huggingface.co/datasets/lerobot/pusht"
DATA = "data/chunk-000/file-000.parquet"
EPISODES = "meta/episodes/chunk-000/file-000.parquet"
TASKS = "meta/tasks.parquet"
HASHES = {
    "meta/info.json": "0b46becc21ad92b117da1e95daf44110bb21d0bde75638b4ad167c8ccd959d38",
    DATA: "9abc0431f12d10c33b5b37b1bf1e6e557c8ae1c8c29fb86658969e48fcbbbf01",
    EPISODES: "bc1226f33d3d1635ec1954f9942073709be8e106fde5f4f16b9d52edc4e0ebc4",
    TASKS: "95535f5152771bc0a6ab0fd93d23d7c1fecd9b7a13dc117852dfdd94f5b2c711",
}
VIDEO = "videos/observation.image/chunk-000/file-000.mp4"
HASHES[VIDEO] = "f58d11857651dad5983c021b56a50a68a6ce19068834c1fb5cac099219fb3a78"
EPISODE_LIMIT = 3
TIME_TOLERANCE = 1e-4  # seconds, accommodates this dataset's float32 timestamps


def dataset_file(name):
    """Fetch only allowlisted files; verify downloads and cached copies."""
    expected = HASHES[name]
    path = Path(__file__).resolve().parent / ".data" / REVISION / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as target:
                temporary = Path(target.name)
                with urllib.request.urlopen(f"{SOURCE}/resolve/{REVISION}/{name}", timeout=60) as response:
                    size = 0
                    while chunk := response.read(1024 * 1024):
                        size += len(chunk)
                        if size > 8_000_000:
                            raise ValueError(f"download exceeds size limit: {name}")
                        target.write(chunk)
            if hashlib.sha256(temporary.read_bytes()).hexdigest() != expected:
                raise ValueError(f"SHA-256 mismatch: {name}")
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError(f"SHA-256 mismatch: {name}")
    return path


SAMPLE_COLUMNS = (
    "episode_index long, sample_index long, video_timestamp double, "
    "episode_timestamp double, width int, height int, rgb_sha256 string"
)
SAMPLE_SCHEMA = pa.schema(
    [
        ("episode_index", pa.int64()),
        ("sample_index", pa.int64()),
        ("video_timestamp", pa.float64()),
        ("episode_timestamp", pa.float64()),
        ("width", pa.int32()),
        ("height", pa.int32()),
        ("rgb_sha256", pa.string()),
    ]
)


def sample_camera_frames(batches):
    """Decode a path manifest, emitting at most one small row per episode second."""
    import av

    # Fetch where the task runs. Only paths/metadata and small outputs cross stages.
    path = dataset_file(VIDEO)
    for batch in batches:
        for episode in batch.to_pylist():
            start, end = episode["video_start"], episode["video_end"]
            with av.open(str(path)) as container:
                stream = container.streams.video[0]
                container.seek(int(start / stream.time_base), stream=stream, backward=True)
                next_second = 0
                for frame in container.decode(stream):
                    if frame.pts is None:
                        raise ValueError("video frame has no presentation timestamp")
                    video_time = float(frame.pts * stream.time_base)
                    if video_time + 1e-8 < start:
                        continue
                    if video_time >= end - 1e-8:
                        break
                    episode_time = video_time - start
                    if episode_time + 1e-8 < next_second:
                        continue
                    rgb = frame.to_ndarray(format="rgb24")
                    row = {
                        "episode_index": episode["episode_index"],
                        "sample_index": next_second,
                        "video_timestamp": video_time,
                        "episode_timestamp": episode_time,
                        "width": frame.width,
                        "height": frame.height,
                        "rgb_sha256": hashlib.sha256(rgb.tobytes()).hexdigest(),
                    }
                    yield pa.RecordBatch.from_pylist([row], schema=SAMPLE_SCHEMA)
                    next_second = math.floor(episode_time + 1e-8) + 1


def align(frames, samples, fps):
    """Nearest state within half a sample period; ties choose the earlier frame."""
    states = frames.select(
        F.col("episode_index").alias("state_episode"),
        "frame_index",
        F.col("timestamp").cast("double").alias("state_timestamp"),
        F.get(F.col("`observation.state`"), 0).cast("double").alias("state_x"),
        F.get(F.col("`observation.state`"), 1).cast("double").alias("state_y"),
        F.get("action", 0).cast("double").alias("action_x"),
        F.get("action", 1).cast("double").alias("action_y"),
    )
    error = F.abs(F.col("episode_timestamp") - F.col("state_timestamp"))
    candidates = samples.join(
        states,
        (F.col("episode_index") == F.col("state_episode")) & (error <= 0.5 / fps + TIME_TOLERANCE),
        "left",
    ).withColumn("time_error_seconds", error)
    nearest = Window.partitionBy("episode_index", "sample_index").orderBy(
        F.col("time_error_seconds").asc_nulls_last(), F.col("frame_index").asc_nulls_last()
    )
    alignment = (
        candidates.withColumn("rank", F.row_number().over(nearest))
        .filter("rank = 1")
        .select(
            "episode_index",
            "sample_index",
            "video_timestamp",
            "episode_timestamp",
            "width",
            "height",
            "rgb_sha256",
            "frame_index",
            "state_timestamp",
            "time_error_seconds",
            "state_x",
            "state_y",
            "action_x",
            "action_y",
            F.col("frame_index").isNotNull().alias("matched"),
        )
    )
    coverage = alignment.groupBy("episode_index").agg(
        F.count("*").alias("sampled_frames"),
        F.sum(F.col("matched").cast("long")).alias("matched_frames"),
        F.sum((~F.col("matched")).cast("long")).alias("unmatched_frames"),
        F.max("time_error_seconds").alias("max_time_error_seconds"),
    )
    return {
        "alignment": alignment.orderBy("episode_index", "sample_index"),
        "coverage": coverage.orderBy("episode_index"),
    }


def run(spark):
    info = json.loads(dataset_file("meta/info.json").read_text())
    assert info["codebase_version"] == "v3.0" and info["fps"] == 10
    # This small example deliberately handles one named video shard and camera.
    episodes = spark.read.parquet(str(dataset_file(EPISODES))).filter(F.col("episode_index") < EPISODE_LIMIT)
    manifest = episodes.select(
        "episode_index",
        F.col("`videos/observation.image/from_timestamp`").alias("video_start"),
        F.col("`videos/observation.image/to_timestamp`").alias("video_end"),
    ).repartition(2)
    frames = spark.read.parquet(str(dataset_file(DATA))).filter(F.col("episode_index") < EPISODE_LIMIT)
    # Materialize decoded rows once: both outputs reuse them without decoding again.
    import atexit
    import shutil

    temporary = tempfile.mkdtemp(prefix="robot-camera-")
    atexit.register(shutil.rmtree, temporary, ignore_errors=True)
    manifest.mapInArrow(sample_camera_frames, SAMPLE_COLUMNS).write.mode("overwrite").parquet(temporary)
    return align(frames, spark.read.parquet(temporary), info["fps"])


def reference():
    """Sequential decode and a Python nearest-neighbor scan, independent of SQL."""
    import av
    import pyarrow.parquet as pq

    fps = json.loads(dataset_file("meta/info.json").read_text())["fps"]
    episodes = [
        r for r in pq.read_table(dataset_file(EPISODES)).to_pylist() if r["episode_index"] < EPISODE_LIMIT
    ]
    states = pq.read_table(dataset_file(DATA)).to_pylist()
    # Decode the beginning once without seeking or using the worker sampler.
    boundaries = {
        r["episode_index"]: (
            r["videos/observation.image/from_timestamp"],
            r["videos/observation.image/to_timestamp"],
        )
        for r in episodes
    }
    alignment = []
    seen = set()
    with av.open(str(dataset_file(VIDEO))) as container:
        for frame in container.decode(video=0):
            t = float(frame.pts * frame.time_base)
            if t >= max(end for _, end in boundaries.values()) - 1e-8:
                break
            for episode, (start, end) in boundaries.items():
                local = t - start
                if t + 1e-8 < start or t >= end - 1e-8:
                    continue
                second = math.floor(local + 1e-8)
                if (episode, second) in seen:
                    continue
                seen.add((episode, second))
                candidates = [
                    r
                    for r in states
                    if r["episode_index"] == episode
                    and abs(r["timestamp"] - local) <= 0.5 / fps + TIME_TOLERANCE
                ]
                best = (
                    min(candidates, key=lambda r: (abs(r["timestamp"] - local), r["frame_index"]))
                    if candidates
                    else None
                )
                alignment.append(
                    {
                        "episode_index": episode,
                        "sample_index": second,
                        "video_timestamp": t,
                        "episode_timestamp": local,
                        "width": frame.width,
                        "height": frame.height,
                        "rgb_sha256": hashlib.sha256(frame.to_ndarray(format="rgb24").tobytes()).hexdigest(),
                        "frame_index": best["frame_index"] if best else None,
                        "state_timestamp": best["timestamp"] if best else None,
                        "time_error_seconds": abs(best["timestamp"] - local) if best else None,
                        "state_x": best["observation.state"][0] if best else None,
                        "state_y": best["observation.state"][1] if best else None,
                        "action_x": best["action"][0] if best else None,
                        "action_y": best["action"][1] if best else None,
                        "matched": best is not None,
                    }
                )
    alignment.sort(key=lambda r: (r["episode_index"], r["sample_index"]))
    coverage = []
    for episode in sorted(boundaries):
        rows = [r for r in alignment if r["episode_index"] == episode]
        errors = [r["time_error_seconds"] for r in rows if r["matched"]]
        coverage.append(
            {
                "episode_index": episode,
                "sampled_frames": len(rows),
                "matched_frames": len(errors),
                "unmatched_frames": len(rows) - len(errors),
                "max_time_error_seconds": max(errors) if errors else None,
            }
        )
    schema = pa.schema(
        [
            *SAMPLE_SCHEMA,
            ("frame_index", pa.int64()),
            ("state_timestamp", pa.float64()),
            ("time_error_seconds", pa.float64()),
            ("state_x", pa.float64()),
            ("state_y", pa.float64()),
            ("action_x", pa.float64()),
            ("action_y", pa.float64()),
            ("matched", pa.bool_()),
        ]
    )
    return {"alignment": pa.Table.from_pylist(alignment, schema=schema), "coverage": coverage}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        results = run(spark)
        results["coverage"].show(truncate=False)
        results["alignment"].drop("rgb_sha256").show(10, truncate=False)
    finally:
        spark.stop()
        server.stop()
