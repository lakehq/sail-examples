# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Curate simulated robot demonstrations with episode-level quality checks."""

import hashlib
import json
import math
import os
import tempfile
import urllib.request
from pathlib import Path

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


def curate(frames, episodes, tasks, fps):
    """Return a quality report and a whole-episode split for a two-coordinate task."""
    order = Window.partitionBy("episode_index").orderBy("frame_index", "timestamp")
    previous = F.lag("timestamp").over(order)
    timestamp = F.col("timestamp").cast("double")
    bad_time = (
        timestamp.isNull()
        | F.isnan(timestamp)
        | (F.abs(timestamp - F.col("frame_index") / fps) > TIME_TOLERANCE)
        | (previous.isNotNull() & (F.abs(timestamp - previous - 1 / fps) > TIME_TOLERANCE))
    )
    invalid = F.lit(False)
    for name in ("observation.state", "action"):
        vector = F.col(f"`{name}`")
        invalid = invalid | vector.isNull() | (F.size(vector) != 2)
        for coordinate in (0, 1):
            value = F.get(vector, coordinate)
            invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
    counted = (
        frames.withColumn("bad_time", F.coalesce(bad_time, F.lit(True)).cast("long"))
        .withColumn("invalid_vector", invalid.cast("long"))
        .groupBy("episode_index")
        .agg(
            F.count("*").alias("frames"),
            F.countDistinct("frame_index").alias("distinct_frames"),
            F.min("frame_index").alias("first_frame"),
            F.max("frame_index").alias("last_frame"),
            F.sum("bad_time").alias("bad_timestamps"),
            F.sum("invalid_vector").alias("invalid_vectors"),
            F.min("task_index").alias("task_index"),
            F.countDistinct("task_index").alias("task_count"),
            F.count("task_index").alias("task_frames"),
        )
    )
    # A full join keeps both entirely missing episodes and unlisted episode IDs visible.
    report = episodes.select("episode_index", F.col("length").alias("expected_frames")).join(
        counted, "episode_index", "full"
    )
    report = report.join(tasks, "task_index", "left").fillna(
        0, subset=["frames", "distinct_frames", "bad_timestamps", "invalid_vectors", "task_count"]
    )
    usable = (
        (F.col("frames") == F.col("expected_frames"))
        & (F.col("distinct_frames") == F.col("expected_frames"))
        & (F.col("first_frame") == 0)
        & (F.col("last_frame") == F.col("expected_frames") - 1)
        & (F.col("bad_timestamps") == 0)
        & (F.col("invalid_vectors") == 0)
        & (F.col("task_count") == 1)
        & (F.col("task_frames") == F.col("frames"))
        & F.col("task").isNotNull()
    )
    report = report.select(
        "episode_index",
        "task",
        "expected_frames",
        "frames",
        (F.col("frames") - F.col("distinct_frames")).alias("duplicate_frames"),
        "bad_timestamps",
        "invalid_vectors",
        F.coalesce(usable, F.lit(False)).alias("usable"),
    )
    selected = report.filter("usable").select(
        "episode_index",
        "task",
        "frames",
        F.when(F.col("episode_index") % 5 == 0, "validation").otherwise("train").alias("split"),
    )
    return {"quality": report.orderBy("episode_index"), "selection": selected.orderBy("episode_index")}


def run(spark):
    info = json.loads(dataset_file("meta/info.json").read_text())
    assert info["codebase_version"] == "v3.0" and info["fps"] == 10
    frames = spark.read.parquet(str(dataset_file(DATA)))
    episodes = spark.read.parquet(str(dataset_file(EPISODES)))
    tasks = spark.read.parquet(str(dataset_file(TASKS))).select(
        "task_index", F.col("__index_level_0__").alias("task")
    )
    return curate(frames, episodes, tasks, info["fps"])


def reference():
    """An independent Python oracle, checked by the maintenance harness."""
    import pyarrow.parquet as pq

    fps = json.loads(dataset_file("meta/info.json").read_text())["fps"]
    tasks = {r["task_index"]: r["__index_level_0__"] for r in pq.read_table(dataset_file(TASKS)).to_pylist()}
    lengths = {r["episode_index"]: r["length"] for r in pq.read_table(dataset_file(EPISODES)).to_pylist()}
    grouped = {episode: [] for episode in lengths}
    for row in pq.read_table(dataset_file(DATA)).to_pylist():
        grouped.setdefault(row["episode_index"], []).append(row)
    quality, selection = [], []
    for episode, rows in sorted(grouped.items()):
        rows.sort(key=lambda r: (r["frame_index"], r["timestamp"]))
        ids = [r["frame_index"] for r in rows]
        bad_times = invalid = 0
        for index, row in enumerate(rows):
            t = row["timestamp"]
            bad_times += int(
                not math.isfinite(t)
                or abs(t - row["frame_index"] / fps) > TIME_TOLERANCE
                or (index > 0 and abs(t - rows[index - 1]["timestamp"] - 1 / fps) > TIME_TOLERANCE)
            )
            invalid += int(
                any(
                    len(row[name]) != 2 or not all(math.isfinite(x) for x in row[name])
                    for name in ("observation.state", "action")
                )
            )
        task_ids = {r["task_index"] for r in rows}
        task = tasks.get(min(task_ids)) if task_ids else None
        usable = (
            ids == list(range(lengths.get(episode, -1)))
            and bad_times == invalid == 0
            and len(task_ids) == 1
            and task is not None
        )
        quality.append(
            {
                "episode_index": episode,
                "task": task,
                "expected_frames": lengths.get(episode),
                "frames": len(rows),
                "duplicate_frames": len(ids) - len(set(ids)),
                "bad_timestamps": bad_times,
                "invalid_vectors": invalid,
                "usable": usable,
            }
        )
        if usable:
            selection.append(
                {
                    "episode_index": episode,
                    "task": task,
                    "frames": len(rows),
                    "split": "validation" if episode % 5 == 0 else "train",
                }
            )
    return {"quality": quality, "selection": selection}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        for name, result in run(spark).items():
            print(name)
            result.show(10, truncate=False)
    finally:
        spark.stop()
        server.stop()
