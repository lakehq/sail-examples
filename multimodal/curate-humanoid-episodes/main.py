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
"""Curate humanoid robot episodes and align every camera frame of one episode."""

import hashlib
import json
import os
import tempfile
import urllib.request
from pathlib import Path

import pyarrow as pa
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

REVISION = "57faa2cf516e008f96d91fe3b67ad53a74f012e6"
SOURCE = "https://huggingface.co/datasets/unitreerobotics/G1_Dex3_BlockStacking_Dataset"
DATA = "data/chunk-000/file-000.parquet"
EPISODES = "meta/episodes/chunk-000/file-000.parquet"
TASKS = "meta/tasks.parquet"
HASHES = {
    "meta/info.json": "3466132be194fb9487db582dd37afbb2585f924d1b722a8fc6ac30920a471927",
    DATA: "d49fd8e3e8d5d7bd682897644017c0a2542275af80f2fe0a936734d8abbd1d7b",
    EPISODES: "127bf236532cde778b197774f94e1869de6e5db7ab08aa7296baad737e1ba658",
    TASKS: "7bbcf6b738f9b336dade138404208e9ada671c50be38eb5e460ba443668330aa",
}
SIZE_LIMIT = 70_000_000  # bytes per downloaded file; the state/action Parquet is about 63 MB
CAMERAS = ("cam_left_high", "cam_right_high", "cam_left_wrist", "cam_right_wrist")
EPISODE = 279  # the episode whose camera frames are decoded
JOINTS = 28  # 14 arm joints and 14 hand joints, in state and in action
TIME_TOLERANCE = 1e-4  # seconds, accommodates this dataset's float32 timestamps
# SHA-256 over the concatenated hex SHA-256 of every decoded RGB frame, in frame order.
# The videos are read in byte ranges, so the decoded pixels are verified instead of whole files.
FRAME_DIGESTS = {
    "cam_left_high": "8cac56cfea9fa18cf0d2b3d92d8c7933c35173f829e1017cb9f2734fd585220c",
    "cam_right_high": "dd817452c211fea2a30eb87d4298d4407ebcdc28eb6a33a385c0005713d30dbe",
    "cam_left_wrist": "670929d773d9c01d1589f1fb8843a71864028d544bde920443d4e4cad78d2343",
    "cam_right_wrist": "7057d2b0f27c5c111e263679aa11f30ad2ba67f5545e2eac6619bfe28cd46f55",
}


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
                        if size > SIZE_LIMIT:
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


FRAME_COLUMNS = (
    "camera string, episode_index long, frame_number long, video_timestamp double, "
    "episode_timestamp double, width int, height int, rgb_sha256 string"
)
FRAME_SCHEMA = pa.schema(
    [
        ("camera", pa.string()),
        ("episode_index", pa.int64()),
        ("frame_number", pa.int64()),
        ("video_timestamp", pa.float64()),
        ("episode_timestamp", pa.float64()),
        ("width", pa.int32()),
        ("height", pa.int32()),
        ("rgb_sha256", pa.string()),
    ]
)


def decode_every_frame(batches):
    """Decode every frame of each camera segment and emit one small record per frame."""
    import av

    for batch in batches:
        for segment in batch.to_pylist():
            start, end = segment["video_start"], segment["video_end"]
            digest = hashlib.sha256()
            rows = []
            number = 0
            # The shared video file holds many episodes; seek to this one over HTTP instead of downloading it.
            with av.open(segment["video_url"], timeout=60) as container:
                stream = container.streams.video[0]
                container.seek(int(start / stream.time_base), stream=stream, backward=True)
                for frame in container.decode(stream):
                    if frame.pts is None:
                        raise ValueError("video frame has no presentation timestamp")
                    video_time = float(frame.pts * stream.time_base)
                    if video_time + 1e-6 < start:
                        continue
                    if video_time >= end - 1e-6:
                        break
                    rgb_sha256 = hashlib.sha256(frame.to_ndarray(format="rgb24").tobytes()).hexdigest()
                    digest.update(rgb_sha256.encode())
                    rows.append(
                        {
                            "camera": segment["camera"],
                            "episode_index": segment["episode_index"],
                            "frame_number": number,
                            "video_timestamp": video_time,
                            "episode_timestamp": video_time - start,
                            "width": frame.width,
                            "height": frame.height,
                            "rgb_sha256": rgb_sha256,
                        }
                    )
                    number += 1
                    if len(rows) == 256:
                        yield pa.RecordBatch.from_pylist(rows, schema=FRAME_SCHEMA)
                        rows = []
            if digest.hexdigest() != FRAME_DIGESTS[segment["camera"]]:
                raise ValueError(f"decoded frames differ from the pinned revision: {segment['camera']}")
            if rows:
                yield pa.RecordBatch.from_pylist(rows, schema=FRAME_SCHEMA)


def curate(frames, episodes, tasks, fps):
    """Return a quality report and a whole-episode split for 28-joint state and action vectors."""
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
        bad_value = F.exists(vector, lambda v: v.isNull() | F.isnan(v) | (F.abs(v) == float("inf")))
        invalid = invalid | vector.isNull() | (F.size(vector) != JOINTS) | bad_value
    counted = (
        frames.withColumn("bad_time", F.coalesce(bad_time, F.lit(True)).cast("long"))
        .withColumn("invalid_vector", F.coalesce(invalid, F.lit(True)).cast("long"))
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
    quality = report.select(
        "episode_index",
        "task",
        "expected_frames",
        "frames",
        (F.col("frames") - F.col("distinct_frames")).alias("duplicate_frames"),
        "bad_timestamps",
        "invalid_vectors",
        F.coalesce(usable, F.lit(False)).alias("usable"),
    )
    selection = quality.filter("usable").select(
        "episode_index",
        "task",
        "frames",
        F.when(F.col("episode_index") % 5 == 0, "validation").otherwise("train").alias("split"),
    )
    return {"quality": quality.orderBy("episode_index"), "selection": selection.orderBy("episode_index")}


def align(frames, decoded, fps):
    """Match each decoded frame to the nearest state record within half a record period."""
    states = frames.select(
        F.col("episode_index").alias("state_episode"),
        "frame_index",
        F.col("timestamp").cast("double").alias("state_timestamp"),
    )
    error = F.abs(F.col("episode_timestamp") - F.col("state_timestamp"))
    candidates = decoded.join(
        states,
        (F.col("episode_index") == F.col("state_episode")) & (error <= 0.5 / fps + TIME_TOLERANCE),
        "left",
    ).withColumn("time_error_seconds", error)
    nearest = Window.partitionBy("camera", "episode_index", "frame_number").orderBy(
        F.col("time_error_seconds").asc_nulls_last(), F.col("frame_index").asc_nulls_last()
    )
    alignment = (
        candidates.withColumn("rank", F.row_number().over(nearest))
        .filter("rank = 1")
        .select(
            "camera",
            "episode_index",
            "frame_number",
            "episode_timestamp",
            "frame_index",
            "time_error_seconds",
            F.col("frame_index").isNotNull().alias("matched"),
        )
    )
    coverage = alignment.groupBy("camera").agg(
        F.count("*").alias("decoded_frames"),
        F.sum(F.col("matched").cast("long")).alias("matched_frames"),
        F.countDistinct("frame_index").alias("distinct_records"),
        F.max("time_error_seconds").alias("max_time_error_seconds"),
    )
    return {
        "alignment": alignment.orderBy("camera", "frame_number"),
        "coverage": coverage.orderBy("camera"),
    }


def camera_segments(episodes):
    """One row per camera: the video file holding the episode and the episode's time span in it."""
    segments = None
    for camera in CAMERAS:
        prefix = f"videos/observation.images.{camera}"
        segment = episodes.filter(F.col("episode_index") == EPISODE).select(
            F.lit(camera).alias("camera"),
            "episode_index",
            F.concat(
                F.lit(f"{SOURCE}/resolve/{REVISION}/{prefix}/chunk-"),
                F.lpad(F.col(f"`{prefix}/chunk_index`").cast("string"), 3, "0"),
                F.lit("/file-"),
                F.lpad(F.col(f"`{prefix}/file_index`").cast("string"), 3, "0"),
                F.lit(".mp4"),
            ).alias("video_url"),
            F.col(f"`{prefix}/from_timestamp`").cast("double").alias("video_start"),
            F.col(f"`{prefix}/to_timestamp`").cast("double").alias("video_end"),
        )
        segments = segment if segments is None else segments.unionByName(segment)
    return segments


def run(spark):
    info = json.loads(dataset_file("meta/info.json").read_text())
    assert info["codebase_version"] == "v3.0" and float(info["fps"]) == 30.0
    assert all(info["features"][name]["shape"] == [JOINTS] for name in ("observation.state", "action"))
    fps = float(info["fps"])
    frames = spark.read.parquet(str(dataset_file(DATA)))
    episodes = spark.read.parquet(str(dataset_file(EPISODES)))
    tasks = spark.read.parquet(str(dataset_file(TASKS))).select(
        "task_index", F.col("__index_level_0__").alias("task")
    )
    results = curate(frames, episodes, tasks, fps)

    # Materialize decoded rows once, so the outputs reuse them without decoding again.
    import atexit
    import shutil

    temporary = tempfile.mkdtemp(prefix="humanoid-frames-")
    atexit.register(shutil.rmtree, temporary, ignore_errors=True)
    segments = camera_segments(episodes)
    if segments.count() != len(CAMERAS):
        raise ValueError(f"episode {EPISODE} is missing from the episode metadata")
    segments.repartition(len(CAMERAS)).mapInArrow(decode_every_frame, FRAME_COLUMNS).write.mode(
        "overwrite"
    ).parquet(temporary)
    episode_frames = frames.filter(F.col("episode_index") == EPISODE)
    results.update(align(episode_frames, spark.read.parquet(temporary), fps))
    return results


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        results = run(spark)
        results["selection"].groupBy("split").count().orderBy("split").show()
        results["quality"].filter(~F.col("usable")).show(truncate=False)
        results["coverage"].show(truncate=False)
        results["alignment"].show(5, truncate=False)
    finally:
        spark.stop()
        server.stop()
