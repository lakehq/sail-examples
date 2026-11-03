# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "faster-whisper==1.2.1",
#   "ctranslate2==4.8.2",
#   "av==18.0.0",
#   "onnxruntime==1.23.2",
#   "tokenizers==0.22.1",
#   "huggingface-hub==0.36.0",
#   "tqdm==4.70.1",
# ]
# ///
"""Transcribe bounded spoken requests, score a visible intent baseline, then aggregate with SQL."""

from __future__ import annotations

import atexit
import hashlib
import io
import math
import re
import shutil
import tempfile
import wave
from pathlib import Path

DATASET_REPO = "PolyAI/minds14"
DATASET_REVISION = "28a8ee8911625fe71b2e0462c3dc14132d1058fa"
DATASET_FILE = "en-US/train-00000-of-00001.parquet"
DATASET_SHA256 = "37004471dc896ce20771b3fdda0ee8fb33ec7a030fb4e2fd047c561ec0a1ee30"
MODEL_REPO = "Systran/faster-whisper-tiny.en"
MODEL_REVISION = "0d3d19a32d3338f10357c0889762bd8d64bbdeba"
MODEL_FILES = ("model.bin", "config.json", "tokenizer.json", "vocabulary.txt")
MAX_CLIPS = 12
MAX_SECONDS = 180.0
HARD_MAX_CLIPS = 563
HARD_MAX_SECONDS = 3600.0
MAX_CLIP_SECONDS = 90.0
INTENTS = (
    "abroad",
    "address",
    "app_error",
    "atm_limit",
    "balance",
    "business_loan",
    "card_issues",
    "cash_deposit",
    "direct_debit",
    "freeze",
    "high_value_payment",
    "joint_account",
    "latest_transactions",
    "pay_bill",
)

# Each matching expression contributes one vote. A tied best score abstains.
RULES = {
    "abroad": (r"\babroad\b", r"\boverseas\b", r"\bforeign\b", r"\banother country\b"),
    "address": (r"\baddress\b", r"\bmoved (?:house|home)\b"),
    "app_error": (r"\bapp\b", r"\bapplication\b", r"\blog ?in\b", r"\bcrash\w*\b"),
    "atm_limit": (r"\b(?:atm|withdraw\w*|cash)\b.*\blimit\b", r"\blimit\b.*\b(?:atm|withdraw\w*)\b"),
    "balance": (r"\bbalance\b", r"\bavailable funds\b", r"\bhow much money\b"),
    "business_loan": (r"\bbusiness\b.*\b(?:loan|borrow)\b", r"\bloan\b.*\bbusiness\b"),
    "card_issues": (r"\bcard\b.*\b(?:declined|rejected|broken|damaged|working|expired)\b",),
    "cash_deposit": (r"\bdeposit\w*\b", r"\b(?:pay|put|add)\b.*\bcash\b"),
    "direct_debit": (r"\bdirect debit\b", r"\bstanding order\b"),
    "freeze": (r"\bfreeze\b", r"\bblock\b", r"\bsuspend\b"),
    "high_value_payment": (r"\b(?:large|huge|high value)\b.*\b(?:payment|transfer|sum|amount)\b",),
    "joint_account": (r"\bjoint\b", r"\bshared account\b"),
    "latest_transactions": (r"\btransactions?\b", r"\bstatement\b", r"\bpayment history\b"),
    "pay_bill": (r"\bbills?\b", r"\butility\b", r"\belectricity\b"),
}
MANIFEST_SCHEMA = (
    "request_id string, duplicate_group string, split string, reporting_group string, "
    "seconds double, decode_status string, path string, supplied_asr string, gold_intent string"
)
OBSERVATION_SCHEMA = "request_id string, whisper_text string, whisper_status string"


def temp_dir(prefix: str) -> str:
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_text(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def classify_text(text: str | None) -> str:
    """Return a keyword vote winner, or unknown for empty, unmatched or tied text."""
    normalized = normalize_text(text or "")
    scores = {
        intent: sum(bool(re.search(pattern, normalized)) for pattern in patterns)
        for intent, patterns in RULES.items()
    }
    best = max(scores.values())
    winners = [intent for intent, score in scores.items() if score == best]
    return winners[0] if best and len(winners) == 1 else "unknown"


def validate_bounds(max_clips, max_seconds):
    if (
        not isinstance(max_clips, int)
        or isinstance(max_clips, bool)
        or not 1 <= max_clips <= HARD_MAX_CLIPS
        or not isinstance(max_seconds, (int, float))
        or isinstance(max_seconds, bool)
        or not math.isfinite(max_seconds)
        or not 0 < max_seconds <= HARD_MAX_SECONDS
    ):
        raise ValueError("bounds require 1..563 clips and positive finite seconds <= 3600")


def normalize_audio(content: bytes) -> tuple[bytes, float]:
    """Validate PCM directly, or decode compressed WAVE into bounded mono PCM."""
    if content[:4] != b"RIFF" or content[8:12] != b"WAVE":
        raise ValueError("expected WAVE audio")
    try:
        with wave.open(io.BytesIO(content), "rb") as reader:
            seconds = reader.getnframes() / reader.getframerate()
            if (
                reader.getcomptype() != "NONE"
                or not 0 < seconds <= MAX_CLIP_SECONDS
                or reader.getnchannels() != 1
            ):
                raise ValueError("expected bounded nonempty mono PCM audio")
            expected = reader.getnframes() * reader.getsampwidth()
            if len(reader.readframes(reader.getnframes())) != expected:
                raise ValueError("truncated WAVE audio")
        return content, seconds
    except (EOFError, wave.Error):
        pass

    import av
    from av.error import FFmpegError

    pcm = bytearray()
    decoded_seconds = 0.0
    rate = 16000
    resampler = av.AudioResampler(format="s16", layout="mono", rate=rate)
    try:
        with av.open(io.BytesIO(content)) as container:
            if len(container.streams.audio) != 1:
                raise ValueError("expected one audio stream")
            for frame in container.decode(audio=0):
                if not 0 < frame.sample_rate <= 192000:
                    raise ValueError("unsupported audio sample rate")
                decoded_seconds += frame.samples / frame.sample_rate
                if decoded_seconds > MAX_CLIP_SECONDS:
                    raise ValueError("decoded audio exceeds per-clip duration bound")
                for converted in resampler.resample(frame):
                    pcm.extend(converted.to_ndarray().tobytes())
                if len(pcm) > 2 * rate * MAX_CLIP_SECONDS:
                    raise ValueError("decoded audio exceeds PCM frame bound")
            for converted in resampler.resample(None):
                pcm.extend(converted.to_ndarray().tobytes())
    except FFmpegError as error:
        raise ValueError("audio decoder rejected the file") from error
    samples = len(pcm) // 2
    if len(pcm) % 2 or not 0 < samples <= rate * MAX_CLIP_SECONDS:
        raise ValueError("invalid decoded PCM frame count")
    seconds = samples / rate
    if abs(seconds - decoded_seconds) > 1 / rate:
        raise ValueError("decoded duration and PCM frame count differ")
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(rate)
        writer.writeframes(pcm)
    return output.getvalue(), seconds


def prepare_requests(
    rows: list[dict], directory: Path, max_clips=MAX_CLIPS, max_seconds=MAX_SECONDS
) -> list[dict]:
    """Select a hash-ordered prefix without inspecting labels or model predictions."""
    validate_bounds(max_clips, max_seconds)
    directory.mkdir(parents=True, exist_ok=True)
    candidates = []
    parents = list(range(len(rows)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    seen = {}
    for index, row in enumerate(rows):
        audio = row["audio"]["bytes"]
        if not isinstance(audio, bytes):
            raise TypeError("the pinned dataset must embed audio bytes")
        audio_hash = hashlib.sha256(audio).hexdigest()
        request_id = hashlib.sha256(f"{index}:{audio_hash}".encode()).hexdigest()
        text = row.get("transcription") or ""
        for key in (("audio", audio_hash), ("text", normalize_text(text))):
            if key[0] == "text" and not key[1]:
                continue
            if key in seen:
                parents[root(index)] = root(seen[key])
            else:
                seen[key] = index
        try:
            staged_audio, seconds = normalize_audio(audio)
            status = "ok"
        except (EOFError, wave.Error, ValueError, ZeroDivisionError):
            staged_audio = audio
            seconds, status = 0.0, "decode_error"
        candidates.append(
            {
                "request_id": request_id,
                "audio": staged_audio,
                "seconds": seconds,
                "decode_status": status,
                "supplied_asr": text,
                "source_index": index,
            }
        )

    groups = {}
    for index, candidate in enumerate(candidates):
        groups.setdefault(root(index), []).append(candidate["request_id"])
    group_ids = {key: min(members) for key, members in groups.items()}
    selected, seconds = [], 0.0
    for candidate in sorted(candidates, key=lambda item: item["request_id"]):
        if len(selected) == max_clips or seconds + candidate["seconds"] > max_seconds:
            break  # Keep a prefix, rather than searching for easier or shorter later clips.
        index = candidate["source_index"]
        group = group_ids[root(index)]
        split = "train" if int(group[:8], 16) % 5 < 2 else "test"
        path = directory / f"{candidate['request_id']}.wav"
        path.write_bytes(candidate["audio"])
        label = rows[index]["intent_class"]
        if not isinstance(label, int) or not 0 <= label < len(INTENTS):
            raise ValueError("unexpected intent label in the pinned dataset")
        selected.append(
            {
                "request_id": candidate["request_id"],
                "duplicate_group": group,
                "split": split,
                "reporting_group": f"synthetic-{int(group[:8], 16) % 2}",
                "seconds": candidate["seconds"],
                "decode_status": candidate["decode_status"],
                "path": str(path),
                "supplied_asr": candidate["supplied_asr"],
                "gold_intent": INTENTS[label],
            }
        )
        seconds += candidate["seconds"]
    if not selected:
        raise ValueError("no request fits the selected duration bound")
    return selected


def load_requests(directory: Path, max_clips=MAX_CLIPS, max_seconds=MAX_SECONDS) -> list[dict]:
    validate_bounds(max_clips, max_seconds)
    import pyarrow.parquet as pq
    from huggingface_hub import hf_hub_download

    source = Path(
        hf_hub_download(
            DATASET_REPO, DATASET_FILE, repo_type="dataset", revision=DATASET_REVISION, token=False
        )
    )
    if digest_file(source) != DATASET_SHA256:
        raise ValueError("MINDS-14 dataset SHA-256 mismatch")
    rows = pq.read_table(source, columns=["audio", "transcription", "intent_class"]).to_pylist()
    return prepare_requests(rows, directory, max_clips, max_seconds)


def transcribe(batches):
    """Only opaque identifiers, staged audio paths and decode status enter this task."""
    import threading

    import pyarrow as pa
    from av.error import FFmpegError
    from faster_whisper import WhisperModel
    from huggingface_hub import hf_hub_download
    from tqdm import tqdm

    tqdm.set_lock(threading.RLock())
    model = None
    schema = pa.schema([(name, pa.string()) for name in ("request_id", "whisper_text", "whisper_status")])
    for batch in batches:
        for item in batch.to_pylist():
            text, status = "", item["decode_status"]
            if status == "ok":
                if model is None:
                    files = [
                        hf_hub_download(MODEL_REPO, name, revision=MODEL_REVISION, token=False)
                        for name in MODEL_FILES
                    ]
                    model = WhisperModel(
                        str(Path(files[0]).parent), device="cpu", compute_type="float32", cpu_threads=1
                    )
                try:
                    segments, _ = model.transcribe(
                        item["path"],
                        language="en",
                        beam_size=1,
                        temperature=0.0,
                        condition_on_previous_text=False,
                    )
                    text = " ".join(segment.text.strip() for segment in segments).strip()
                    status = "ok" if text else "empty_transcript"
                except (OSError, ValueError, RuntimeError, FFmpegError):
                    # A file failure remains a row. Model setup failures above stop the run.
                    status = "transcription_error"
            yield pa.RecordBatch.from_pylist(
                [{"request_id": item["request_id"], "whisper_text": text, "whisper_status": status}],
                schema=schema,
            )


def analyze(spark, requests, observations):
    """Join observed text to labels after inference, and keep source-specific denominators."""
    from pyspark.sql import functions as F

    for name, frame in (("request", requests), ("observation", observations)):
        if frame.filter(F.col("request_id").isNull()).limit(1).count():
            raise ValueError(f"null {name} ID")
        if frame.groupBy("request_id").count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"duplicate {name} ID")
    if (
        observations.select("request_id")
        .join(requests.select("request_id"), "request_id", "left_anti")
        .limit(1)
        .count()
    ):
        raise ValueError("unknown observation ID")
    joined = requests.join(observations, "request_id", "left")
    supplied = joined.select(
        "request_id",
        "split",
        "reporting_group",
        "gold_intent",
        F.lit("supplied_asr").alias("text_source"),
        F.col("supplied_asr").alias("text"),
        F.when(F.length(F.trim("supplied_asr")) > 0, "ok").otherwise("empty_transcript").alias("status"),
    )
    whisper = joined.select(
        "request_id",
        "split",
        "reporting_group",
        "gold_intent",
        F.lit("whisper").alias("text_source"),
        F.col("whisper_text").alias("text"),
        F.coalesce("whisper_status", F.lit("missing_output")).alias("status"),
    )
    classify = F.udf(classify_text, "string")
    predictions = (
        supplied.unionByName(whisper)
        .withColumn(
            "predicted_intent", F.when(F.col("status") == "ok", classify("text")).otherwise("unknown")
        )
        .withColumn("correct", (F.col("predicted_intent") == F.col("gold_intent")).cast("long"))
    )
    summary = (
        predictions.groupBy("text_source", "split")
        .agg(
            F.count("*").alias("selected_requests"),
            F.sum("correct").alias("correct_requests"),
            F.expr("sum(CASE WHEN predicted_intent = 'unknown' THEN 1 ELSE 0 END)").alias("unknown_requests"),
            F.expr("sum(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END)").alias("input_errors"),
            F.countDistinct("gold_intent").alias("observed_intents"),
        )
        .withColumn("accuracy", F.col("correct_requests") / F.col("selected_requests"))
        .withColumn(
            "coverage", (F.col("selected_requests") - F.col("unknown_requests")) / F.col("selected_requests")
        )
    )
    labels = spark.createDataFrame([(intent,) for intent in INTENTS], "intent string")
    per_class = (
        predictions.crossJoin(labels)
        .groupBy("text_source", "split", "intent")
        .agg(
            F.sum(
                F.when(
                    (F.col("gold_intent") == F.col("intent"))
                    & (F.col("predicted_intent") == F.col("intent")),
                    1,
                ).otherwise(0)
            ).alias("tp"),
            F.sum(F.when(F.col("gold_intent") == F.col("intent"), 1).otherwise(0)).alias("support"),
            F.sum(F.when(F.col("predicted_intent") == F.col("intent"), 1).otherwise(0)).alias("predicted"),
        )
        .withColumn(
            "f1",
            F.when(
                F.col("support") + F.col("predicted") > 0,
                2.0 * F.col("tp") / (F.col("support") + F.col("predicted")),
            ).otherwise(0.0),
        )
    )
    macro = per_class.groupBy("text_source", "split").agg(F.avg("f1").alias("macro_f1_all_14"))
    summary = summary.join(macro, ["text_source", "split"])
    topics = predictions.groupBy("text_source", "split", "reporting_group", "predicted_intent").agg(
        F.count("*").alias("requests")
    )
    confusion = predictions.groupBy("text_source", "split", "gold_intent", "predicted_intent").count()
    return {
        "requests": requests.drop("path").orderBy("request_id"),
        "predictions": predictions.orderBy("text_source", "request_id"),
        "summary": summary.orderBy("text_source", "split"),
        "topics": topics.orderBy("text_source", "split", "reporting_group", "predicted_intent"),
        "confusion": confusion.orderBy("text_source", "split", "gold_intent", "predicted_intent"),
    }


def run(spark, max_clips=MAX_CLIPS, max_seconds=MAX_SECONDS):
    requests = spark.createDataFrame(
        load_requests(Path(temp_dir("support-clips-")), max_clips, max_seconds), MANIFEST_SCHEMA
    )
    observed_path = temp_dir("support-transcripts-")
    requests.select("request_id", "path", "decode_status").repartition(1).mapInArrow(
        transcribe, OBSERVATION_SCHEMA
    ).write.mode("overwrite").parquet(observed_path)
    observations = spark.read.schema(OBSERVATION_SCHEMA).parquet(observed_path)
    from pyspark.sql import functions as F

    if (
        not observations.filter((F.col("whisper_status") == "ok") & (F.length("whisper_text") > 0))
        .limit(1)
        .count()
    ):
        raise RuntimeError("no selected request produced a successful Whisper transcript")
    return analyze(spark, requests, observations)


def parse_args(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-clips", type=int, default=MAX_CLIPS, help="selected request ceiling (1..563)")
    parser.add_argument(
        "--max-seconds", type=float, default=MAX_SECONDS, help="valid audio seconds ceiling (0..3600]"
    )
    parser.add_argument("--json", action="store_true", help="print all result tables as JSON")
    args = parser.parse_args(argv)
    try:
        validate_bounds(args.max_clips, args.max_seconds)
    except ValueError as error:
        parser.error(str(error))
    return args


def main(argv=None):
    import json

    args = parse_args(argv)
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    try:
        results = run(spark, args.max_clips, args.max_seconds)
        if args.json:
            print(
                json.dumps(
                    {name: [row.asDict() for row in result.collect()] for name, result in results.items()},
                    sort_keys=True,
                )
            )
        else:
            results["summary"].show(truncate=False)
            results["topics"].show(truncate=False)
    finally:
        spark.stop()
        server.stop()


if __name__ == "__main__":
    main()
