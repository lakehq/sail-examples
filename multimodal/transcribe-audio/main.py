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
#   "onnxruntime==1.31.1",
#   "tokenizers==0.22.1",
#   "huggingface-hub==0.36.0",
#   "tqdm==4.70.1",
# ]
# ///
"""Transcribe a folder of audio files with Whisper inside the engine, and score the transcripts.

The pattern: a table of audio paths goes through `mapInArrow`, which loads the speech model once
per partition and returns one transcript per file. SQL then compares each transcript with a
reference transcript and computes the character error rate.
"""

import atexit
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_REPO = "Systran/faster-whisper-tiny.en"  # OpenAI Whisper tiny.en, converted for CTranslate2
MODEL_REVISION = "0d3d19a32d3338f10357c0889762bd8d64bbdeba"  # pinned: every run uses the same weights
MODEL_FILES = ["model.bin", "config.json", "tokenizer.json", "vocabulary.txt"]
# 73 short read-speech clips from the LibriSpeech corpus (CC BY 4.0), with reference transcripts.
CLIPS_REPO = "hf-internal-testing/librispeech_asr_dummy"
CLIPS_REVISION = "5be91486e11a2d616f4ec5db8d3fd248585ac07a"
CLIPS_FILE = "clean/validation-00000-of-00001.parquet"

ARROW_SCHEMA = pa.schema([("id", pa.string()), ("seconds", pa.float64()), ("transcript", pa.string())])
SCHEMA = "id string, seconds double, transcript string"


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def download_clips(directory: Path) -> list[tuple[str, str, str]]:
    """Save the sample clips as FLAC files and return (id, path, reference transcript) per clip."""
    import pyarrow.parquet as pq
    from huggingface_hub import hf_hub_download

    table = pq.read_table(
        hf_hub_download(CLIPS_REPO, CLIPS_FILE, repo_type="dataset", revision=CLIPS_REVISION)
    )
    clips = []
    for row in table.select(["id", "audio", "text"]).to_pylist():
        path = directory / f"{row['id']}.flac"
        path.write_bytes(row["audio"]["bytes"])
        clips.append((row["id"], str(path), row["text"]))
    return clips


def transcribe(batches):
    """mapInArrow function: everything before the loop runs once per partition."""
    import threading

    from faster_whisper import WhisperModel
    from huggingface_hub import hf_hub_download
    from tqdm import tqdm

    # faster-whisper creates a tqdm progress bar even when progress is off, and tqdm's default lock
    # starts a multiprocessing helper process. In local mode this UDF runs inside the Sail server
    # process, so swap in a thread lock (a setting for the whole process) and start no process there.
    tqdm.set_lock(threading.RLock())

    # Download where the task runs (cached per machine), so this also works on cluster workers.
    files = [hf_hub_download(MODEL_REPO, name, revision=MODEL_REVISION) for name in MODEL_FILES]
    model_dir = str(Path(files[0]).parent)
    # One thread per model: the engine runs partitions in parallel already.
    model = WhisperModel(model_dir, device="cpu", compute_type="float32", cpu_threads=1)

    for batch in batches:
        rows = {name: [] for name in ARROW_SCHEMA.names}
        for clip_id, path in zip(batch.column("id").to_pylist(), batch.column("path").to_pylist()):
            # One temperature of 0 uses greedy decoding without sampled retries.
            segments, info = model.transcribe(
                path, language="en", beam_size=1, temperature=0.0, condition_on_previous_text=False
            )
            rows["id"].append(clip_id)
            rows["seconds"].append(round(info.duration, 3))
            rows["transcript"].append(" ".join(segment.text.strip() for segment in segments))
        yield pa.RecordBatch.from_pydict(rows, schema=ARROW_SCHEMA)


def normalized(column: str):
    """Lowercase, letters, digits and apostrophes only, single spaces: compare words, not punctuation."""
    text = F.regexp_replace(F.lower(F.col(column)), r"[^a-z0-9' ]", " ")
    return F.trim(F.regexp_replace(text, r" +", " "))


def run(spark: SparkSession) -> dict[str, DataFrame]:
    # A table of paths, one row per audio file, with the reference transcript to score against.
    clips = spark.createDataFrame(
        download_clips(Path(temp_dir("clips-"))), "id string, path string, reference string"
    )

    # Transcribe, then save once: the queries below must not run the model again.
    transcripts_path = temp_dir("transcripts-")
    clips.select("id", "path").repartition(4).mapInArrow(transcribe, SCHEMA).write.mode("overwrite").parquet(
        transcripts_path
    )
    transcripts = spark.read.parquet(transcripts_path).join(clips.select("id", "reference"), "id")

    # Character error rate: edits needed to turn the transcript into the reference, per reference character.
    scored = transcripts.select(
        "id",
        "seconds",
        "transcript",
        F.levenshtein(normalized("transcript"), normalized("reference")).alias("edits"),
        F.length(normalized("reference")).alias("reference_chars"),
    )
    summary = scored.agg(
        F.count("*").alias("clips"),
        F.round(F.sum("seconds") / 60, 2).alias("minutes"),
        (F.sum("edits") / F.sum("reference_chars")).alias("character_error_rate"),
    )
    return {"transcripts": scored.orderBy("id"), "summary": summary}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["transcripts"].select("id", "seconds", "transcript").show(5, truncate=80)
    results["summary"].show()
    spark.stop()
    server.stop()
