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
#   "huggingface-hub==1.5.0",
#   "tqdm==4.70.1",
#   "transformers==5.10.1",
#   "regex==2026.9.29",
# ]
# ///
"""The full test-set evaluation: transcribe all of LibriSpeech test-clean and compute the word error rate.

Uses the same transcription function as main.py on the 2,620 utterances of the standard
test-clean split, normalizes text the way the Whisper paper did, and computes the corpus word
error rate in SQL, for comparison with the paper's published figure for the same model.
"""

import json
import sys
from pathlib import Path

import main
from main import temp_dir, transcribe
from pyspark import cloudpickle
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

# Send main.py's code with the UDF, not a reference to the module, so workers need no copy of main.py.
cloudpickle.register_pickle_by_value(main)

TEST_REPO = "openslr/librispeech_asr"  # LibriSpeech (CC BY 4.0)
TEST_REVISION = "71cacbfb7e2354c4226d01e70d77d5fca3d04ba1"
TEST_FILE = "clean/test/0000.parquet"  # 350 MB
NORMALIZER = ("openai/whisper-tiny.en", "normalizer.json", "87c7102498dcde7456f24cfd30239ca606ed9063")
# Whisper paper (Radford et al., 2022), Appendix D.1.1, Table 8: tiny.en on LibriSpeech test-clean,
# greedy decoding, word error rate in percent.
PUBLISHED_WER = 5.6


def write_clips(directory: Path) -> list[tuple[str, str, str]]:
    """Save every test-clean utterance as a FLAC file; return (id, path, reference transcript)."""
    import pyarrow.parquet as pq
    from huggingface_hub import hf_hub_download

    source = pq.ParquetFile(
        hf_hub_download(TEST_REPO, TEST_FILE, repo_type="dataset", revision=TEST_REVISION)
    )
    clips = []
    for batch in source.iter_batches(batch_size=200, columns=["id", "audio", "text"]):  # bounded memory
        for row in batch.to_pylist():
            path = directory / f"{row['id']}.flac"
            path.write_bytes(row["audio"]["bytes"])
            clips.append((row["id"], str(path), row["text"]))
    return clips


def run(spark: SparkSession) -> DataFrame:
    clips = spark.createDataFrame(
        write_clips(Path(temp_dir("test-clean-"))), "id string, path string, reference string"
    )
    transcripts_path = temp_dir("test-clean-transcripts-")
    clips.select("id", "path").repartition(8).mapInArrow(
        transcribe, "id string, seconds double, transcript string"
    ).write.mode("overwrite").parquet(transcripts_path)

    @pandas_udf("string")
    def normalized(texts):
        """Whisper's English text normalizer, as used for the paper's word error rates."""
        from huggingface_hub import hf_hub_download
        from transformers.models.whisper.english_normalizer import EnglishTextNormalizer

        repo, name, revision = NORMALIZER
        normalize = EnglishTextNormalizer(
            json.loads(Path(hf_hub_download(repo, name, revision=revision)).read_text())
        )
        return texts.map(normalize)

    @pandas_udf("long")
    def word_errors(hypotheses, references):
        """Word-level edit distance: substitutions, deletions and insertions."""
        import pandas as pd

        def distance(hypothesis: str, reference: str) -> int:
            hyp, ref = hypothesis.split(), reference.split()
            previous = list(range(len(hyp) + 1))
            for i, word in enumerate(ref, start=1):
                current = [i]
                for j, other in enumerate(hyp, start=1):
                    current.append(
                        min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (word != other))
                    )
                previous = current
            return previous[-1]

        return pd.Series([distance(h, r) for h, r in zip(hypotheses, references)])

    scored = (
        spark.read.parquet(transcripts_path)
        .join(clips.select("id", "reference"), "id")
        .select(
            "id",
            "seconds",
            normalized("transcript").alias("hypothesis"),
            normalized("reference").alias("truth"),
        )
        .select(
            "id",
            "seconds",
            word_errors("hypothesis", "truth").alias("errors"),
            F.size(F.split(F.trim("truth"), " ")).alias("words"),
        )
    )
    return scored.agg(
        F.count("*").alias("utterances"),
        F.round(F.sum("seconds") / 3600, 2).alias("hours"),
        F.sum("words").alias("reference_words"),
        F.sum("errors").alias("word_errors"),
        F.round(F.sum("errors") / F.sum("words") * 100, 2).alias("wer_percent"),
        F.lit(PUBLISHED_WER).alias("published_wer_percent"),
    )


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    result = run(spark)
    if "--json" in sys.argv:  # one machine-readable line, for automated runs
        import pysail

        print("LARGE " + json.dumps({**result.first().asDict(), "pysail": pysail.__version__}))
    else:
        result.show()
    spark.stop()
    server.stop()
