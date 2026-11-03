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
# ]
# ///
"""Embed text with an ONNX model inside the engine, then search it with SQL vector functions.

The model loads once per partition inside `mapInArrow`. Embeddings are stored as
ARRAY<FLOAT>, saved to Parquet, and ranked with `vector_cosine_similarity`.
"""

import atexit
import shutil
import tempfile

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"  # pinned: every run uses the same weights
MAX_TOKENS = 128
INFERENCE_BATCH_SIZE = 16  # bound model tensors independently of Arrow input batch size

DOCUMENTS = [
    "Parquet stores data by column, so queries read only the columns they need.",
    "Delta Lake adds ACID transactions and time travel on top of Parquet files.",
    "Iceberg tables track snapshots so readers see a consistent view of the data.",
    "Partitioning a table by date lets queries skip files outside the date range.",
    "Compaction merges many small files into fewer large ones to speed up reads.",
    "Object storage such as S3 holds the files of a lakehouse table.",
    "A table format records schema changes so old and new files stay readable.",
    "Column statistics in file footers help the engine prune row groups.",
    "A streaming job reads events continuously and updates results as data arrives.",
    "Watermarks tell a streaming aggregation how long to wait for late events.",
    "Kafka topics deliver ordered records that consumers process in real time.",
    "Checkpointing lets a stream restart from where it stopped after a failure.",
    "Windowed counts group events into fixed or sliding time intervals.",
    "Exactly-once delivery means each event affects the output a single time.",
    "Backpressure slows producers when consumers cannot keep up with the stream.",
    "A sink writes the continuously updated results of a stream to storage.",
    "An embedding turns a sentence into a vector that captures its meaning.",
    "Cosine similarity compares two vectors by the angle between them.",
    "A language model can label support tickets or summarize documents.",
    "Retrieval finds the documents most similar to a question before answering it.",
    "Fine-tuning adapts a pretrained model to a specific task with new examples.",
    "Batch inference runs a model over a whole table instead of one request at a time.",
    "Tokenizers split text into the pieces a model reads as input.",
    "Evaluation measures how often a model's answers are correct on labeled data.",
]
QUERIES = [
    "Which table formats add transactions and snapshots on top of Parquet files?",
    "What happens when events arrive late in a real-time pipeline?",
    "How can I find text with a similar meaning?",
]


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def embed(batches):
    """mapInArrow function: everything before the loop runs once per partition."""
    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    # Download where the task runs (cached per machine), so this also works on cluster workers.
    model_path = hf_hub_download(MODEL_REPO, "onnx/model.onnx", revision=MODEL_REVISION)
    tokenizer_path = hf_hub_download(MODEL_REPO, "tokenizer.json", revision=MODEL_REVISION)
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1  # fixed reduction order, so vectors do not vary between runs
    session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
    tokenizer = Tokenizer.from_file(tokenizer_path)
    tokenizer.enable_truncation(MAX_TOKENS)
    # Fixed-length padding makes a row's vector independent of the other rows in its batch, and
    # engines split rows into batches differently.
    tokenizer.enable_padding(length=MAX_TOKENS, pad_id=0, pad_token="[PAD]")

    for input_batch in batches:
        for start in range(0, input_batch.num_rows, INFERENCE_BATCH_SIZE):
            batch = input_batch.slice(start, INFERENCE_BATCH_SIZE)
            encodings = tokenizer.encode_batch(batch.column("text").to_pylist())
            ids = np.array([e.ids for e in encodings], dtype=np.int64)
            mask = np.array([e.attention_mask for e in encodings], dtype=np.int64)
            feeds = {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)}
            (hidden,) = session.run(["last_hidden_state"], feeds)
            weights = mask.astype(np.float32)[:, :, None]
            pooled = (hidden * weights).sum(axis=1) / np.clip(weights.sum(axis=1), 1e-9, None)
            vectors = pooled / np.clip(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-12, None)
            yield pa.RecordBatch.from_pydict(
                {
                    "id": batch.column("id"),
                    "embedding": pa.array(vectors.astype(np.float32).tolist(), pa.list_(pa.float32())),
                }
            )


def run(spark: SparkSession) -> dict[str, DataFrame]:
    schema = "id long, embedding array<float>"
    documents = spark.createDataFrame(list(enumerate(DOCUMENTS)), "id long, text string")
    queries = spark.createDataFrame(list(enumerate(QUERIES)), "id long, text string")

    # Embed in two partitions (the model loads once in each), then save the vectors once.
    vectors_path = temp_dir("vectors-")
    documents.repartition(2).mapInArrow(embed, schema).write.mode("overwrite").parquet(vectors_path)
    document_vectors = spark.read.parquet(vectors_path).withColumnRenamed("embedding", "doc_vec")
    query_vectors = queries.mapInArrow(embed, schema).withColumnRenamed("embedding", "query_vec")

    scored = (
        query_vectors.withColumnRenamed("id", "query_id")
        .crossJoin(document_vectors.withColumnRenamed("id", "doc_id"))
        .select(
            "query_id",
            "doc_id",
            F.expr("vector_cosine_similarity(query_vec, doc_vec)").cast("double").alias("score"),
        )
    )
    rank = F.row_number().over(Window.partitionBy("query_id").orderBy(F.desc("score"), "doc_id"))
    top3 = scored.withColumn("rank", rank).filter("rank <= 3")
    texts = documents.withColumnRenamed("id", "doc_id").withColumnRenamed("text", "document")
    results = top3.join(texts, "doc_id")
    return {
        "top3": results.select("query_id", "rank", "doc_id", "score", "document").orderBy("query_id", "rank"),
        "dimensions": spark.read.parquet(vectors_path).select(F.size("embedding").alias("dim")).distinct(),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    top3 = run(spark)["top3"].collect()
    for query_id, question in enumerate(QUERIES):
        print(question)
        for row in (r for r in top3 if r["query_id"] == query_id):
            print(f"  {row['rank']}. {row['score']:.3f}  {row['document']}")
    spark.stop()
    server.stop()
