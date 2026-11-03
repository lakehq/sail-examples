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
#   "fpdf2==2.8.9",
#   "pypdf==6.21.1",
# ]
# ///
"""Turn a folder of PDFs into searchable chunks: extract, chunk, embed, search.

Each PDF is read inside `mapInArrow` from a table of paths. Pages become overlapping text
chunks, the chunks are embedded with an ONNX model loaded once per partition, and a question
is answered with the most similar chunks, cited by document and page.
"""

import atexit
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"  # pinned: every run uses the same weights
CHUNK_WORDS = 60
OVERLAP_WORDS = 15
CHUNKS_PER_BATCH = 32
INFERENCE_BATCH_SIZE = 16  # bound model tensors independently of Arrow input batch size
QUESTION = "What happens if a customer is charged twice?"

DOCUMENTS = {
    "billing-policy": [
        (
            "Invoices are issued on the first day of each month. Payment is due within thirty days. "
            "Customers can pay by card or bank transfer, and receipts are sent by email."
        ),
        (
            "If a customer is charged twice, the duplicate charge is refunded within five business days. "
            "Support confirms the refund by email and the credit appears on the next statement."
        ),
        (
            "Annual plans are billed upfront. A customer who cancels in the first fourteen days receives "
            "a full refund; later cancellations stop renewal but are not prorated."
        ),
    ],
    "incident-report": [
        (
            "On the night of the incident, report exports failed with error 500 for forty minutes. "
            "The cause was an expired certificate on the storage gateway."
        ),
        (
            "Engineers rotated the certificate, replayed the failed exports and added an alert that "
            "fires seven days before any certificate expires."
        ),
    ],
    "onboarding-guide": [
        (
            "To invite a teammate, open Settings, choose Members and enter their email address. "
            "Invitations expire after seven days."
        ),
        (
            "New workspaces include a sample project. Delete it from the project menu once your own "
            "data is loaded."
        ),
        (
            "Single sign-on can be enabled by an administrator under Security. Members then sign in "
            "with the company identity provider. Before you turn it on, check that every member has an "
            "account in the identity provider with the same email address, because members without one "
            "can no longer sign in. Administrators keep a password login as a fallback in case the "
            "identity provider is unavailable. Group mappings decide which projects each member can see, "
            "and they are applied each time a member signs in. Removing a member from the identity "
            "provider removes their access at the next sign in, and an administrator can also revoke "
            "active sessions from the Members page right away."
        ),
    ],
    "data-retention": [
        (
            "Raw event data is kept for thirteen months. Aggregated reports are kept for five years. "
            "Deleted projects are purged after thirty days."
        ),
        "Customers can export all of their data at any time as Parquet files from the Export page.",
    ],
}


CHUNK_SCHEMA = pa.schema(
    [("doc", pa.string()), ("page", pa.int32()), ("chunk", pa.int32()), ("text", pa.string())]
)


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def make_pdfs(directory: Path) -> None:
    from fpdf import FPDF

    for name, pages in DOCUMENTS.items():
        pdf = FPDF()
        pdf.set_font("Helvetica", size=11)
        for text in pages:
            pdf.add_page()
            pdf.multi_cell(0, 6, text)
        pdf.output(str(directory / f"{name}.pdf"))


def extract_chunks(batches):
    """One input row per PDF; at most 32 text chunks per output batch."""
    from pypdf import PdfReader

    step = CHUNK_WORDS - OVERLAP_WORDS
    for batch in batches:
        for path in batch.column("path").to_pylist():
            rows = {name: [] for name in CHUNK_SCHEMA.names}
            for page_number, page in enumerate(PdfReader(path).pages, start=1):
                words = page.extract_text().split()
                if not words:  # blank or image-only page: nothing to index
                    continue
                for index, start in enumerate(range(0, max(len(words) - OVERLAP_WORDS, 1), step)):
                    rows["doc"].append(Path(path).stem)
                    rows["page"].append(page_number)
                    rows["chunk"].append(index)
                    rows["text"].append(" ".join(words[start : start + CHUNK_WORDS]))
                    if len(rows["doc"]) == CHUNKS_PER_BATCH:
                        yield pa.RecordBatch.from_pydict(rows, schema=CHUNK_SCHEMA)
                        rows = {name: [] for name in CHUNK_SCHEMA.names}
            if rows["doc"]:
                yield pa.RecordBatch.from_pydict(rows, schema=CHUNK_SCHEMA)


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
    tokenizer.enable_truncation(128)
    # Fixed-length padding makes a row's vector independent of the other rows in its batch.
    tokenizer.enable_padding(length=128, pad_id=0, pad_token="[PAD]")
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
                    "chunk_id": batch.column("chunk_id"),
                    "embedding": pa.array(vectors.astype(np.float32).tolist(), pa.list_(pa.float32())),
                }
            )


def run(spark: SparkSession) -> dict[str, DataFrame]:
    folder = Path(temp_dir("pdfs-"))
    make_pdfs(folder)

    # 1. Extract and chunk: a table of paths, one PDF per row, read inside mapInArrow.
    paths = spark.createDataFrame([(str(p),) for p in sorted(folder.glob("*.pdf"))], "path string")
    chunks_path = temp_dir("chunks-")
    chunked = paths.repartition(2).mapInArrow(extract_chunks, "doc string, page int, chunk int, text string")
    chunked.write.mode("overwrite").parquet(chunks_path)
    chunks = spark.read.parquet(chunks_path).withColumn(
        "chunk_id", F.concat_ws(":", "doc", F.col("page").cast("string"), F.col("chunk").cast("string"))
    )

    # 2. Embed once per chunk and save the vectors.
    vectors_path = temp_dir("vectors-")
    vectors_out = (
        chunks.select("chunk_id", "text")
        .repartition(2)
        .mapInArrow(embed, "chunk_id string, embedding array<float>")
    )
    vectors_out.write.mode("overwrite").parquet(vectors_path)
    vectors = spark.read.parquet(vectors_path)

    # 3. Search: embed the question the same way and rank chunks by cosine similarity.
    question = spark.createDataFrame([("question", QUESTION)], "chunk_id string, text string")
    question_vector = question.mapInArrow(embed, "chunk_id string, embedding array<float>").select(
        F.col("embedding").alias("query_vec")
    )
    scored = vectors.crossJoin(question_vector).select(
        "chunk_id", F.expr("vector_cosine_similarity(embedding, query_vec)").cast("double").alias("score")
    )
    top3 = scored.orderBy(F.desc("score"), "chunk_id").limit(3).join(chunks, "chunk_id")

    per_doc = chunks.groupBy("doc").agg(F.countDistinct("page").alias("pages"), F.count("*").alias("chunks"))
    return {
        "top3": top3.select("doc", "page", "chunk", "score", "text").orderBy(
            F.desc("score"), "doc", "page", "chunk"
        ),
        "per_document": per_doc.orderBy("doc"),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["per_document"].show()
    print(QUESTION)
    for rank, row in enumerate(results["top3"].collect(), start=1):
        print(f"  {rank}. {row['score']:.3f}  {row['doc']} page {row['page']}: {row['text'][:80]}")
    spark.stop()
    server.stop()
