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
#   "huggingface-hub==1.31.0",
#   "datasets==5.1.0",
# ]
# ///
"""Build a versioned search evaluation snapshot and consume its exported Parquet records."""

import argparse
import atexit
import hashlib
import json
import math
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
MAX_TOKENS = 128
INFERENCE_BATCH_SIZE = 16
K = 3
PROCESSING_CONFIG = {
    "k": K,
    "embedding_max_tokens": MAX_TOKENS,
    "embedding_pooling": "attention_mask_mean",
    "embedding_normalization": "l2",
    "lexical_tokens": "distinct_lowercase_ascii_alphanumeric",
    "tie_break": "document_id_ascending",
}
SCHEMAS = {
    "documents": "row_id string, document_id string, revision long, text string",
    "queries": "row_id string, query_id string, revision long, text string, query_group string",
    "judgments": "row_id string, query_id string, query_revision long, document_id string, document_revision long, judgment_revision long, grade long, label_origin string",
}
PAIR_COLUMNS = [
    "snapshot_id",
    "method",
    "split",
    "query_group",
    "query_id",
    "query_revision",
    "query",
    "document_id",
    "document_revision",
    "document",
    "judgment_revision",
    "grade",
    "label_origin",
    "rank",
    "score",
]


def temp_dir(prefix):
    path = Path(tempfile.mkdtemp(prefix=prefix))
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def normalized(column):
    # Explicit ASCII whitespace has the same meaning on both engines.
    return F.lower(F.trim(F.regexp_replace(column, "[ \\t\\r\\n]+", " ")))


def quarantine(frame, kind, reason):
    return frame.select(
        F.lit(kind).alias("record_type"),
        "row_id",
        reason.alias("reason"),
        F.col("_source_payload").alias("payload"),
    )


def latest_records(frame, kind, keys, revision, payload, invalid, barrier=None):
    """Keep one unambiguous latest record; retain every other source row with a reason."""
    columns = frame.columns
    tagged = frame.withColumn("_invalid", invalid)
    rejected = quarantine(
        frame.join(tagged.select("row_id", "_invalid"), "row_id").where("_invalid IS NOT NULL"),
        kind,
        F.col("_invalid"),
    )
    valid = tagged.where("_invalid IS NULL").drop("_invalid")
    # Invalid payloads at the newest identifiable version block older values.
    if barrier is None:
        barrier = F.col(revision).isNotNull() & (F.col(revision) > 0)
        for key in keys:
            barrier = barrier & F.col(key).isNotNull() & (F.trim(F.col(key)) != "")
    identifiable = tagged.where(barrier)
    newest = identifiable.groupBy(*keys).agg(F.max(revision).alias("_newest"))
    blocked = (
        identifiable.join(newest, keys)
        .where((F.col(revision) == F.col("_newest")) & F.col("_invalid").isNotNull())
        .select(*keys)
        .distinct()
        .withColumn("_blocked", F.lit(True))
    )
    joined = valid.join(newest, keys)
    rejected = rejected.unionByName(
        quarantine(
            joined.where(F.col(revision) < F.col("_newest")).select(*columns),
            kind,
            F.lit("superseded_revision"),
        )
    )
    current = joined.where(F.col(revision) == F.col("_newest")).drop("_newest").join(blocked, keys, "left")
    rejected = rejected.unionByName(
        quarantine(current.where("_blocked = true").select(*columns), kind, F.lit("invalid_latest_revision"))
    )
    current = current.where(F.col("_blocked").isNull()).drop("_blocked")
    variants = (
        current.select(*keys, *payload)
        .distinct()
        .groupBy(*keys)
        .count()
        .withColumnRenamed("count", "_variants")
    )
    current = current.join(variants, keys)
    rejected = rejected.unionByName(
        quarantine(
            current.where("_variants > 1").select(*columns), kind, F.lit("conflicting_latest_revision")
        )
    )
    unambiguous = current.where("_variants = 1").drop("_variants")
    ranked = unambiguous.withColumn("_copy", F.row_number().over(Window.partitionBy(*keys).orderBy("row_id")))
    rejected = rejected.unionByName(
        quarantine(ranked.where("_copy > 1").select(*columns), kind, F.lit("exact_duplicate"))
    )
    return ranked.where("_copy = 1").select(*columns), rejected


def deduplicate_text(frame, kind, identity, group=None):
    frame = frame.withColumn("content_group", F.sha2(normalized(F.col("text")), 256))
    if group:
        counts = frame.groupBy("content_group").agg(F.countDistinct(group).alias("_groups"))
        frame = frame.join(counts, "content_group")
        crossed = frame.where("_groups > 1").drop("_groups")
        rejected = quarantine(crossed, kind, F.lit("duplicate_query_across_groups"))
        frame = frame.where("_groups = 1").drop("_groups")
    else:
        rejected = quarantine(frame.where(F.lit(False)), kind, F.lit("duplicate_content"))
    ranked = frame.withColumn(
        "_copy", F.row_number().over(Window.partitionBy("content_group").orderBy(identity))
    )
    rejected = rejected.unionByName(
        quarantine(ranked.where("_copy > 1").drop("_copy"), kind, F.lit("duplicate_content"))
    )
    return ranked.where("_copy = 1").drop("_copy"), rejected


def curate(spark, fixture):
    """Version matching precedes labels; missing or quarantined labels stay missing."""
    for name in SCHEMAS:
        records = fixture[name]
        if any(not isinstance(row.get("row_id"), str) or not row["row_id"].strip() for row in records):
            raise ValueError(f"row_id must be a nonempty string in {name}")
        if len({row["row_id"] for row in records}) != len(records):
            raise ValueError(f"duplicate input row_id in {name}")
        integer_fields = (
            ("query_revision", "document_revision", "judgment_revision", "grade")
            if name == "judgments"
            else ("revision",)
        )
        for row in records:
            for field in integer_fields:
                if row.get(field) is not None and type(row[field]) is not int:
                    raise ValueError(f"{name}.{field} must be an integer or null")
    # Freeze original values before joins, including explicit nulls in rejected rows.
    frames = {
        name: spark.createDataFrame(
            [
                {**row, "_source_payload": json.dumps(row, sort_keys=True, separators=(",", ":"))}
                for row in fixture[name]
            ],
            schema + ", _source_payload string",
        )
        for name, schema in SCHEMAS.items()
    }
    documents, rejected = latest_records(
        frames["documents"],
        "document",
        ["document_id"],
        "revision",
        ["text"],
        F.when(F.col("document_id").isNull() | (F.trim("document_id") == ""), "invalid_id")
        .when(F.col("revision").isNull() | (F.col("revision") <= 0), "invalid_revision")
        .when(F.col("text").isNull() | (normalized(F.col("text")) == ""), "empty_text"),
    )
    documents, duplicate_documents = deduplicate_text(documents, "document", "document_id")
    queries, rejected_queries = latest_records(
        frames["queries"],
        "query",
        ["query_id"],
        "revision",
        ["text", "query_group"],
        F.when(F.col("query_id").isNull() | (F.trim("query_id") == ""), "invalid_id")
        .when(F.col("revision").isNull() | (F.col("revision") <= 0), "invalid_revision")
        .when(F.col("text").isNull() | (normalized(F.col("text")) == ""), "empty_text")
        .when(
            F.col("query_group").isNull() | ~F.col("query_group").isin(*fixture["split_groups"]),
            "unknown_query_group",
        ),
    )
    queries, duplicate_queries = deduplicate_text(queries, "query", "query_id", "query_group")
    groups = spark.createDataFrame(
        sorted(fixture["split_groups"].items()), "query_group string, split string"
    )
    if set(fixture["split_groups"].values()) != {"train", "validation", "test"}:
        raise ValueError("fixture needs fixed train, validation and test query groups")
    queries = queries.join(groups, "query_group")
    if not documents.take(1) or not queries.take(1):
        raise ValueError("need at least one valid document and query")
    joined = (
        frames["judgments"]
        .join(queries.select("query_id", F.col("revision").alias("_query_revision")), "query_id", "left")
        .join(
            documents.select("document_id", F.col("revision").alias("_document_revision")),
            "document_id",
            "left",
        )
    )
    invalid = (
        F.when(F.col("_query_revision").isNull(), "missing_query")
        .when(F.col("_document_revision").isNull(), "missing_document")
        .when(
            F.col("query_revision").isNull()
            | F.col("document_revision").isNull()
            | (F.col("query_revision") != F.col("_query_revision"))
            | (F.col("document_revision") != F.col("_document_revision")),
            "revision_mismatch",
        )
        .when(F.col("judgment_revision").isNull() | (F.col("judgment_revision") <= 0), "invalid_revision")
        .when(F.col("grade").isNull() | ~F.col("grade").isin(0, 1, 2), "invalid_grade")
        .when(F.col("label_origin").isNull() | (F.trim("label_origin") == ""), "missing_label_origin")
    )
    judgments, rejected_judgments = latest_records(
        joined,
        "judgment",
        ["query_id", "document_id"],
        "judgment_revision",
        ["grade", "label_origin"],
        invalid,
        barrier=(F.col("query_revision") == F.col("_query_revision"))
        & (F.col("document_revision") == F.col("_document_revision"))
        & (F.col("judgment_revision") > 0),
    )
    judgments = judgments.select(*frames["judgments"].columns)
    for other in [duplicate_documents, rejected_queries, duplicate_queries, rejected_judgments]:
        rejected = rejected.unionByName(other)
    return {
        "documents": documents.drop("_source_payload"),
        "queries": queries.drop("_source_payload"),
        "judgments": judgments.drop("_source_payload"),
        "quarantine": rejected,
    }


def embed(batches):
    """One model per partition, with bounded inference and explicit Arrow output types."""
    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    model_path = hf_hub_download(MODEL_REPO, "onnx/model.onnx", revision=MODEL_REVISION, token=False)
    tokenizer_path = hf_hub_download(MODEL_REPO, "tokenizer.json", revision=MODEL_REVISION, token=False)
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1
    session = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
    tokenizer = Tokenizer.from_file(tokenizer_path)
    tokenizer.enable_truncation(MAX_TOKENS)
    tokenizer.enable_padding(length=MAX_TOKENS, pad_id=0, pad_token="[PAD]")
    schema = pa.schema([("kind", pa.string()), ("id", pa.string()), ("embedding", pa.list_(pa.float32()))])
    for batch in batches:
        for start in range(0, batch.num_rows, INFERENCE_BATCH_SIZE):
            part = batch.slice(start, INFERENCE_BATCH_SIZE)
            encodings = tokenizer.encode_batch(part.column("text").to_pylist())
            ids = np.asarray([e.ids for e in encodings], dtype=np.int64)
            mask = np.asarray([e.attention_mask for e in encodings], dtype=np.int64)
            (hidden,) = session.run(
                ["last_hidden_state"],
                {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)},
            )
            weights = mask.astype(np.float32)[:, :, None]
            pooled = (hidden * weights).sum(axis=1) / np.clip(weights.sum(axis=1), 1e-9, None)
            vectors = pooled / np.clip(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-12, None)
            if not np.isfinite(vectors).all():
                raise ValueError("nonfinite model vectors")
            yield pa.RecordBatch.from_pydict(
                {
                    "kind": part.column("kind"),
                    "id": part.column("id"),
                    "embedding": vectors.astype(np.float32).tolist(),
                },
                schema=schema,
            )


def lexical_tokens(column):
    tokens = F.array_distinct(F.split(F.trim(F.regexp_replace(F.lower(column), "[^a-z0-9]+", " ")), " "))
    return F.filter(tokens, lambda token: token != "")


def rank_pairs(spark, tables, destination):
    documents, queries = tables["documents"], tables["queries"]
    texts = documents.select(
        F.lit("document").alias("kind"), F.col("document_id").alias("id"), "text"
    ).unionByName(queries.select(F.lit("query").alias("kind"), F.col("query_id").alias("id"), "text"))
    texts.coalesce(1).mapInArrow(embed, "kind string, id string, embedding array<float>").write.parquet(
        str(destination)
    )
    vectors = spark.read.parquet(str(destination))
    dv = vectors.where("kind = 'document'").select(
        F.col("id").alias("document_id"), F.col("embedding").alias("doc_vec")
    )
    qv = vectors.where("kind = 'query'").select(
        F.col("id").alias("query_id"), F.col("embedding").alias("query_vec")
    )
    paired = (
        queries.select(
            "query_id",
            F.col("revision").alias("query_revision"),
            F.col("text").alias("query"),
            "query_group",
            "split",
        )
        .crossJoin(
            documents.select(
                "document_id", F.col("revision").alias("document_revision"), F.col("text").alias("document")
            )
        )
        .join(
            tables["judgments"].select(
                "query_id", "document_id", "judgment_revision", "grade", "label_origin"
            ),
            ["query_id", "document_id"],
            "left",
        )
    )
    embedded = (
        paired.join(qv, "query_id")
        .join(dv, "document_id")
        .withColumn("score", F.expr("vector_cosine_similarity(query_vec, doc_vec)").cast("double"))
        .drop("query_vec", "doc_vec")
        .withColumn("method", F.lit("minilm"))
    )
    lexical = paired.withColumn(
        "score",
        F.size(F.array_intersect(lexical_tokens(F.col("query")), lexical_tokens(F.col("document")))).cast(
            "double"
        ),
    ).withColumn("method", F.lit("token_overlap"))
    ranked = embedded.unionByName(lexical).withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("method", "query_id").orderBy(F.desc("score"), "document_id")),
    )
    return ranked


def evaluate(rankings, k=K):
    """Only exhaustive, positive-bearing query judgments enter rank metrics."""
    if not isinstance(k, int) or isinstance(k, bool) or k <= 0:
        raise ValueError("k must be a positive integer")
    ideal = Window.partitionBy("method", "query_id").orderBy(F.desc("grade"), "document_id")
    scored = rankings.withColumn("_ideal_rank", F.row_number().over(ideal))
    gain = F.pow(F.lit(2.0), F.col("grade")) - 1.0
    weight = F.log2(F.col("rank") + 1)
    ideal_weight = F.log2(F.col("_ideal_rank") + 1)
    metrics = (
        scored.groupBy("method", "query_id", "split")
        .agg(
            F.count("*").alias("documents"),
            F.sum(F.when(F.col("grade").isNotNull(), 1).otherwise(0)).alias("judged_documents"),
            F.sum(F.when(F.col("grade") > 0, 1).otherwise(0)).alias("relevant_documents"),
            F.sum(F.when((F.col("rank") <= k) & (F.col("grade") > 0), 1).otherwise(0)).alias("_hits"),
            F.max(
                F.when((F.col("rank") <= k) & (F.col("grade") > 0), 1.0 / F.col("rank")).otherwise(0.0)
            ).alias("_rr"),
            F.sum(F.when(F.col("rank") <= k, gain / weight).otherwise(0.0)).alias("_dcg"),
            F.sum(F.when(F.col("_ideal_rank") <= k, gain / ideal_weight).otherwise(0.0)).alias("_idcg"),
            F.sum(F.when((F.col("rank") <= k) & F.col("grade").isNull(), 1).otherwise(0)).alias(
                "unjudged_in_top_k"
            ),
        )
        .withColumn(
            "status",
            F.when(F.col("judged_documents") < F.col("documents"), "incomplete_judgments")
            .when(F.col("relevant_documents") == 0, "no_relevant_documents")
            .otherwise("scored"),
        )
    )
    return metrics.select(
        "method",
        "query_id",
        "split",
        "documents",
        "judged_documents",
        "relevant_documents",
        "unjudged_in_top_k",
        "status",
        F.when(F.col("status") == "scored", F.col("_hits") / F.col("relevant_documents")).alias(
            "recall_at_k"
        ),
        F.when(F.col("status") == "scored", F.col("_rr")).alias("mrr_at_k"),
        F.when(F.col("status") == "scored", F.col("_dcg") / F.col("_idcg")).alias("ndcg_at_k"),
    )


def write_table(frame, directory):
    directory = Path(directory)
    frame.write.parquet(str(directory))
    if not list(directory.glob("*.parquet")):
        # A downstream Parquet consumer also needs a physical typed empty file.
        directory.mkdir(parents=True, exist_ok=True)
        table = frame.toArrow()
        pq.write_table(table, directory / "empty.parquet")


def export_and_load(tables, root, snapshot_id, fixture_sha256):
    """Publish no data: write a new local snapshot, then consume its ranked pair splits."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "snapshot_id": snapshot_id,
        "format_version": 1,
        "fixture_sha256": fixture_sha256,
        "model": MODEL_REPO,
        "model_revision": MODEL_REVISION,
        "processing_config": PROCESSING_CONFIG,
        "tables": {},
    }
    for name, frame in tables.items():
        write_table(frame, root / name)
        files = sorted((root / name).glob("*.parquet"))
        manifest["tables"][name] = {
            "rows": frame.count(),
            "files": [{"path": str(p.relative_to(root)), "sha256": sha256(p)} for p in files],
        }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return consume_export(root, snapshot_id, fixture_sha256)


def consume_export(root, expected_snapshot, expected_fixture_sha256=None):
    from datasets import disable_progress_bars, load_dataset

    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["snapshot_id"] != expected_snapshot:
        raise ValueError("snapshot revision mismatch")
    if (
        manifest["format_version"] != 1
        or manifest["model"] != MODEL_REPO
        or manifest["model_revision"] != MODEL_REVISION
    ):
        raise ValueError("model or export format revision mismatch")
    if manifest.get("processing_config") != PROCESSING_CONFIG:
        raise ValueError("processing configuration mismatch")
    if expected_fixture_sha256 is not None and manifest["fixture_sha256"] != expected_fixture_sha256:
        raise ValueError("fixture checksum mismatch")
    if set(manifest["tables"]) != {"documents", "queries", "judgments", "quarantine", "rankings", "metrics"}:
        raise ValueError("snapshot table set differs")
    for table in manifest["tables"].values():
        for item in table["files"]:
            candidate = (root / item["path"]).resolve()
            if not candidate.is_relative_to(root.resolve()) or sha256(candidate) != item["sha256"]:
                raise ValueError("snapshot file checksum mismatch")
    exported = {}
    for name, table in manifest["tables"].items():
        records = [row for item in table["files"] for row in pq.read_table(root / item["path"]).to_pylist()]
        if len(records) != table["rows"]:
            raise ValueError("exported table count differs")
        exported[name] = records
    files = [str(root / x["path"]) for x in manifest["tables"]["rankings"]["files"]]
    disable_progress_bars()
    loaded = load_dataset(
        "parquet", data_files={"ranked_pairs": files}, cache_dir=str(root / "loader-cache")
    )["ranked_pairs"]
    if loaded.num_rows != manifest["tables"]["rankings"]["rows"] or loaded.column_names != PAIR_COLUMNS:
        raise ValueError("loader keys or row count differ")
    if any(
        loaded.features[name].dtype != dtype
        for name, dtype in {
            "query_revision": "int64",
            "document_revision": "int64",
            "judgment_revision": "int64",
            "grade": "int64",
            "rank": "int32",
            "score": "float64",
        }.items()
    ):
        raise ValueError("loader types differ")
    for name in set(PAIR_COLUMNS) - {
        "query_revision",
        "document_revision",
        "judgment_revision",
        "grade",
        "rank",
        "score",
    }:
        if loaded.features[name].dtype not in {"string", "large_string"}:
            raise ValueError("loader text types differ")
    documents = {r["document_id"]: r for r in exported["documents"]}
    queries = {r["query_id"]: r for r in exported["queries"]}
    judgments = {(r["query_id"], r["document_id"]): r for r in exported["judgments"]}
    for row in loaded:
        query, document = queries[row["query_id"]], documents[row["document_id"]]
        if (
            row["snapshot_id"] != expected_snapshot
            or row["query_revision"] != query["revision"]
            or row["document_revision"] != document["revision"]
            or row["query"] != query["text"]
            or row["document"] != document["text"]
            or row["split"] != query["split"]
            or row["query_group"] != query["query_group"]
        ):
            raise ValueError("ranked pair revision or content mismatch")
        judgment = judgments.get((row["query_id"], row["document_id"]))
        if any(
            row[key] != (judgment[key] if judgment else None)
            for key in ["judgment_revision", "grade", "label_origin"]
        ):
            raise ValueError("ranked pair judgment mismatch")
    keys = [(r["method"], r["query_id"], r["document_id"]) for r in loaded]
    expected_keys = {
        (method, query, document)
        for method in ["minilm", "token_overlap"]
        for query in queries
        for document in documents
    }
    if len(keys) != len(set(keys)) or set(keys) != expected_keys:
        raise ValueError("missing or duplicate ranked pair in loader")
    for method, query in {(r[0], r[1]) for r in keys}:
        rows = [r for r in loaded if r["method"] == method and r["query_id"] == query]
        if sorted(r["rank"] for r in rows) != list(range(1, len(documents) + 1)) or not all(
            math.isfinite(r["score"]) for r in rows
        ):
            raise ValueError("invalid ranks or scores in loader")
    return [
        {"split": split, "rows": sum(r["split"] == split for r in loaded), "snapshot_id": expected_snapshot}
        for split in ["train", "validation", "test"]
    ]


def run(spark: SparkSession, export_dir=None):
    if export_dir is not None and Path(export_dir).exists():
        raise FileExistsError("export directory already exists")
    fixture_bytes = (HERE / "fixture.json").read_bytes()
    fixture = json.loads(fixture_bytes)
    tables = curate(spark, fixture)
    root = temp_dir("search-evaluation-")
    # Freeze curated inputs once before model calls and reuse them in evaluation/export.
    for name, frame in list(tables.items()):
        write_table(frame, root / name)
        tables[name] = spark.read.schema(frame.schema).parquet(str(root / name))
    rankings = (
        rank_pairs(spark, tables, root / "vectors")
        .withColumn("snapshot_id", F.lit(fixture["snapshot_id"]))
        .select(*PAIR_COLUMNS)
    )
    write_table(rankings, root / "rankings")
    rankings = spark.read.schema(rankings.schema).parquet(str(root / "rankings"))
    metrics = evaluate(rankings)
    outputs = {**tables, "rankings": rankings, "metrics": metrics}
    loaded = export_and_load(
        outputs,
        export_dir or root / "export",
        fixture["snapshot_id"],
        hashlib.sha256(fixture_bytes).hexdigest(),
    )
    outputs["consumer"] = spark.createDataFrame(loaded, "split string, rows long, snapshot_id string")
    return {
        name: frame.orderBy(
            *{
                "documents": ["document_id"],
                "queries": ["query_id"],
                "judgments": ["query_id", "document_id"],
                "quarantine": ["record_type", "row_id"],
                "rankings": ["method", "query_id", "rank"],
                "metrics": ["method", "query_id"],
                "consumer": ["split"],
            }[name]
        )
        for name, frame in outputs.items()
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, help="new local directory for a retained snapshot")
    args = parser.parse_args()
    server = SparkConnectServer()
    server.start()
    spark = SparkSession.builder.remote(f"sc://localhost:{server.listening_address[1]}").getOrCreate()
    try:
        result = run(spark, args.export_dir)
        result["metrics"].show(40, truncate=False)
        result["consumer"].show(truncate=False)
    finally:
        spark.stop()
        server.stop()
