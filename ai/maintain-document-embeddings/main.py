# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "certifi==2026.10.19",
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "numpy==2.5.4",
#   "onnxruntime==1.31.1",
#   "tokenizers==0.22.1",
#   "huggingface-hub==0.36.0",
# ]
# ///
"""Maintain a local document embedding table from complete source snapshots."""

import argparse
import atexit
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import uuid
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
MAX_TOKENS = 128
DIMENSION = 384
BATCH_SIZE = 16
MAX_DOCUMENTS = 128
MAX_TEXT_BYTES = 16_384
MAX_CHUNKS = 512
MAX_CACHE_ROWS = 4096
DOCUMENT_SCHEMA = "doc_id STRING, text STRING"
MAPPING_SCHEMA = "doc_id STRING, chunk_index LONG, text STRING, content_key STRING, vector_key STRING"
VECTOR_SCHEMA = "vector_key STRING, content_key STRING, space_id STRING, embedding ARRAY<FLOAT>"
DOCUMENT_ARROW_SCHEMA = pa.schema([("doc_id", pa.string()), ("text", pa.string())])
MAPPING_ARROW_SCHEMA = pa.schema(
    [
        ("doc_id", pa.string()),
        ("chunk_index", pa.int64()),
        ("text", pa.string()),
        ("content_key", pa.string()),
        ("vector_key", pa.string()),
    ]
)
VECTOR_ARROW_SCHEMA = pa.schema(
    [
        ("vector_key", pa.string()),
        ("content_key", pa.string()),
        ("space_id", pa.string()),
        ("embedding", pa.list_(pa.float32())),
    ]
)
STATS_SCHEMA = (
    "phase STRING, documents LONG, chunks LONG, unique_content LONG, "
    "encoded_chunks LONG, encoder_calls LONG, reused_chunks LONG"
)
CURRENT_SCHEMA = pa.schema(
    [
        ("doc_id", pa.string()),
        ("chunk_index", pa.int64()),
        ("text", pa.string()),
        ("content_key", pa.string()),
        ("vector_key", pa.string()),
        ("embedding", pa.list_(pa.float32())),
    ]
)
TOP_SCHEMA = pa.schema(
    [
        ("rank", pa.int64()),
        ("doc_id", pa.string()),
        ("score", pa.float64()),
        ("chunk_index", pa.int64()),
        ("text", pa.string()),
    ]
)
INITIAL_DOCUMENTS = [
    {"doc_id": "parquet-guide", "text": "Parquet stores data by column.\n\nThis paragraph will be removed."},
    {"doc_id": "table-guide", "text": "Delta Lake adds transactions and time travel to Parquet tables."},
    {
        "doc_id": "stream-guide",
        "text": "Watermarks describe how long a streaming aggregation waits for late events.",
    },
    {"doc_id": "vector-guide", "text": "An embedding represents a text as a vector for similarity search."},
]
UPDATED_DOCUMENTS = [
    {
        "doc_id": "parquet-guide",
        "text": "Parquet stores data by column.\n\nColumn statistics help skip row groups.",
    },
    INITIAL_DOCUMENTS[1],
    INITIAL_DOCUMENTS[3],
    {"doc_id": "parquet-copy", "text": "Parquet stores data by column."},
    {"doc_id": "iceberg-guide", "text": "Iceberg records snapshots of a table and its data files."},
]
QUERY = "Parquet stores data by column."


class SnapshotValidationError(ValueError):
    def __init__(self, rejected):
        self.rejected = rejected
        super().__init__(json.dumps({"rejected": rejected}, ensure_ascii=False))


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def file_sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    with Path(path).open("x") as output:
        output.write(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")
        output.flush()
        os.fsync(output.fileno())


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def temporary_directory():
    path = Path(tempfile.mkdtemp(prefix="maintained-embeddings-"))
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def validate_documents(documents):
    rows = list(documents)
    rejected = []
    if len(rows) > MAX_DOCUMENTS:
        raise SnapshotValidationError([{"reason": "too_many_documents", "maximum": MAX_DOCUMENTS}])
    ids = Counter(
        row.get("doc_id") for row in rows if isinstance(row, dict) and isinstance(row.get("doc_id"), str)
    )
    for index, row in enumerate(rows):
        reason = None
        if not isinstance(row, dict) or set(row) != {"doc_id", "text"}:
            reason = "expected_doc_id_and_text"
        elif not isinstance(row["doc_id"], str) or not row["doc_id"].strip() or len(row["doc_id"]) > 128:
            reason = "invalid_doc_id"
        elif ids[row["doc_id"]] != 1:
            reason = "duplicate_doc_id"
        elif not isinstance(row["text"], str) or not row["text"].strip():
            reason = "empty_or_nonstring_text"
        elif len(row["text"].encode("utf-8")) > MAX_TEXT_BYTES:
            reason = "document_too_large"
        if reason:
            rejected.append({"row": index, "reason": reason, "source": row})
    if rejected:
        raise SnapshotValidationError(rejected)
    return sorted(rows, key=lambda row: row["doc_id"])


def load_documents(path):
    path = Path(path)
    if path.stat().st_size > MAX_DOCUMENTS * MAX_TEXT_BYTES:
        raise SnapshotValidationError([{"reason": "source_file_too_large"}])
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue

        def unique_members(pairs, number=number, line=line):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise SnapshotValidationError(
                        [{"line": number, "reason": "duplicate_json_member", "member": key, "source": line}]
                    )
                value[key] = item
            return value

        try:
            rows.append(json.loads(line, object_pairs_hook=unique_members))
        except json.JSONDecodeError as error:
            raise SnapshotValidationError(
                [{"line": number, "reason": "invalid_json", "source": line}]
            ) from error
    return validate_documents(rows)


def embedding_spec(max_tokens=MAX_TOKENS, model_revision=MODEL_REVISION):
    if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or not 8 <= max_tokens <= 512:
        raise ValueError("max_tokens must be an integer from 8 to 512")
    if not isinstance(model_revision, str) or not re.fullmatch(r"[0-9a-f]{40}", model_revision):
        raise ValueError("model_revision must be an exact 40-character revision")
    return {
        "model_repo": MODEL_REPO,
        "model_revision": model_revision,
        "model_file": "onnx/model.onnx",
        "tokenizer_file": "tokenizer.json",
        "dimension": DIMENSION,
        "chunker": "blank-line-paragraphs-lf-strip-v1",
        "max_tokens": max_tokens,
        "overlong_chunks": "reject",
        "padding": "fixed-length-pad-0",
        "pooling": "attention-mask-mean",
        "normalization": "l2-epsilon-1e-12",
        "dtype": "float32",
        "provider": "CPUExecutionProvider",
        "intra_op_threads": 1,
        "encoder": "minilm-onnx-v1",
        "onnxruntime": "1.31.1",
        "tokenizers": "0.22.1",
        "numpy": "2.5.4",
    }


def vector_key(space_id, content_key):
    return digest({"space_id": space_id, "content_key": content_key})


def chunk_documents(documents, spec):
    space_id = digest(spec)
    chunks = []
    for row in documents:
        paragraphs = re.split(r"\n\s*\n", row["text"].replace("\r\n", "\n").replace("\r", "\n"))
        for index, paragraph in enumerate(part.strip() for part in paragraphs if part.strip()):
            content_key = hashlib.sha256(paragraph.encode("utf-8")).hexdigest()
            chunks.append(
                {
                    "doc_id": row["doc_id"],
                    "chunk_index": index,
                    "text": paragraph,
                    "content_key": content_key,
                    "vector_key": vector_key(space_id, content_key),
                }
            )
    if len(chunks) > MAX_CHUNKS:
        raise SnapshotValidationError([{"reason": "too_many_chunks", "maximum": MAX_CHUNKS}])
    return chunks


@contextmanager
def writer_lock(state_dir):
    if "://" in str(state_dir):
        raise ValueError("state_dir must be a local POSIX filesystem path")
    state = Path(state_dir).absolute()
    if state.is_symlink():
        raise ValueError("state_dir must not be a symlink")
    state.mkdir(parents=True, exist_ok=True)
    allowed = {"LOCK", "CURRENT.json", "generations", ".staging"}
    if any(path.name not in allowed for path in state.iterdir()):
        raise ValueError("state_dir contains files outside this example's state format")
    if any(path.is_symlink() for path in state.iterdir()):
        raise ValueError("state entries must not be symlinks")
    with (state / "LOCK").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another writer holds this state directory") from error
        try:
            (state / "generations").mkdir(exist_ok=True)
            (state / ".staging").mkdir(exist_ok=True)
            yield state
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def load_encoder(spec):
    import importlib.metadata

    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    for package in ("onnxruntime", "tokenizers", "numpy"):
        if importlib.metadata.version(package) != spec[package]:
            raise ValueError(f"{package} does not match the embedding-space identity")
    paths = {
        name: hf_hub_download(spec["model_repo"], spec[name], revision=spec["model_revision"], token=False)
        for name in ("model_file", "tokenizer_file")
    }
    options = ort.SessionOptions()
    options.intra_op_num_threads = spec["intra_op_threads"]
    session = ort.InferenceSession(
        paths["model_file"], sess_options=options, providers=["CPUExecutionProvider"]
    )
    if session.get_providers() != ["CPUExecutionProvider"]:
        raise ValueError("the CPU example must use only CPUExecutionProvider")
    tokenizer = Tokenizer.from_file(paths["tokenizer_file"])
    tokenizer.no_truncation()
    tokenizer.enable_padding(length=spec["max_tokens"], pad_id=0, pad_token="[PAD]")
    return session, tokenizer, {name: file_sha256(path) for name, path in paths.items()}


def encode_batch(session, tokenizer, rows, spec, artifacts, witness_dir=None):
    import numpy as np

    encodings = tokenizer.encode_batch([row["text"] for row in rows])
    if any(len(value.ids) > spec["max_tokens"] for value in encodings):
        raise ValueError(
            f"a paragraph exceeds {spec['max_tokens']} tokenizer tokens; split it before updating"
        )
    ids = np.array([value.ids for value in encodings], dtype=np.int64)
    mask = np.array([value.attention_mask for value in encodings], dtype=np.int64)
    feeds = {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)}
    witness = {
        "status": "started",
        "vector_keys": [row["vector_key"] for row in rows],
        "artifacts": artifacts,
    }
    witness_path = None
    if witness_dir is not None:
        witness_path = Path(witness_dir) / f"{uuid.uuid4().hex}.json"
        write_json(witness_path, witness)
    (hidden,) = session.run(["last_hidden_state"], feeds)
    weights = mask.astype(np.float32)[:, :, None]
    pooled = (hidden * weights).sum(axis=1) / np.clip(weights.sum(axis=1), 1e-9, None)
    vectors = (pooled / np.clip(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-12, None)).astype(
        np.float32
    )
    if vectors.shape != (len(rows), DIMENSION) or not np.isfinite(vectors).all():
        raise ValueError("encoder returned invalid vectors")
    if witness_path is not None:
        completed = witness_path.with_suffix(".completed")
        write_json(completed, witness | {"status": "completed"})
        os.replace(completed, witness_path)
    return vectors.tolist()


def make_embedder(spec, witness_dir, expected_artifacts=None):
    def embed(batches):
        session, tokenizer, artifacts = load_encoder(spec)
        if expected_artifacts is not None and artifacts != expected_artifacts:
            raise ValueError("downloaded model/tokenizer bytes differ from the published cache")
        schema = pa.schema(
            [
                ("vector_key", pa.string()),
                ("content_key", pa.string()),
                ("space_id", pa.string()),
                ("embedding", pa.list_(pa.float32())),
            ]
        )
        pending = []

        def emit(rows):
            vectors = encode_batch(session, tokenizer, rows, spec, artifacts, witness_dir)
            return pa.RecordBatch.from_pylist(
                [
                    {
                        "vector_key": row["vector_key"],
                        "content_key": row["content_key"],
                        "space_id": digest(spec),
                        "embedding": vector,
                    }
                    for row, vector in zip(rows, vectors, strict=True)
                ],
                schema=schema,
            )

        # Accumulate at most 16 texts across incoming batches, so call accounting
        # does not depend on how either engine divides its Arrow stream.
        for batch in batches:
            for row in batch.to_pylist():
                pending.append(row)
                if len(pending) == BATCH_SIZE:
                    yield emit(pending)
                    pending = []
        if pending:
            yield emit(pending)

    return embed


def read_witnesses(path):
    return [json.loads(file.read_text()) for file in sorted(Path(path).glob("*.json"))]


def write_parquet(frame, path, schema, row_count):
    """Write a typed artifact even when an engine emits nothing for empty input."""
    path = Path(path)
    if row_count == 0:
        path.mkdir()
        pq.write_table(pa.Table.from_pylist([], schema=schema), path / "part-empty.parquet")
    else:
        frame.coalesce(1).write.parquet(str(path))


def validate_generation(path):
    path = Path(path)
    if (
        path.is_symlink()
        or (path / "manifest.json").is_symlink()
        or any(file.is_symlink() for file in path.rglob("*"))
    ):
        raise ValueError("generation must not be a symlink")
    manifest = json.loads((path / "manifest.json").read_text())
    if manifest["format_version"] != 1 or digest(manifest["embedding_spec"]) != manifest["space_id"]:
        raise ValueError("incompatible embedding manifest")
    spec = manifest["embedding_spec"]
    if spec != embedding_spec(spec["max_tokens"], spec["model_revision"]):
        raise ValueError("unsupported embedding-space settings")
    files = {str(file.relative_to(path)): file for file in path.rglob("*") if file.is_file()}
    expected = {row["path"]: row for row in manifest["files"]}
    if set(files) - {"manifest.json"} != set(expected):
        raise ValueError("generation file inventory differs")
    for name, metadata in expected.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or files[name].is_symlink():
            raise ValueError("unsafe generation file")
        if file_sha256(files[name]) != metadata["sha256"]:
            raise ValueError(f"generation file checksum differs: {name}")
    documents = pq.read_table(path / "documents").to_pylist()
    mapping = pq.read_table(path / "mapping").to_pylist()
    vector_table = pq.read_table(path / "vectors")
    if vector_table.num_rows:
        vector_type = vector_table.schema.field("embedding").type
        if not pa.types.is_list(vector_type) or vector_type.value_type != pa.float32():
            raise ValueError("vectors must use variable-length ARRAY<FLOAT>")
    vectors = vector_table.to_pylist()
    artifacts = manifest["artifacts"]
    if vectors and (
        not isinstance(artifacts, dict)
        or set(artifacts) != {"model_file", "tokenizer_file"}
        or any(
            not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
            for value in artifacts.values()
        )
    ):
        raise ValueError("cached vectors require model and tokenizer file identities")
    documents = validate_documents(documents)
    if digest(documents) != manifest["source_snapshot_sha256"]:
        raise ValueError("source snapshot identity differs")
    if sorted(mapping, key=lambda row: (row["doc_id"], row["chunk_index"])) != chunk_documents(
        documents, manifest["embedding_spec"]
    ):
        raise ValueError("source-to-chunk mapping differs")
    if len(vectors) > MAX_CACHE_ROWS:
        raise ValueError("cache exceeds this example's row bound")
    indexed = {}
    for row in vectors:
        vector = row["embedding"]
        if any(
            not isinstance(row[field], str) or not re.fullmatch(r"[0-9a-f]{64}", row[field])
            for field in ("vector_key", "content_key", "space_id")
        ):
            raise ValueError("vector identities must be SHA-256 strings")
        if (
            row["vector_key"] in indexed
            or row["space_id"] != manifest["space_id"]
            or row["vector_key"] != vector_key(row["space_id"], row["content_key"])
        ):
            raise ValueError("duplicate or incompatible vector key")
        if (
            not isinstance(vector, list)
            or len(vector) != DIMENSION
            or not all(isinstance(value, (int, float)) and math.isfinite(value) for value in vector)
        ):
            raise ValueError("vector must contain 384 finite values")
        if not math.isclose(math.sqrt(sum(value * value for value in vector)), 1.0, abs_tol=1e-4):
            raise ValueError("embedding is not unit normalized")
        indexed[row["vector_key"]] = row
    if any(
        row["vector_key"] not in indexed or indexed[row["vector_key"]]["content_key"] != row["content_key"]
        for row in mapping
    ):
        raise ValueError("current chunks have missing or incompatible vectors")
    if manifest["rows"] != {"documents": len(documents), "chunks": len(mapping), "vectors": len(vectors)}:
        raise ValueError("generation row counts differ")
    return manifest


def read_current(spark, state_dir):
    state = Path(state_dir)
    pointer = state / "CURRENT.json"
    if pointer.is_symlink():
        raise ValueError("CURRENT must not be a symlink")
    if not pointer.exists():
        return None
    current = json.loads(pointer.read_text())
    if not isinstance(current, dict) or set(current) != {"generation_id", "manifest_sha256"}:
        raise ValueError("invalid CURRENT pointer")
    generation = current["generation_id"]
    if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
        raise ValueError("invalid generation identifier")
    path = state / "generations" / generation
    if (state / "generations").is_symlink() or path.is_symlink() or (path / "manifest.json").is_symlink():
        raise ValueError("generation paths must not be symlinks")
    if file_sha256(path / "manifest.json") != current["manifest_sha256"]:
        raise ValueError("CURRENT manifest checksum differs")
    manifest = validate_generation(path)
    return {
        "path": path,
        "manifest": manifest,
        "documents": spark.read.schema(DOCUMENT_SCHEMA).parquet(str(path / "documents")),
        "mapping": spark.read.schema(MAPPING_SCHEMA).parquet(str(path / "mapping")),
        "vectors": spark.read.schema(VECTOR_SCHEMA).parquet(str(path / "vectors")),
    }


def current_view(snapshot):
    return (
        snapshot["mapping"]
        .join(snapshot["vectors"].select("vector_key", "embedding"), "vector_key")
        .select("doc_id", "chunk_index", "text", "content_key", "vector_key", "embedding")
        .orderBy("doc_id", "chunk_index")
    )


def publish_generation(state, stage):
    """Publish one complete generation on the same local filesystem."""
    validate_generation(stage)
    destination = state / "generations" / stage.name
    os.rename(stage, destination)
    sync_directory(state / "generations")
    validate_generation(destination)
    next_pointer = state / ".staging" / f"CURRENT-{stage.name}.tmp"
    write_json(
        next_pointer,
        {"generation_id": stage.name, "manifest_sha256": file_sha256(destination / "manifest.json")},
    )
    os.replace(next_pointer, state / "CURRENT.json")
    sync_directory(state)
    return destination


def update(spark, documents, state_dir, *, max_tokens=MAX_TOKENS, model_revision=MODEL_REVISION):
    """Apply a full authoritative snapshot while holding the writer lock."""
    documents = validate_documents(documents)
    spec = embedding_spec(max_tokens, model_revision)
    mapping = chunk_documents(documents, spec)
    space_id, source_id = digest(spec), digest(documents)
    with writer_lock(state_dir) as state:
        previous = read_current(spark, state)
        compatible = previous is not None and previous["manifest"]["space_id"] == space_id
        cached = previous["vectors"] if compatible else spark.createDataFrame([], VECTOR_SCHEMA)
        requested = spark.createDataFrame(mapping, MAPPING_SCHEMA)
        distinct = requested.select("vector_key", "content_key", "text").dropDuplicates(["vector_key"])
        work = (
            distinct.join(cached.select("vector_key"), "vector_key", "left_anti")
            .orderBy("vector_key")
            .coalesce(1)
        )
        work_keys = [row.vector_key for row in work.select("vector_key").collect()]
        stats = {
            "documents": len(documents),
            "chunks": len(mapping),
            "unique_content": len({row["vector_key"] for row in mapping}),
            "encoded_chunks": len(work_keys),
            "encoder_calls": 0,
            "reused_chunks": len({row["vector_key"] for row in mapping}) - len(work_keys),
        }
        if compatible and previous["manifest"]["source_snapshot_sha256"] == source_id:
            if work_keys:
                raise ValueError("unchanged source has missing cached vectors")
            return {"snapshot": previous, "stats": stats, "witnesses": []}
        generation = uuid.uuid4().hex
        stage = state / ".staging" / generation
        stage.mkdir()
        witnesses = stage / "witnesses"
        witnesses.mkdir()
        write_parquet(
            spark.createDataFrame(documents, DOCUMENT_SCHEMA),
            stage / "documents",
            DOCUMENT_ARROW_SCHEMA,
            len(documents),
        )
        write_parquet(requested, stage / "mapping", MAPPING_ARROW_SCHEMA, len(mapping))
        artifacts = previous["manifest"]["artifacts"] if compatible else None
        if work_keys:
            # Only this write can execute document inference. Every later use is
            # a fresh read of these files, not another action on the UDF plan.
            work.mapInArrow(make_embedder(spec, str(witnesses), artifacts), VECTOR_SCHEMA).write.parquet(
                str(stage / "new-vectors")
            )
            new_vectors = spark.read.schema(VECTOR_SCHEMA).parquet(str(stage / "new-vectors"))
        else:
            new_vectors = spark.createDataFrame([], VECTOR_SCHEMA)
        observed = read_witnesses(witnesses)
        if any(row["status"] != "completed" for row in observed) or Counter(
            key for row in observed for key in row["vector_keys"]
        ) != Counter(work_keys):
            raise ValueError("inference witnesses show incomplete work or repeated attempts")
        if observed:
            identities = [row["artifacts"] for row in observed]
            if any(identity != identities[0] for identity in identities):
                raise ValueError("workers used different model or tokenizer bytes")
            artifacts = identities[0]
        stats["encoder_calls"] = len(observed)
        vectors = cached.unionByName(new_vectors)
        vector_count = vectors.count()
        if vector_count > MAX_CACHE_ROWS:
            raise ValueError("cache exceeds this example's row bound")
        write_parquet(vectors, stage / "vectors", VECTOR_ARROW_SCHEMA, vector_count)
        manifest = {
            "format_version": 1,
            "embedding_spec": spec,
            "space_id": space_id,
            "source_snapshot_sha256": source_id,
            "artifacts": artifacts,
            "rows": {"documents": len(documents), "chunks": len(mapping), "vectors": vector_count},
            "update": stats,
            "files": [
                {"path": str(path.relative_to(stage)), "sha256": file_sha256(path)}
                for path in sorted(stage.rglob("*"))
                if path.is_file()
            ],
        }
        write_json(stage / "manifest.json", manifest)
        publish_generation(state, stage)
        return {"snapshot": read_current(spark, state), "stats": stats, "witnesses": observed}


def search(spark, snapshot, query=QUERY, limit=3):
    if not isinstance(query, str) or not query.strip() or len(query.encode()) > MAX_TEXT_BYTES:
        raise ValueError("query must be nonempty bounded text")
    if snapshot["manifest"]["rows"]["chunks"] == 0:
        return spark.createDataFrame(
            [], "rank LONG, doc_id STRING, score DOUBLE, chunk_index LONG, text STRING"
        )
    spec = snapshot["manifest"]["embedding_spec"]
    key = hashlib.sha256(query.encode()).hexdigest()
    row = {"vector_key": vector_key(digest(spec), key), "content_key": key, "text": query}
    work = temporary_directory()
    (work / "witnesses").mkdir()
    spark.createDataFrame([row], "vector_key STRING, content_key STRING, text STRING").coalesce(1).mapInArrow(
        make_embedder(spec, str(work / "witnesses"), snapshot["manifest"]["artifacts"]), VECTOR_SCHEMA
    ).write.parquet(str(work / "query"))
    question = (
        spark.read.schema(VECTOR_SCHEMA)
        .parquet(str(work / "query"))
        .select(F.col("embedding").alias("query_vector"))
    )
    scored = (
        current_view(snapshot)
        .crossJoin(question)
        .withColumn("score", F.expr("vector_cosine_similarity(embedding, query_vector)").cast("double"))
    )
    best = scored.withColumn(
        "chunk_rank",
        F.row_number().over(Window.partitionBy("doc_id").orderBy(F.desc("score"), "chunk_index")),
    ).filter("chunk_rank = 1")
    return (
        best.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("score"), "doc_id")))
        .filter(F.col("rank") <= limit)
        .select(F.col("rank").cast("long").alias("rank"), "doc_id", "score", "chunk_index", "text")
        .orderBy("rank")
    )


def run(spark: SparkSession) -> dict[str, DataFrame]:
    state = temporary_directory()
    summaries = []
    for phase, documents in [
        ("initial", INITIAL_DOCUMENTS),
        ("updated", UPDATED_DOCUMENTS),
        ("no-op", UPDATED_DOCUMENTS),
    ]:
        result = update(spark, documents, state)
        summaries.append({"phase": phase, **result["stats"]})
    return {
        "current": current_view(result["snapshot"]),
        "updates": spark.createDataFrame(summaries, STATS_SCHEMA).orderBy("phase"),
        "top3": search(spark, result["snapshot"]),
    }


def reference():
    """Rebuild the final corpus with Python and real ONNX inference, without state."""
    spec = embedding_spec()
    space_id = digest(spec)
    rows = []
    for document in sorted(UPDATED_DOCUMENTS, key=lambda row: row["doc_id"]):
        text = document["text"].replace("\r\n", "\n").replace("\r", "\n")
        paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n", text) if paragraph.strip()]
        for index, paragraph in enumerate(paragraphs):
            content = hashlib.sha256(paragraph.encode()).hexdigest()
            rows.append(
                {
                    "doc_id": document["doc_id"],
                    "chunk_index": index,
                    "text": paragraph,
                    "content_key": content,
                    "vector_key": vector_key(space_id, content),
                }
            )
    unique = {row["vector_key"]: row for row in rows}
    keys = sorted(unique)
    session, tokenizer, artifacts = load_encoder(spec)
    vectors = {}
    for start in range(0, len(keys), BATCH_SIZE):
        batch_keys = keys[start : start + BATCH_SIZE]
        values = encode_batch(session, tokenizer, [unique[key] for key in batch_keys], spec, artifacts)
        vectors.update(zip(batch_keys, values, strict=True))
    question = encode_batch(session, tokenizer, [{"vector_key": "query", "text": QUERY}], spec, artifacts)[0]
    complete = [row | {"embedding": vectors[row["vector_key"]]} for row in rows]
    scored = {}
    for row in complete:
        vector = row["embedding"]
        similarity = sum(a * b for a, b in zip(question, vector, strict=True)) / (
            math.sqrt(sum(value * value for value in question))
            * math.sqrt(sum(value * value for value in vector))
        )
        if row["doc_id"] not in scored or similarity > scored[row["doc_id"]]["score"]:
            scored[row["doc_id"]] = {key: row[key] for key in ("doc_id", "chunk_index", "text")} | {
                "score": similarity
            }
    top = [
        row | {"rank": index}
        for index, row in enumerate(
            sorted(scored.values(), key=lambda row: (-row["score"], row["doc_id"]))[:3], 1
        )
    ]
    summaries = [
        {
            "phase": "initial",
            "documents": 4,
            "chunks": 5,
            "unique_content": 5,
            "encoded_chunks": 5,
            "encoder_calls": 1,
            "reused_chunks": 0,
        },
        {
            "phase": "updated",
            "documents": 5,
            "chunks": 6,
            "unique_content": 5,
            "encoded_chunks": 2,
            "encoder_calls": 1,
            "reused_chunks": 3,
        },
        {
            "phase": "no-op",
            "documents": 5,
            "chunks": 6,
            "unique_content": 5,
            "encoded_chunks": 0,
            "encoder_calls": 0,
            "reused_chunks": 5,
        },
    ]
    stats_schema = pa.schema(
        [(name, pa.string() if name == "phase" else pa.int64()) for name in summaries[0]]
    )
    return {
        "current": pa.Table.from_pylist(complete, schema=CURRENT_SCHEMA),
        "updates": pa.Table.from_pylist(sorted(summaries, key=lambda row: row["phase"]), schema=stats_schema),
        "top3": pa.Table.from_pylist(top, schema=TOP_SCHEMA),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents", type=Path, help="complete authoritative JSONL source snapshot")
    parser.add_argument("--state-dir", type=Path, help="local state directory retained between updates")
    parser.add_argument("--query", help="optional search, encoded separately from document update accounting")
    args = parser.parse_args()
    if bool(args.documents) != bool(args.state_dir):
        parser.error("supply --documents and --state-dir together")
    if args.query and not args.documents:
        parser.error("--query requires --documents and --state-dir")
    server = SparkConnectServer()
    server.start()
    spark = None
    try:
        spark = SparkSession.builder.remote(f"sc://localhost:{server.listening_address[1]}").getOrCreate()
        if args.documents:
            result = update(spark, load_documents(args.documents), args.state_dir)
            print(json.dumps(result["stats"], sort_keys=True))
            print(f"Current generation: {result['snapshot']['path']}")
            if args.query:
                search(spark, result["snapshot"], args.query).show(truncate=False)
        else:
            result = run(spark)
            result["updates"].show(truncate=False)
            result["top3"].show(truncate=False)
            print(
                "The demo uses isolated temporary state. Use --documents and --state-dir to retain your table."
            )
    finally:
        if spark is not None:
            spark.stop()
        server.stop()
