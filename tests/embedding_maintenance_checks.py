"""Independent real-model, invalid-input and process-restart controls."""

import contextlib
import hashlib
import importlib.util
import json
import os
import re
import uuid
from pathlib import Path


def load_example():
    path = Path(__file__).resolve().parent.parent / "ai/maintain-document-embeddings/main.py"
    spec = importlib.util.spec_from_file_location("embedding_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def canonical_hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def expected_mapping(documents, space_id):
    rows = []
    for document in sorted(documents, key=lambda row: row["doc_id"]):
        text = document["text"].replace("\r\n", "\n").replace("\r", "\n")
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
        for index, paragraph in enumerate(paragraphs):
            content = hashlib.sha256(paragraph.encode()).hexdigest()
            rows.append(
                {
                    "doc_id": document["doc_id"],
                    "chunk_index": index,
                    "text": paragraph,
                    "content_key": content,
                    "vector_key": canonical_hash({"space_id": space_id, "content_key": content}),
                }
            )
    return rows


def cpu_oracle(texts, spec):
    """Use fresh CPU inference one row at a time, without the example's encoder."""
    if not texts:
        return {}
    import numpy as np
    import onnxruntime as ort
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    model = hf_hub_download(
        spec["model_repo"], "onnx/model.onnx", revision=spec["model_revision"], token=False
    )
    tokenizer_path = hf_hub_download(
        spec["model_repo"], "tokenizer.json", revision=spec["model_revision"], token=False
    )
    tokenizer = Tokenizer.from_file(tokenizer_path)
    tokenizer.no_truncation()
    tokenizer.enable_padding(length=spec["max_tokens"], pad_id=0, pad_token="[PAD]")
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1
    session = ort.InferenceSession(model, sess_options=options, providers=["CPUExecutionProvider"])
    assert session.get_providers() == ["CPUExecutionProvider"]
    vectors = {}
    for text in sorted(set(texts)):
        encoded = tokenizer.encode(text)
        assert len(encoded.ids) <= spec["max_tokens"]
        ids = np.asarray([encoded.ids], dtype=np.int64)
        mask = np.asarray([encoded.attention_mask], dtype=np.int64)
        hidden = session.run(
            ["last_hidden_state"],
            {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)},
        )[0][0]
        # Select actual tokens rather than reproducing the batched pooling code.
        pooled = hidden[np.asarray(encoded.attention_mask, dtype=bool)].mean(axis=0)
        vectors[text] = pooled / np.linalg.norm(pooled)
    return vectors


def assert_complete(spark, module, state, documents):
    import numpy as np

    snapshot = module.read_current(spark, state)
    expected = expected_mapping(documents, snapshot["manifest"]["space_id"])
    actual = [row.asDict() for row in module.current_view(snapshot).collect()]
    assert len(actual) == len(expected)
    assert len({(row["doc_id"], row["chunk_index"]) for row in actual}) == len(expected)
    mapping_keys = ("doc_id", "chunk_index", "text", "content_key", "vector_key")
    assert [{key: row[key] for key in mapping_keys} for row in actual] == expected
    oracle = (
        cpu_oracle([row["text"] for row in expected], snapshot["manifest"]["embedding_spec"])
        if expected
        else {}
    )
    for row in actual:
        vector = np.asarray(row["embedding"])
        assert vector.shape == (384,) and np.isfinite(vector).all()
        np.testing.assert_allclose(np.linalg.norm(vector), 1, atol=1e-4)
        np.testing.assert_allclose(vector, oracle[row["text"]], rtol=1e-5, atol=1e-6)
    return snapshot, actual


def audit_records(directory, kind="ort"):
    return [json.loads(path.read_text()) for path in sorted(directory.glob(f"{kind}-*.json"))]


def instrument_encoder(module, directory):
    """Observe real ORT calls inside either engine's Python worker."""
    original = module.make_embedder
    directory.mkdir()

    def observed_factory(*args, **kwargs):
        function = original(*args, **kwargs)

        def observed(batches):
            import onnxruntime as ort

            real_session = ort.InferenceSession

            class ObservedSession:
                def __init__(self, *args, **kwargs):
                    self.session = real_session(*args, **kwargs)

                def get_providers(self):
                    return self.session.get_providers()

                def run(self, names, feeds):
                    shape = feeds["input_ids"].shape
                    assert 0 < shape[0] <= 16
                    path = directory / f"ort-{uuid.uuid4().hex}.json"
                    record = {"status": "started", "rows": shape[0], "tokens": shape[1]}
                    path.write_text(json.dumps(record))
                    output = self.session.run(names, feeds)
                    path.write_text(json.dumps(record | {"status": "completed"}))
                    return output

            def incoming():
                for batch in batches:
                    path = directory / f"arrow-{uuid.uuid4().hex}.json"
                    path.write_text(json.dumps({"rows": batch.num_rows}))
                    yield batch

            ort.InferenceSession = ObservedSession
            try:
                yield from function(incoming())
            finally:
                ort.InferenceSession = real_session

        return observed

    module.make_embedder = observed_factory


@contextlib.contextmanager
def forbid_encoder(module):
    original = module.make_embedder

    def unexpected(*args, **kwargs):
        raise AssertionError("a no-op or invalid pointer reached document inference")

    module.make_embedder = unexpected
    try:
        yield
    finally:
        module.make_embedder = original


def expect_failure(function, phrase):
    from pyspark.errors import PySparkException

    try:
        function()
    except (ValueError, RuntimeError, PySparkException) as error:
        assert phrase in str(error), f"expected {phrase!r}, got {error!r}"
        return error
    else:
        raise AssertionError(f"expected rejection containing {phrase!r}")


def input_contracts(spark, module, root):
    """No model imports, construction or artifact reads are needed for this phase."""
    state = root / "state"
    source = root / "source.jsonl"
    valid = [{"doc_id": "valid", "text": "One valid JSONL document."}]
    with forbid_encoder(module):
        empty = module.update(spark, [], state)
        assert empty["stats"]["encoded_chunks"] == empty["stats"]["encoder_calls"] == 0
        snapshot = module.read_current(spark, state)
        assert (
            snapshot["documents"].collect()
            == snapshot["mapping"].collect()
            == snapshot["vectors"].collect()
            == []
        )
        assert module.current_view(snapshot).collect() == []
        assert module.search(spark, snapshot).collect() == []
        pointer = (state / "CURRENT.json").read_bytes()
        assert module.update(spark, [], state)["stats"]["encoder_calls"] == 0
        assert (state / "CURRENT.json").read_bytes() == pointer

        invalid = [
            ([valid[0], valid[0]], "duplicate_doc_id"),
            ([{"doc_id": None, "text": "text"}], "invalid_doc_id"),
            ([{"doc_id": "id", "text": None}], "empty_or_nonstring_text"),
            ([{"doc_id": "", "text": "text"}], "invalid_doc_id"),
            ([{"doc_id": "id", "text": " \n "}], "empty_or_nonstring_text"),
            ([{"doc_id": "id", "text": "é" * 8193}], "document_too_large"),
            ([{"doc_id": str(i), "text": "text"} for i in range(129)], "too_many_documents"),
            ([{"doc_id": "id", "text": "\n\n".join(["x"] * 513)}], "too_many_chunks"),
        ]
        for documents, reason in invalid:
            expect_failure(lambda documents=documents: module.update(spark, documents, state), reason)
            assert (state / "CURRENT.json").read_bytes() == pointer
        source.write_text(json.dumps(valid[0]) + "\n")
        assert module.load_documents(source) == valid
        loader_invalid = [
            ('{"doc_id":"stable","text":"first","text":"second"}\n', "duplicate_json_member"),
            ('{"doc_id":"first","doc_id":"stable","text":"value"}\n', "duplicate_json_member"),
            ('{"doc_id":"stable","text":}\n', "invalid_json"),
            ('{"doc_id":"id","text":"text","unknown":1}\n', "expected_doc_id_and_text"),
            ("null\n", "expected_doc_id_and_text"),
            (json.dumps({"doc_id": "id", "text": None}) + "\n", "empty_or_nonstring_text"),
            (json.dumps({"doc_id": "id", "text": "é" * 8193}) + "\n", "document_too_large"),
            (" " * (128 * 16384 + 1), "source_file_too_large"),
        ]
        for payload, reason in loader_invalid:
            source.write_text(payload)
            error = expect_failure(lambda: module.update(spark, module.load_documents(source), state), reason)
            if reason == "duplicate_json_member":
                rejected = error.rejected[0]
                assert rejected["line"] == 1 and rejected["source"] == payload.rstrip("\n")
                assert rejected["member"] in {"doc_id", "text"}
            assert (state / "CURRENT.json").read_bytes() == pointer
        source.write_text("\n \t\n")
        assert module.load_documents(source) == []
        assert module.update(spark, module.load_documents(source), state)["stats"]["encoder_calls"] == 0
        assert (state / "CURRENT.json").read_bytes() == pointer

    specs = [
        module.embedding_spec(),
        module.embedding_spec(max_tokens=96),
        module.embedding_spec(model_revision="a" * 40),
    ]
    spaces = [canonical_hash(spec) for spec in specs]
    assert len(set(spaces)) == 3
    content = hashlib.sha256(valid[0]["text"].encode()).hexdigest()
    keys = [module.vector_key(space, content) for space in spaces]
    assert keys == [canonical_hash({"space_id": space, "content_key": content}) for space in spaces]
    assert len(set(keys)) == 3
    return {"invalid_snapshots": len(invalid), "invalid_jsonl": len(loader_invalid), "empty_search": True}


def maintenance(spark, module, root):
    state, audit = root / "state", root / "calls"
    instrument_encoder(module, audit)
    paragraphs = [
        f"Paragraph {i:03d} describes sensor reading number {i} and its unique event." for i in range(129)
    ]
    initial = [
        {"doc_id": "corpus", "text": "\n\n".join(paragraphs)},
        {"doc_id": "copy", "text": paragraphs[0] + "\n\n" + paragraphs[17]},
    ]
    first = module.update(spark, initial, state)
    records = audit_records(audit)
    incoming = audit_records(audit, "arrow")
    assert len(incoming) > 1 and sum(row["rows"] for row in incoming) == 129, incoming
    assert sorted(row["rows"] for row in records) == [1] + [16] * 8, records
    assert all(row["status"] == "completed" and row["tokens"] == 128 for row in records)
    assert first["stats"]["encoded_chunks"] == 129
    before, _ = assert_complete(spark, module, state, initial)
    assert before["vectors"].count() == 129

    edited = paragraphs[:64]
    edited[5] = "A newly corrected paragraph replaces the old sensor explanation."
    updated = [
        {"doc_id": "corpus", "text": "\n\n".join(edited)},
        {"doc_id": "new-alias", "text": paragraphs[0]},
    ]
    second = module.update(spark, updated, state)
    assert len(audit_records(audit)) == len(records) + 1
    assert second["stats"]["encoded_chunks"] == 1
    snapshot, current = assert_complete(spark, module, state, updated)
    assert snapshot["vectors"].count() == 130  # Inactive reusable content remains cached.
    assert {row["doc_id"] for row in current} == {"corpus", "new-alias"}
    shared = [row for row in current if row["text"] == paragraphs[0]]
    assert len(shared) == 2 and len({row["vector_key"] for row in shared}) == 1
    pointer = (state / "CURRENT.json").read_bytes()
    call_count = len(audit_records(audit))
    with forbid_encoder(module):
        noop = module.update(spark, list(reversed(updated)), state)
        assert noop["stats"]["encoded_chunks"] == noop["stats"]["encoder_calls"] == 0
        for _ in range(2):
            assert [row.asDict() for row in module.current_view(noop["snapshot"]).collect()] == current
    assert (state / "CURRENT.json").read_bytes() == pointer
    assert len(audit_records(audit)) == call_count

    # A real change in token padding/preprocessing must invalidate all active vectors.
    changed = module.update(spark, updated, state, max_tokens=96)
    assert changed["stats"]["encoded_chunks"] == 64
    new_records = audit_records(audit)
    assert len(new_records) == call_count + 4
    assert sum(row["rows"] for row in new_records if row["tokens"] == 96) == 64
    assert all(row["status"] == "completed" for row in new_records)
    after, _ = assert_complete(spark, module, state, updated)
    assert after["manifest"]["space_id"] != snapshot["manifest"]["space_id"]
    assert after["vectors"].count() == 64
    assert not {row.vector_key for row in snapshot["mapping"].collect()} & {
        row.vector_key for row in after["mapping"].collect()
    }
    # Revision identity is checked without downloading a second model revision.
    other_spec = module.embedding_spec(max_tokens=96, model_revision="a" * 40)
    assert canonical_hash(other_spec) != after["manifest"]["space_id"]
    assert not {row["vector_key"] for row in expected_mapping(updated, canonical_hash(other_spec))} & {
        row.vector_key for row in after["mapping"].collect()
    }
    return {"unique_initial": 129, "initial_calls": len(records), "active_after_edit": len(current)}


def recovery(spark, module, root, phase):
    state = root / "state"
    initial = [{"doc_id": "stable", "text": "The committed paragraph remains readable after interruption."}]
    changed = [{"doc_id": "stable", "text": "The replacement is published only after the pointer switch."}]
    record = root / "committed.json"
    if phase == "interrupt":
        module.update(spark, initial, state)
        _, rows = assert_complete(spark, module, state, initial)
        pointer = (state / "CURRENT.json").read_bytes()
        record.write_text(json.dumps({"pointer": pointer.decode(), "rows": rows, "pid": os.getpid()}))
        original = module.os.replace

        def interrupt(source, destination):
            if Path(destination) == state / "CURRENT.json":
                raise RuntimeError("simulated interruption before pointer publication")
            return original(source, destination)

        module.os.replace = interrupt
        try:
            expect_failure(lambda: module.update(spark, changed, state), "before pointer publication")
        finally:
            module.os.replace = original
        assert (state / "CURRENT.json").read_bytes() == pointer
        assert len(list((state / "generations").iterdir())) == 2
        assert list((state / ".staging").glob("CURRENT-*.tmp"))
        return "unpublished update left the committed pointer unchanged"

    committed = json.loads(record.read_text())
    assert committed["pid"] != os.getpid(), "reopen must use a fresh interpreter and engine"
    assert (state / "CURRENT.json").read_text() == committed["pointer"]
    _, rows = assert_complete(spark, module, state, initial)
    assert rows == committed["rows"]
    with forbid_encoder(module):
        assert module.update(spark, initial, state)["stats"]["encoder_calls"] == 0

    pointer = (state / "CURRENT.json").read_bytes()
    expect_failure(
        lambda: module.update(spark, [{"doc_id": "long", "text": "word " * 129}], state),
        "tokenizer tokens",
    )
    assert (state / "CURRENT.json").read_bytes() == pointer
    for corrupt, reason in [
        ({"generation_id": "../escape", "manifest_sha256": "0" * 64}, "generation identifier"),
        ({**json.loads(pointer), "manifest_sha256": "0" * 64}, "manifest checksum"),
    ]:
        (state / "CURRENT.json").write_text(json.dumps(corrupt))
        try:
            with forbid_encoder(module):
                expect_failure(lambda: module.read_current(spark, state), reason)
                expect_failure(lambda: module.update(spark, initial, state), reason)
            assert json.loads((state / "CURRENT.json").read_text()) == corrupt
        finally:
            (state / "CURRENT.json").write_bytes(pointer)
    _, rows = assert_complete(spark, module, state, initial)
    assert rows == committed["rows"]
    module.update(spark, changed, state)
    final, _ = assert_complete(spark, module, state, changed)
    cached = {row.vector_key for row in final["vectors"].collect()}
    with forbid_encoder(module):
        # A blank source file is an authoritative empty snapshot, so it deletes
        # active mappings while retaining compatible reusable vectors.
        source = root / "blank-source.jsonl"
        source.write_text("\n \t\n")
        deleted = module.update(spark, module.load_documents(source), state)
        assert deleted["stats"]["encoder_calls"] == 0
        empty_snapshot, rows = assert_complete(spark, module, state, [])
        assert not rows
        assert {row.vector_key for row in empty_snapshot["vectors"].collect()} == cached
        empty_pointer = (state / "CURRENT.json").read_bytes()
        assert module.update(spark, [], state)["stats"]["encoder_calls"] == 0
        assert (state / "CURRENT.json").read_bytes() == empty_pointer
    return {
        "reopened": True,
        "rejected_token_overflow": True,
        "corrupt_pointers": 2,
    }


def check(spark, engine):
    if engine == "spark":
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "37")
    module = load_example()
    root = Path(os.environ["EMBEDDING_CHECK_ROOT"])
    root.mkdir(parents=True, exist_ok=True)
    phase = os.environ["EMBEDDING_CHECK_PHASE"]
    if phase == "inputs":
        return input_contracts(spark, module, root)
    if phase == "maintenance":
        return maintenance(spark, module, root)
    assert phase in ("interrupt", "reopen")
    return recovery(spark, module, root, phase)
