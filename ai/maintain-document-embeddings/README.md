---
title: Maintain document embeddings as sources change
id: maintain-document-embeddings
facts: [vector-functions, cache-does-not-keep-results]
summary: Update a search dataset without re-embedding unchanged documents.
description: Use when you need to update a local embedding table after documents are added, edited or removed, while reusing compatible vectors for unchanged content.
tier: ai
level: advanced
tags: [embeddings, incremental-updates, onnx, mapInArrow, parquet]
modality: text
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 120
needs_key: false
needs_gpu: false
prompt: Use Sail to maintain a document embedding table across complete source snapshots. Encode only new content with the pinned MiniLM CPU model, preserve document-to-chunk mappings, exclude deleted documents from search, and publish a complete local generation after validation.
---

# Maintain document embeddings as sources change

Keep a local document search table current without re-encoding every paragraph.
Sail selects missing content with an anti-join, runs a pinned all-MiniLM-L6-v2
ONNX model, and writes vectors before using them in the next table generation.
Documents with identical paragraphs share vectors. Removing a source document
removes its chunks from the current search view.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run ai/maintain-document-embeddings/main.py
```

The isolated demonstration builds a table, edits one document, adds two IDs
(one sharing existing content), deletes one document and applies an unchanged
snapshot. Its update report separates unique paragraphs encoded, actual ONNX
calls and compatible vectors reused. Search encodes its query separately; query
calls are not included in the update report.
The model and tokenizer are fetched anonymously at an exact revision and cached
locally. First-download time is separate from subsequent execution.

The harness also rebuilds the final corpus with Python and fresh real-model
inference, without reading the maintained state. It compares every mapping,
vector, update count and ranked result. This verifies maintenance correctness,
not retrieval quality on arbitrary questions. The demo question exactly matches
a supplied paragraph.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3 (revision 2).

```text
+-------+---------+------+--------------+--------------+-------------+-------------+
|phase  |documents|chunks|unique_content|encoded_chunks|encoder_calls|reused_chunks|
+-------+---------+------+--------------+--------------+-------------+-------------+
|initial|4        |5     |5             |5             |1            |0            |
|no-op  |5        |6     |5             |0             |0            |5            |
|updated|5        |6     |5             |2             |1            |3            |
+-------+---------+------+--------------+--------------+-------------+-------------+

+----+-------------+-----------------+-----------+---------------------------------------------------------------+
|rank|doc_id       |score            |chunk_index|text                                                           |
+----+-------------+-----------------+-----------+---------------------------------------------------------------+
|1   |parquet-copy |1.0              |0          |Parquet stores data by column.                                 |
|2   |parquet-guide|1.0              |0          |Parquet stores data by column.                                 |
|3   |table-guide  |0.581671953201294|0          |Delta Lake adds transactions and time travel to Parquet tables.|
+----+-------------+-----------------+-----------+---------------------------------------------------------------+

The demo uses isolated temporary state. Use --documents and --state-dir to retain your table.
```

<!-- sail-output:end -->

## Maintain your own documents

Use a JSONL file containing a **complete snapshot**, with one unique string ID
and nonempty text per line:

```json
{"doc_id":"storage","text":"Parquet stores data by column.\n\nStatistics help skip row groups."}
{"doc_id":"tables","text":"Iceberg records table snapshots."}
```

Save the JSONL above as `ai/maintain-document-embeddings/documents.jsonl`, then run:

```bash
uv run ai/maintain-document-embeddings/main.py \
  --documents ai/maintain-document-embeddings/documents.jsonl \
  --state-dir ai/maintain-document-embeddings/.data/my-index
uv run ai/maintain-document-embeddings/main.py \
  --documents ai/maintain-document-embeddings/documents.jsonl \
  --state-dir ai/maintain-document-embeddings/.data/my-index \
  --query "table snapshots"
```

Edit `ai/maintain-document-embeddings/documents.jsonl` and run the same command again. IDs absent from the new snapshot
are deleted from the active view. An empty file therefore removes every active
document. Duplicate IDs or JSON object members, nulls, unknown fields, blank text and oversized input
reject the entire update; they never silently delete or replace an existing row.
The exception retains rejected source rows and reason codes.

Paragraphs are split at blank lines after CRLF/CR normalization and outer
whitespace trimming. Case and text inside paragraphs are preserved. A paragraph
must fit 128 tokenizer tokens, including special tokens; longer paragraphs fail
instead of being truncated. Earlier batches may already have run, but the failed
update does not change the current view. Split long paragraphs first. Custom
input is bounded to 128 documents, 16 KiB per document,
512 paragraphs and 4,096 retained cached vectors. This is a small local recipe.

## What is stored

Start with `update` for the distinct-content anti-join and persisted vectors,
`make_embedder` for bounded model calls, and `current_view` for joining current
document mappings to vectors. `publish_generation` coordinates the local
filesystem publication after those tables are complete.

```text
my-index/
  LOCK
  CURRENT.json
  generations/<generation>/
    documents/
    mapping/
    vectors/
    new-vectors/       # present when this update encoded content
    witnesses/
    manifest.json
  .staging/           # incomplete, unpublished attempts
```

The source-to-chunk mapping is separate from the vector cache. Keys include the
canonical paragraph hash and an embedding-space identity: exact model revision,
tokenizer, chunking, token limit, fixed padding, pooling, normalization, float32
and encoder settings. The manifest records the actual model/tokenizer file
hashes. Incompatible model or preprocessing settings invalidate reuse.

Only one write executes document inference. Later checks and joins read its
Parquet output. Each ONNX call records a durable started/completed witness and
the actual keys passed to it. Publication checks those witnesses against missing
keys, so an unexpected repeat or incomplete attempt fails validation. A no-op
branches before constructing the inference plan and reports zero document calls.
The implementation buffers at most 16 texts across incoming Arrow batches.

The complete generation is validated for file hashes, source/chunk identity,
unique keys, vector coverage, finite 384-dimensional values and unit norms. It
then replaces the small `CURRENT.json` pointer. Readers resolve that pointer
once and use the named immutable generation. Cached vectors no longer referenced
by a document remain available for later reuse. Old generations and abandoned
attempts remain on disk; source deletion is not secure erasure or garbage
collection.

Each changed snapshot encodes missing content, then rewrites and validates the
documents, mapping and entire compatible vector cache. Opening current state
also validates its files. Larger tables need a different persistence design;
the incremental part of this recipe is inference reuse.

## Publication and failure scope

This recipe requires one writer and a local POSIX filesystem shared by the
client and its local engine. The writer holds a nonblocking filesystem lock.
It rejects state directories containing unrelated files and symlinks.

The old pointer remains current if an update fails before publication. The next
update ignores unpublished attempts and may encode their new paragraphs again.
This does not promise exactly-once model calls, power-loss recovery, concurrent
writer transactions, object-store publication or distributed filesystem support.
`publish_generation` is the separate publication step, after staging and checks.

Use the simpler [embed-and-search](../embed-and-search/) example for a single
embedding/search pass without maintained state.
