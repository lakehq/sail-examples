---
title: Build a versioned search evaluation dataset
id: evaluate-search-dataset
description: Use when you need to curate versioned documents, queries and relevance judgments, compare two retrieval methods, and validate a Parquet snapshot with a dataset loader.
tier: pipelines
level: advanced
tags: [search, retrieval, evaluation, embeddings, parquet, data-quality]
modality: text
engines: both
ordered: true
float_tolerance: 1e-6
timeout: 120
needs_key: false
needs_gpu: false
prompt: Build a versioned search evaluation dataset with query-group splits, explicit relevance-label provenance, conflict quarantine, a token-overlap baseline and local MiniLM embeddings. Preserve missing judgments, compute recall, reciprocal rank and graded nDCG only for fully judged queries with relevant documents, and verify exported Parquet records with Hugging Face Datasets.
---

# Build a versioned search evaluation dataset

This example builds a small search evaluation snapshot from documents, queries and relevance judgments. Sail curates the records, runs a token-overlap baseline and a real local MiniLM embedding model, computes per-query metrics and exports Parquet. A Hugging Face Datasets loader reads every ranked pair and checks its keys, types, versions and counts against the exported source tables.

The committed support-policy fixture has seven retained documents and eight retained queries. The fictional support policies and relevance labels are illustrative fixture expectations, specified independently of retrieval scores. They demonstrate the pipeline, not retrieval quality on a reviewed corpus. The example reports both methods without assuming that embeddings improve the results.

## Run it

```bash
uv run main.py
```

The first run downloads the pinned model and tokenizer from Hugging Face. It runs on a CPU without credentials. The default export is temporary. To keep a snapshot in a new directory:

```bash
uv run main.py --export-dir .data/support-faq-v1
```

An existing export directory is rejected. The program prints per-query metrics and the number of ranked pairs loaded for each split. All exported tables, including quarantined input rows, remain in the retained directory.

## Keep versions and judgments together

[fixture.json](fixture.json) contains three input tables and a fixed query-group split map:

- Documents have an ID, a positive revision and text.
- Queries have an ID, a positive revision, text and a declared query group.
- Judgments identify both the query revision and the document revision. Each has its own revision, a relevance grade and a `label_origin`.

Grade `0` means not relevant, `1` means partially relevant and `2` means directly relevant under this fixture's authored expectations. More than one document can be relevant to a query. The `label_origin` value is `authored_expected`; retrieval scores never create labels.

Revision and grade fields must be integers or null in the input JSON. Floats, booleans and numeric strings reject the bundle before DataFrame construction, so schema conversion cannot truncate a fractional revision or label. Nulls and invalid integer values continue through the curation rules below.

The latest identifiable revision controls each record. Conflicting payloads at the same latest revision are all quarantined. An invalid payload at the latest identifiable revision blocks older valid values and valid peers at that revision. An invalid ID or nonpositive revision cannot establish a version barrier. Judgment rows referring to missing or different query or document revisions are quarantined before current judgments are selected. They cannot override labels for the current content.

Exact duplicate rows keep one deterministic representative. Document bodies and query text are also deduplicated after lowercasing and normalizing spaces, tabs, carriage returns and line feeds. A duplicate body keeps the lowest document ID; its labels are not transferred to the retained ID. Identical normalized query text assigned to different groups is quarantined on both sides. Input row IDs must be unique, nonempty strings; a bundle with invalid row IDs is rejected before curation. Within an accepted bundle, every source row is retained once or appears once in `quarantine` with its payload and reason. Original records are serialized once before queries, preserving their source fields and explicit nulls in quarantine.

The fixed groups map to `train`, `validation` and `test`. These are query-group splits over a **shared document corpus**. Normalized identical query text cannot cross groups. This does not detect paraphrases or near duplicates, establish document-disjoint splits, or establish that the embedding model has not seen related text during training. A production dataset needs an appropriate grouping policy and reviewed relevance labels.

## Compare ranked results without changing the labels

The token-overlap score counts distinct shared ASCII alphanumeric tokens. MiniLM embeds each query and document and uses cosine similarity. Both methods rank the entire retained corpus; equal scores are ordered by document ID.

The model is [`sentence-transformers/all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) at revision `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`, using its ONNX weights and tokenizer. The implementation truncates input to 128 tokens, applies attention-mask mean pooling and normalizes the vectors. Longer documents need a deliberate chunking or truncation policy. Inference uses bounded batches of at most 16 texts, with model setup once in the single embedding partition.

For each method and query, the example reports recall, reciprocal rank and graded normalized discounted cumulative gain at `k=3`. A grade above zero counts as relevant. nDCG uses gain `2^grade - 1` and logarithmic rank discount.

Missing judgments remain null in ranked results. A query with any missing corpus judgment receives `incomplete_judgments` and null metrics. The program does not remove unjudged candidates or treat them as negative. An exhaustively judged query with no relevant documents receives `no_relevant_documents` and null metrics, because these rank metrics do not measure whether a system should abstain. Other queries receive `scored`.

The fixture intentionally includes one incomplete query and one exhaustively judged query with no relevant documents. Neither is included in a headline average. The arithmetic tests use a separate plain Python implementation and hand-calculated cases; they validate metric computation, not the semantic correctness of the labels.

## Use the exported snapshot

The export contains `documents`, `queries`, `judgments`, `quarantine`, `rankings` and `metrics` Parquet directories. `manifest.json` records the snapshot ID, fixture checksum, model revision, processing configuration, file checksums and row counts. The configuration records `k`, token truncation, pooling, normalization, lexical tokenization and the tie break. Ranked pairs carry the exact query and document revisions, relevance grade, label origin, method, split, rank and score.

`consume_export(path, expected_snapshot)` in [main.py](main.py) verifies the snapshot and model revisions, requires the current processing configuration, and checks file checksums and exported row counts. It then uses `datasets.load_dataset("parquet", ...)` to load the complete ranked-pair table. It checks column types, unique and complete query-document-method keys, contiguous ranks and the correspondence of content and judgments to the exported source tables. Its optional `expected_fixture_sha256` argument also checks the manifest against a caller-supplied fixture checksum. Checksums detect changed files relative to the manifest; they do not authenticate a manifest received from another party.

The consumer reads actual records, including null judgments. This establishes that the snapshot can be ingested by that loader. It does not train or qualify a model. Adapt the three fixture tables and query-group map for another corpus, increment content revisions when text changes, and supply judgments for those exact revisions before interpreting quality metrics.
