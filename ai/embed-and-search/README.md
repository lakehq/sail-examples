---
title: Embed text and search it with vector functions
id: embed-and-search
description: Use when you want to compute text embeddings inside the engine with an ONNX model and find the most similar documents with SQL vector functions.
tier: ai
level: intermediate
tags: [embeddings, onnx, vector-search, cosine-similarity, mapInArrow]
modality: text
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 180
needs_key: false
needs_gpu: false
prompt: Embed a set of documents with the all-MiniLM-L6-v2 ONNX model inside Sail, store the vectors, and return the three most similar documents for each question.
---

# Embed text and search it with vector functions

This example embeds 24 short documents with the all-MiniLM-L6-v2 sentence model inside the engine, saves the vectors, and answers three questions by ranking documents with `vector_cosine_similarity`.

## Run it

```bash
uv run main.py
```

The first run downloads the model (about 90 MB) from Hugging Face into the local Hugging Face cache. No account or key is needed. The model revision is pinned, so every run uses the same weights.

## How it works

- **Load once per partition.** `mapInArrow` downloads the model where the task runs (cached per machine), then opens the ONNX session and the tokenizer before its loop over batches. Code before the loop runs once per partition in `mapInArrow`, not once per batch or row.
- **Embed.** Each Arrow batch is split into groups of at most 16 texts. Each group is tokenized to 128 tokens, run through the model, mean-pooled and normalized to unit length.
- **Store as `ARRAY<FLOAT>`.** The vectors leave `mapInArrow` as a list of float32 values, the type the vector functions accept.
- **Save once.** The document vectors are written to Parquet and read back, so the search never runs the model over the documents again.
- **Search in SQL.** A cross join of questions and documents is scored with `vector_cosine_similarity`, and a window function keeps the top three per question.

The cross join scores every query against every document. Check that this query-document product fits your memory and runtime limits before increasing the input size. This example does not build or query a vector index.

## On Sail

- The vector functions take `ARRAY<FLOAT>`. Sail also rejects Arrow fixed-size lists (as written by NumPy or PyArrow), so cast them with `CAST(col AS ARRAY<FLOAT>)` first.
- Python UDFs run inside the Sail server process. ONNX Runtime releases the GIL while it runs the model.
