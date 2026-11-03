---
title: Search a folder of PDFs
id: pdf-search
description: Use when you need to turn PDF documents into searchable text chunks with embeddings (extract, chunk, embed, search) inside the engine, one row per file in and small rows out.
tier: multimodal
level: intermediate
tags: [pdf, documents, chunking, embeddings, rag, onnx, mapInArrow]
modality: document
engines: both
ordered: true
float_tolerance: 1e-5
timeout: 180
needs_key: false
needs_gpu: false
prompt: Read every PDF in a folder, split each page into overlapping text chunks, embed the chunks with the all-MiniLM-L6-v2 ONNX model, and return the chunks that best answer a question with their document and page.
---

# Search a folder of PDFs

This example writes four short PDF documents (ten pages in all), splits every page into overlapping text chunks, embeds the chunks, and retrieves the three chunks most similar to a question, cited by document and page. This is the ingestion half of a retrieval (RAG) pipeline.

## Run it

```bash
uv run main.py
```

The first run downloads the all-MiniLM-L6-v2 ONNX model (about 90 MB) into the Hugging Face cache. The model revision is pinned.

## How it works

1. **Extract and chunk.** A table of paths, one PDF per row, goes through `mapInArrow`. `pypdf` reads each page, and the text is split into chunks of 60 words that overlap by 15. The function emits at most 32 text chunks per output batch, each with its document, page, chunk number and text. It skips pages with no text. The PDF reader and extracted text for an individual page still consume memory.
2. **Save the chunks.** They are written to Parquet once, so later steps never parse the PDFs again.
3. **Embed.** A second `mapInArrow` downloads the model where the task runs (cached per machine), loads it once per partition, processes at most 16 texts per model call, and returns an `ARRAY<FLOAT>` vector per chunk. The vectors are saved too.
4. **Search.** The question is embedded the same way, every chunk is scored with `vector_cosine_similarity`, and the top three come back with their document and page.

## Use your own PDFs

Replace `make_pdfs` and the `paths` table with your own files. Paths must be readable from wherever the UDF runs: local paths on one machine, object storage URLs (read with a client such as `fsspec`) on a cluster. Scanned PDFs without a text layer need OCR first.
