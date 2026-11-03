---
title: Enrich a table with a model
id: enrich-with-a-model
description: Use when you need a model's answer for every row of a large table and want to pay for one model call per distinct value, not per row.
tier: ai
level: intermediate
tags: [llm, enrichment, udf, mapInArrow, concurrency, distinct]
modality: text
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Label every support ticket in a table with a model, calling the model once per distinct ticket text, and show how many tickets got each label.
---

# Enrich a table with a model

A model call is the slow, expensive, rate-limited step of an enrichment pipeline. This example labels 20,000 support tickets but calls the model only 48 times, once per distinct ticket text. After the answers are saved, no later step calls the model.

## Run it

```bash
uv run main.py
```

A small local service stands in for the model, so the example runs offline with no API key. It labels a ticket with a fixed rule after a 20 ms delay and counts its calls.

## How it works

1. **Reduce.** `tickets.select("text").distinct()` turns 20,000 rows into 48 texts, spread over `ASK_TASKS` partitions.
2. **Ask.** `mapInArrow` sends each partition's texts to the model through a thread pool of `REQUESTS_PER_TASK` workers. With `ASK_TASKS` tasks, at most `ASK_TASKS * REQUESTS_PER_TASK` requests are in flight at once. Rate-limit responses (HTTP 429) and server errors are retried with backoff.
3. **Save.** The answers are written to Parquet once and read back. Later steps join against the saved answers, so they do not call the model.
4. **Integrate.** The saved answers join back onto all 20,000 tickets.

The `stats` result shows 20,000 tickets, 48 distinct texts and 48 model calls for this run. A task the engine retries after a failure calls the model again for its texts.

## Use a real model

Replace `ask_model` with a call to your model's API and keep the other steps. Set `ASK_TASKS * REQUESTS_PER_TASK` to the concurrency the API allows; the engine runs up to `ASK_TASKS` of these tasks at the same time.

## On Sail

- `cache()` and `persist()` do not keep results on Sail: every action recomputes the DataFrame, including the model calls in it. Step 3 writes the answers to Parquet so that one run calls the model once per distinct text.
- Python UDFs run inside the Sail server process. The thread pool lets each task keep several requests waiting on the network at the same time.
