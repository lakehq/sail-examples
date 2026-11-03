---
title: Hello Sail
id: hello-sail
description: Use when you want to run existing Spark SQL and DataFrame code on your laptop with Sail, with no Java and no cluster.
tier: quickstart
level: beginner
tags: [sql, dataframe, window-functions, local]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 60
needs_key: false
needs_gpu: false
prompt: Run this Spark DataFrame and SQL code locally on Sail and show the ten customers with the highest spend.
---

# Hello Sail

This example's Spark SQL and DataFrame code runs unchanged on Sail. It starts a Sail server inside the Python process, connects a normal PySpark session to it over Spark Connect, and runs a SQL aggregation and a window function over generated data.

## Run it

```bash
uv run main.py
```

`uv` installs `pysail` and the PySpark client from the header of `main.py`. Java is not needed.

## How it works

- `SparkConnectServer()` starts Sail in the current process, on a free port.
- `SparkSession.builder.remote(...)` connects the standard PySpark client to that server.
- `run(spark)` is ordinary PySpark: the same function runs on Apache Spark, and this repository's tests check that both engines return the same rows.

To use a Sail server that is already running, skip the first two steps and connect with `SparkSession.builder.remote("sc://localhost:50051")`.
