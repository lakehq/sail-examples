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

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3 (revision 2).

```text
+-----------+------+------+--------+----------+
|customer_id|orders| spend|last_day|spend_rank|
+-----------+------+------+--------+----------+
|         71|    40|2496.0|      22|         1|
|        142|    40|2492.0|      23|         2|
|        213|    40|2488.0|      24|         3|
|         34|    40|2484.0|      25|         4|
|        105|    40|2480.0|      26|         5|
|        176|    40|2476.0|      27|         6|
|        247|    40|2472.0|      28|         7|
|         68|    40|2468.0|      29|         8|
|        139|    40|2464.0|      30|         9|
|        210|    40|2460.0|      21|        10|
+-----------+------+------+--------+----------+

```

<!-- sail-output:end -->

## How it works

- `SparkConnectServer()` starts Sail in the current process, on a free port.
- `SparkSession.builder.remote(...)` connects the standard PySpark client to that server.
- `run(spark)` is ordinary PySpark: the same function runs on Apache Spark, and this repository's tests check that both engines return the same rows.

To use a Sail server that is already running, skip the first two steps and connect with `SparkSession.builder.remote("sc://localhost:50051")`.
