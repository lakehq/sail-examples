---
title: Test Spark code with pytest on Sail
id: test-spark-code-with-pytest
description: Use when you want local unit tests for Spark DataFrame code in pytest, with a Sail session fixture and no Java.
tier: migrate
level: intermediate
tags: [testing, pytest, fixtures, assertDataFrameEqual, local]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Write pytest tests for a Spark DataFrame transformation, using a session-scoped fixture that starts Sail in-process.
---

# Test Spark code with pytest on Sail

Spark transformation code is ordinary Python functions over DataFrames, so it can be unit tested like any other code. This example tests `clean_orders`, a function that normalizes emails, keeps the latest version of each order and stores amounts in cents. The tests run against Sail: Java and a cluster are not needed.

## Run it

```bash
uv run main.py
```

Running `main.py` runs the four tests in `test_orders.py`. To run them with pytest directly, install the same packages and run `pytest test_orders.py`.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
....                                                                     [100%]
4 passed in 1.25s
```

<!-- sail-output:end -->

## How it works

- **`conftest.py`** defines a session-scoped `spark` fixture. It starts a Sail server in the test process with `SparkConnectServer()` and connects a normal PySpark session to it. Every test in the run shares the one server.
- **`test_orders.py`** calls `clean_orders` on a small DataFrame. It checks single properties with plain asserts and the whole result by comparing sorted, collected rows.
- **`main.py`** holds the code under test. The same function runs unchanged in production on Spark or Sail; this repository's checks confirm that `clean_orders` returns the same rows on both engines.

## With the pyspark-client package

- With the lightweight `pyspark-client` package, `pyspark.testing.assertDataFrameEqual` fails before it compares anything: it imports pandas-on-Spark and looks for a local Spark installation. Compare collected rows instead, as these tests do.

## Use it in your project

Copy `conftest.py` into your test directory and add `pysail` and a PySpark client to your test dependencies. Tests that take a `spark` fixture and use the DataFrame and SQL APIs then run on Sail; tests that use `spark.sparkContext`, RDDs, `assertDataFrameEqual` with `pyspark-client`, or JVM-only settings need changes first. Store wall-clock times as `TIMESTAMP_NTZ`, as here: `collect()` returns `TIMESTAMP` values as Python datetimes in the client's local time zone, whatever the session time zone is, so tests that compare them depend on the machine they run on.
