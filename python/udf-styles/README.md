---
title: Choose a Python UDF style
id: udf-styles
facts: [udfs-need-a-session, udfs-share-one-interpreter, mapinarrow-arrow-types, udf-setup-per-partition]
summary: Choose the right way to run Python logic inside a query.
description: Use when you need Python logic in a Spark query and want to pick between built-in functions, a row UDF, a pandas UDF, an Arrow UDF and mapInArrow.
tier: python
level: intermediate
tags: [udf, pandas_udf, arrow_udf, mapInArrow, pyarrow]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-12
timeout: 120
needs_key: false
needs_gpu: false
prompt: Compute the same per-row score with Spark built-in functions, a row UDF, a pandas UDF, an Arrow UDF and mapInArrow, and check that they agree.
---

# Choose a Python UDF style

This example computes one score, `sqrt(a² + b²) + ln(1 + c)`, over 100,000 rows in five ways and checks that all five agree with the built-in version.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run python/udf-styles/main.py
```

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3 (revision 2).

```text
Scores (rounded to 12 decimal places):
+---+--------------+--------------+--------------+--------------+--------------+
| id|       builtin|       row_udf|    pandas_udf|     arrow_udf|  map_in_arrow|
+---+--------------+--------------+--------------+--------------+--------------+
|  0|           0.0|           0.0|           0.0|           0.0|           0.0|
|  1|0.457012005054|0.457012005054|0.457012005054|0.457012005054|0.457012005054|
|  2|0.849485488971|0.849485488971|0.849485488971|0.849485488971|0.849485488971|
|  3|1.201136978368|1.201136978368|1.201136978368|1.201136978368|1.201136978368|
|  4|1.524617590798|1.524617590798|1.524617590798|1.524617590798|1.524617590798|
+---+--------------+--------------+--------------+--------------+--------------+

Maximum absolute difference from built-in <= 1e-12:
+-------+----------+---------+------------+
|row_udf|pandas_udf|arrow_udf|map_in_arrow|
+-------+----------+---------+------------+
|   true|      true|     true|        true|
+-------+----------+---------+------------+

```

<!-- sail-output:end -->

## The five styles, and when to use each

| Style | Python is called | Use it when |
|---|---|---|
| Built-in functions (`F.sqrt`, `F.log1p`) | never | a built-in function exists. Check first; most do. |
| Row UDF (`@F.udf`) | once per row | the logic is plain Python on single values and nothing above fits. |
| pandas UDF (`@pandas_udf`) | once per batch of rows, with pandas Series | the logic is pandas or NumPy code. |
| Arrow UDF (`@arrow_udf`) | once per batch of rows, with Arrow arrays | the logic is `pyarrow.compute` code, or you want to avoid pandas. |
| `mapInArrow` | once per partition, over its record batches | you need setup once per partition (a model, a client), or the output has different rows or columns than the input. |

The console rounds scores to 12 decimal places and shows whether each style's
maximum absolute difference from the built-in result is at most `1e-12`.
Smaller rounding differences can vary across CPUs and math libraries. The `run()`
results, including `max_difference_from_builtin`, retain full precision for the
Sail/Spark comparison tests.

## On Sail

- Python UDFs run inside the Sail server process, in one interpreter. NumPy and PyArrow compute functions release the GIL while they work on a batch.
- In an iterator-style `arrow_udf`, code before the loop runs once per batch, not once per partition. Put per-partition setup in `mapInArrow`, `mapInPandas` or an iterator `pandas_udf`.
- Build `mapInArrow` batches with Arrow types that match the declared schema; Apache Spark rejects an int64 column declared as `int`.
