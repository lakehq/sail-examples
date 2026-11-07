---
title: Choose a Python UDF style
id: udf-styles
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

```bash
uv run main.py
```

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+---+-------------------+-------------------+-------------------+-------------------+-------------------+
| id|            builtin|            row_udf|         pandas_udf|          arrow_udf|       map_in_arrow|
+---+-------------------+-------------------+-------------------+-------------------+-------------------+
|  0|                0.0|                0.0|                0.0|                0.0|                0.0|
|  1|0.45701200505444706|0.45701200505444706|0.45701200505444706|0.45701200505444706|0.45701200505444706|
|  2|  0.849485488971323|  0.849485488971323|  0.849485488971323|  0.849485488971323|  0.849485488971323|
|  3|  1.201136978367944|  1.201136978367944|  1.201136978367944|  1.201136978367944|  1.201136978367944|
|  4|  1.524617590797868| 1.5246175907978683| 1.5246175907978683| 1.5246175907978683| 1.5246175907978683|
+---+-------------------+-------------------+-------------------+-------------------+-------------------+

+--------------------+--------------------+--------------------+--------------------+
|             row_udf|          pandas_udf|           arrow_udf|        map_in_arrow|
+--------------------+--------------------+--------------------+--------------------+
|3.552713678800501...|3.552713678800501...|3.552713678800501...|3.552713678800501...|
+--------------------+--------------------+--------------------+--------------------+

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

The `max_difference_from_builtin` result shows how far each Python style lands from the built-in one; all four are within floating-point rounding.

## On Sail

- Python UDFs run inside the Sail server process, in one interpreter. NumPy and PyArrow compute functions release the GIL while they work on a batch.
- In an iterator-style `arrow_udf`, code before the loop runs once per batch, not once per partition. Put per-partition setup in `mapInArrow`, `mapInPandas` or an iterator `pandas_udf`.
- Build `mapInArrow` batches with Arrow types that match the declared schema; Apache Spark rejects an int64 column declared as `int`.
