---
title: Upsert into a Delta Lake table with MERGE
id: delta-merge-upsert
description: Use when you need to apply a batch of changes (updates, inserts and deletes) to a Delta Lake table with one MERGE INTO statement, and read the table as it was before.
tier: pipelines
level: intermediate
tags: [delta-lake, merge, upsert, time-travel, sql]
modality: tabular
engines: both
spark_extras: [delta]
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Create a Delta Lake table of customers, apply a batch of changes with one MERGE INTO that updates, inserts and deletes rows, then read the table's previous version.
---

# Upsert into a Delta Lake table with MERGE

This example keeps a customer table in Delta Lake. It applies four changes with one `MERGE INTO` statement: an update, a rename, a deletion and an insert. Then it reads the table as it was before the merge.

## Run it

```bash
uv run main.py
```

Sail reads and writes Delta Lake natively, so no Delta package or JVM jars are needed.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
After MERGE:
+---+-----+------+
| id| name|  tier|
+---+-----+------+
|  1|  Ana|  gold|
|  2|   Bo|  gold|
|  3|Cyrus|bronze|
|  4|   Di|silver|
|  6|  Fay|silver|
+---+-----+------+

Version 0 (before MERGE):
+---+----+------+
| id|name|  tier|
+---+----+------+
|  1| Ana|  gold|
|  2|  Bo|silver|
|  3|  Cy|bronze|
|  4|  Di|silver|
|  5|  Ed|bronze|
+---+----+------+

```

<!-- sail-output:end -->

## How it works

- **Create the table.** The initial rows are written with `df.write.format("delta")`, and `CREATE TABLE ... USING delta LOCATION` registers the path so SQL can refer to it by name.
- **One MERGE for every kind of change.** Rows match on `id`:
  - a matched row flagged `deleted` is removed;
  - other matched rows are updated;
  - unmatched rows are inserted, unless they are flagged for deletion.
- **Read an older version.** `option("versionAsOf", 0)` reads the table as it was before the merge.

## On Apache Spark

The same code runs on Apache Spark once Delta Lake is added to the session. The checks run it on Spark 4.2 with `delta-spark` 4.4.1 and compare the tables with Sail's:

```python
spark = (
    SparkSession.builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```
