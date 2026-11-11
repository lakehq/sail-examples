---
title: Upsert into a Delta Lake table with MERGE
id: delta-merge-upsert
facts: [delta-lake-support]
summary: Apply customer updates, inserts and deletes in one SQL statement.
featured: 3
preview: preview.svg
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

# A customer changed. Update the table.

Bo upgrades to gold. Cy changes their name to Cyrus. Ed closes their account,
and Fay joins. Apply all four changes with one SQL statement, then read the
customer table as it was before.

![Customer table before and after MERGE: Bo upgrades, Cy becomes Cyrus, Ed leaves and Fay joins](preview.svg)

## Run it

You need [uv](https://docs.astral.sh/uv/getting-started/installation/).
Run all commands below from the repository root (`sail-examples/`):

```bash
uv run pipelines/delta-merge-upsert/main.py
```

The first run installs
the pinned packages. The customer records are included in the script, so there
is no dataset to download, account to create or API key to supply.

The script prints the updated table, followed by version 0. In the updated table,
Bo is `gold`, Cyrus has ID `3`, and Fay has ID `6`. Ed remains visible only in
version 0. Temporary table files are removed when the process exits.

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

## The query to keep

```sql
MERGE INTO customers AS c
USING changes AS u
ON c.id = u.id
WHEN MATCHED AND u.deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET c.name = u.name, c.tier = u.tier
WHEN NOT MATCHED AND NOT u.deleted
  THEN INSERT (id, name, tier) VALUES (u.id, u.name, u.tier)
```

The ID decides which customer to change. The first matching condition decides
whether to delete, update or insert the row.

Sail executes the SQL and reads and writes Delta Lake directly. Delta Lake keeps
the table's versions, which is why the script can read the earlier rows. No Java
or separate Delta package is needed for this Sail run.

## Try one change: downgrade Ana

[pipelines/delta-merge-upsert/main.py](main.py):

```diff main.py variant=ana-silver
 CHANGES = [
+    (1, "Ana", "silver", False),
     (2, "Bo", "gold", False),  # tier upgrade
     (3, "Cyrus", "bronze", False),  # name change
     (5, "Ed", "bronze", True),  # account closed
     (6, "Fay", "silver", False),  # new customer
 ]
```

From the repository root (`sail-examples/`), run the edited script:

```bash
uv run pipelines/delta-merge-upsert/main.py
```

### Output after this change

<!-- sail-output:start variant=ana-silver -->

Current output · Sail 0.7.3.

```text
After MERGE:
+---+-----+------+
| id| name|  tier|
+---+-----+------+
|  1|  Ana|silver|
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

1. **Create a table.** Write the initial customers in Delta format and register its path as `customers`.
2. **Apply the changes.** `MERGE` matches on ID, removes deleted customers, updates existing customers and adds new ones.
3. **Read the past.** `option("versionAsOf", 0)` reads the rows from before the merge.

## Use your own records

Replace `CUSTOMERS` and `CHANGES` while preserving their declared schemas. This
small example assumes one change per customer in the batch. For retries, older
updates and conflicting versions, see the
[versioned changes guide](../apply-versioned-changes/README.md).

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
