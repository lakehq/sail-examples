---
title: Read a REST API as a table
id: rest-api-as-a-table
facts: [python-data-source-partitions-concurrent, udf-dependencies-in-server-env]
summary: Query a paginated REST API as a table.
description: Use when you want to query a paginated REST API with Spark SQL and DataFrames by writing a Python data source that fetches pages in parallel.
tier: python
level: advanced
tags: [python-data-source, rest-api, http, pagination, partitions]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Write a Spark Python data source that reads every page of a paginated JSON API in parallel, then count the tickets by status.
---

# Read a REST API as a table

This example reads 487 support tickets from a paginated JSON API as a DataFrame, saves them once, and aggregates them by status. It uses a Python data source with one partition per API page, and the engine reads partitions as separate tasks, so pages are fetched concurrently.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run python/rest-api-as-a-table/main.py
```

A small local service stands in for the API (`GET /tickets?page=N`, 50 tickets per page), so the example runs offline.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+-------+------------+
|tickets|distinct_ids|
+-------+------------+
|    487|         487|
+-------+------------+

+-------+-------+------------+
| status|tickets|avg_priority|
+-------+-------+------------+
| closed|    122|         2.0|
|   open|    122|         1.0|
|pending|    121|         4.0|
| solved|    122|         3.0|
+-------+-------+------------+

```

<!-- sail-output:end -->

## How it works

- **`TicketsDataSource`** subclasses `pyspark.sql.datasource.DataSource`. It declares the schema and returns a reader for the `url` option.
- **`partitions()`** asks the API for page 1 and returns one `InputPartition` per page.
- **`read(partition)`** fetches its page and yields one tuple per ticket. Each partition runs as its own task, so several pages are fetched at the same time on both Sail and Spark.
- **`spark.dataSource.register(...)`** makes the source available as `spark.read.format("tickets_api")`. After that, it is an ordinary DataFrame: filter it, join it, write it to a table.
- **Save once.** Every action on a data source DataFrame reads the API again, so the example writes the rows to Parquet and runs its queries on the saved copy.

## Use a real API

Replace `get_page` with a call to your API. Options are part of the query plan and can appear in plan output and logs, so do not pass secrets as options: read credentials inside `read()` from the environment of the process that runs the reader, or from a secret manager. If the API is rate limited, return fewer, larger partitions.
