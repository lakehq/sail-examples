---
title: Build a batch ETL job with data quality checks
id: taxi-etl-job
description: Use when you build a batch pipeline that turns raw files into clean, partitioned Parquet and reporting tables, preserves original rejected records with their source provenance, removes clean duplicates, and replaces one month's outputs on a completed re-run.
tier: pipelines
level: intermediate
tags: [etl, data-quality, parquet, partitioning, idempotent, schema, nyc-taxi, pipelines]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 180
needs_key: false
needs_gpu: false
prompt: Write a Spark batch ETL job that loads twelve monthly NYC green taxi Parquet files into a fixed clean schema, writes original rejected records with their first failed rule and source-file identity, removes clean duplicates, replaces one month's clean and reject directories on re-run, and builds daily and borough-by-month reporting tables.
---

# Build a batch ETL job with data quality checks

This is the shape of most batch pipelines: raw files in, clean partitioned tables and reports out, with every dropped row accounted for. The input is real: every NYC green taxi trip record from 2024, twelve monthly Parquet files published by the NYC Taxi and Limousine Commission (TLC). The job processes one month at a time, the way a scheduler would run it.

## Run it

```bash
uv run main.py
```

The first run downloads the files (16 MB) into `.data/` next to the script.

Of 660,218 raw records, 24,946 fail a rule and 635,272 are kept. Most rejected rows (23,603) fail the duration rule. No kept row is an exact duplicate of another. Running March again leaves the table unchanged: the same row count and the same checksum over every row.

## How it works

1. **A fixed schema.** Each raw file is mapped to the job's own column names and types (`CLEAN_COLUMNS`), so later steps do not depend on how the source names things. The month comes from the file being loaded.
2. **Rules with reasons.** Each rule in `RULES` is a SQL condition. A row's reject reason is the first rule it fails:
   - a missing pickup or dropoff time, distance, fare or total;
   - a pickup outside the file's month;
   - a duration that is not 1 to 180 whole minutes (`timestampdiff(MINUTE, ...)` counts whole minutes);
   - a distance that is not 0 to 100 miles;
   - a negative amount;
   - a pickup or dropoff zone missing from the zone lookup.

   A condition that comes out NULL counts as failed, so a row is never kept because a value it was checked on was missing. Each rejected occurrence keeps its original source row in a `source_record` struct, captured before casts, plus `reject_reason`, the scheduled `month`, relative `source_file` and verified `source_sha256`. The twelve pinned source files share one raw schema. A different source schema or an invalid cast may fail the job before these rules run; this is not a general malformed-file parser. Duplicate zone lookup IDs fail explicitly instead of multiplying source records.
3. **Duplicates.** `dropDuplicates()` removes rows that are identical in every column of the clean schema. Unused original fields and source provenance do not change that identity. Rejected occurrences are never deduplicated: two identical bad input rows remain two rejected rows. Source-file provenance is not a unique physical-row identifier.
4. **One clean and reject directory per month.** `write_month` replaces `clean/month=N` and `rejects/month=N`, including an empty reject partition when a corrected month has no rejected rows. Other months are left alone. The two writes are separate operations, not an atomic transaction; an interruption can leave them inconsistent until a complete re-run. The job re-runs March and compares the clean row count and a checksum (the sum of every row's `hash`). This is a consistency check, not a collision-free proof of row equality. Small adversarial fixtures also compare the persisted rejected payloads as exact multisets and check that a reject-free re-run clears stale rejects.
5. **Read back with the declared schema.** `spark.read.schema(CLEAN_SCHEMA).parquet(clean)` reads all months as one table, with `month` typed from the directory names.
6. **Rejects and reporting tables.** The returned `rejected` DataFrame reads the persisted rejected rows, and `quality` counts them by reason. Daily totals and borough-by-month counts are written as Parquet for a dashboard or a downstream job. Temporary output directories are removed when the script exits; choose durable paths when adapting `write_month` for your own job.

## Run it on a schedule

Turn the loop in `run` into a job that takes the month as an argument, and run it once per month, from cron or a workflow scheduler. Point the paths at object storage on a cluster.

## On Sail

- Write each partition's own directory, as here, to replace one partition. Sail does not support dynamic partition overwrite for Parquet: `mode("overwrite").partitionBy("month")` replaces every month, even with `spark.sql.sources.partitionOverwriteMode` set to `dynamic`. Apache Spark replaces only the months in the data. Delta Lake tables follow that setting on both engines, and Delta Lake's `replaceWhere` works on both.
- Read partitioned data with a schema. Without one, Sail returns partition columns as strings, while Apache Spark infers numbers and dates.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January to December 2024, and the taxi zone lookup table.
