---
title: Query Parquet and CSV files with SQL
id: query-files-with-sql
description: Use when you want to explore data files on your laptop with plain SQL (query a Parquet or CSV file in place, name files as views, join, group and sort) without loading a database or starting a cluster.
tier: quickstart
level: beginner
tags: [sql, parquet, csv, files, temporary-views, joins, nyc-taxi]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Download the NYC green taxi trip records for January 2024 (Parquet) and the taxi zone lookup (CSV), then use Spark SQL on the files to count trips, find the busiest pickup zones, show trips by hour, and compare payment types.
---

# Query Parquet and CSV files with SQL

Point SQL at a file and query it: no tables to create, no data to load first, no cluster. This example uses real data published by the NYC Taxi and Limousine Commission (TLC): the 56,551 green taxi trip records for January 2024, a 1.4 MB Parquet file, and the taxi zone lookup, a 12 KB CSV file. It answers a few first questions about the trips with Spark SQL.

## Run it

```bash
uv run main.py
```

The first run downloads both files into `.data/` next to the script.

The busiest pickup zone is East Harlem North, with 12,141 trips. The median trip is 1.79 miles. The longest recorded trip is 201,421.68 miles, which is a data error: real data needs checks, and the [ETL job example](../../pipelines/taxi-etl-job/README.md) shows how to apply them.

## How it works

1. **Query a file in place.** `` FROM parquet.`path` `` reads a Parquet file directly; `` csv.`path` `` and `` json.`path` `` work the same way, but a CSV file read like this keeps its header line as a data row, so the next step reads the zone lookup through a view.
2. **Name files as views.** `CREATE TEMPORARY VIEW ... USING parquet OPTIONS (path ...)` gives a file a name for the rest of the session. The CSV view declares its column types, so the zone ids are integers that join cleanly with the trip records. `DESCRIBE TABLE trips` lists the columns and types the Parquet file carries.
3. **Join, group and sort.** Trips join the zone lookup on the pickup zone id, then group by zone or by hour of the day.
4. **Decode codes.** A `CASE` expression turns the payment type codes into names, using the TLC data dictionary. The dictionary describes `tip_amount` as "automatically populated for credit card tips. Cash tips are not included." That is why cash trips show a tip share of 0.

## Use your own files

Change the paths. A path can also be a folder or a glob such as `data/2024-*.parquet`, which reads every matching file as one table.

## On Sail

Declaring the CSV column types also keeps the results the same on Sail and Apache Spark: with `inferSchema`, Sail reads whole-number columns as `BIGINT` while Apache Spark reads them as `INT`.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January 2024, and the taxi zone lookup table. Field definitions and codes are in the TLC's [data dictionary for green taxi trips](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf).
