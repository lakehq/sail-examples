---
title: Query Parquet and CSV files with SQL
id: query-files-with-sql
facts: [query-files-directly, schema-inference]
summary: Find the busiest taxi pickup zones with SQL on real files.
featured: 1
preview: preview.svg
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

# Where do people catch taxis in New York?

Start with a Parquet file of taxi trips and a CSV of pickup zones. Join them with
SQL to find the busiest places. Sail runs the queries on your laptop, without
loading a database or setting up a cluster.

![Top five pickup zones: East Harlem North leads with 12,141 trips](preview.svg)

These are the top five zones in the January 2024 NYC green taxi file. The script
prints ten. East Harlem North leads, and the median trip distance is 1.79 miles.
One record claims a trip of 201,421.68 miles: a useful reminder to check real data.

## Run it

You need [uv](https://docs.astral.sh/uv/getting-started/installation/).
Run all commands below from the repository root (`sail-examples/`):

```bash
uv run quickstart/query-files-with-sql/main.py
```

The first run installs the pinned packages and downloads a 1.4 MB Parquet file
and a 12 KB CSV into `quickstart/query-files-with-sql/.data/`. No account, API key or GPU is needed.

The output includes an overview, the busiest pickup zones and payment types.
Look for `56551` in the overview's `trips` column and `East Harlem North` at the
top of the pickup-zone table. The [source code](main.py) also returns hourly
counts and the file's schema for you to explore.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+-----+-------------------+-------------------+------------+---------+
|trips|first_pickup       |last_pickup        |median_miles|max_miles|
+-----+-------------------+-------------------+------------+---------+
|56551|2023-12-31 14:38:47|2024-01-31 23:57:29|1.79        |201421.68|
+-----+-------------------+-------------------+------------+---------+

+---------------------------+---------+-----+
|pickup_zone                |borough  |trips|
+---------------------------+---------+-----+
|East Harlem North          |Manhattan|12141|
|East Harlem South          |Manhattan|8458 |
|Forest Hills               |Queens   |2915 |
|Central Park               |Manhattan|2765 |
|Morningside Heights        |Manhattan|2649 |
|Elmhurst                   |Queens   |2623 |
|Central Harlem             |Manhattan|2589 |
|Fort Greene                |Brooklyn |1898 |
|Downtown Brooklyn/MetroTech|Brooklyn |1624 |
|Astoria                    |Queens   |1521 |
+---------------------------+---------+-----+

+------------+-----+---------+
|payment     |trips|tip_share|
+------------+-----+---------+
|Credit card |36660|0.226    |
|Cash        |15913|0.0      |
|Not recorded|3415 |0.11     |
|No charge   |434  |0.003    |
|Dispute     |128  |0.0      |
|Unknown     |1    |0.0      |
+------------+-----+---------+

```

<!-- sail-output:end -->

## The query to keep

Once the script has named the two files `trips` and `zones`, this is the query
that answers the question:

```sql
SELECT z.Zone AS pickup_zone, z.Borough AS borough, count(*) AS trips
FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
GROUP BY z.Zone, z.Borough
ORDER BY trips DESC, pickup_zone
LIMIT 10
```

Sail reads the files, joins the rows and computes the counts. You write ordinary
Spark SQL; the Python script starts the local engine and prints the results.

## Try one change: only Queens

[quickstart/query-files-with-sql/main.py](main.py), `top_zones`:

```diff main.py variant=queens
     top_zones = spark.sql("""
         SELECT z.Zone AS pickup_zone, z.Borough AS borough, count(*) AS trips
         FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
+        WHERE z.Borough = 'Queens'
         GROUP BY z.Zone, z.Borough
         ORDER BY trips DESC, pickup_zone
         LIMIT 10
     """)
```

From the repository root (`sail-examples/`), run the edited script:

```bash
uv run quickstart/query-files-with-sql/main.py
```

### Output after this change

<!-- sail-output:start variant=queens -->

Current output · Sail 0.7.3.

```text
+-----+-------------------+-------------------+------------+---------+
|trips|first_pickup       |last_pickup        |median_miles|max_miles|
+-----+-------------------+-------------------+------------+---------+
|56551|2023-12-31 14:38:47|2024-01-31 23:57:29|1.79        |201421.68|
+-----+-------------------+-------------------+------------+---------+

+----------------+-------+-----+
|pickup_zone     |borough|trips|
+----------------+-------+-----+
|Forest Hills    |Queens |2915 |
|Elmhurst        |Queens |2623 |
|Astoria         |Queens |1521 |
|Jackson Heights |Queens |1398 |
|Jamaica         |Queens |1339 |
|Woodside        |Queens |981  |
|Kew Gardens     |Queens |649  |
|Elmhurst/Maspeth|Queens |523  |
|Flushing        |Queens |380  |
|Steinway        |Queens |370  |
+----------------+-------+-----+

+------------+-----+---------+
|payment     |trips|tip_share|
+------------+-----+---------+
|Credit card |36660|0.226    |
|Cash        |15913|0.0      |
|Not recorded|3415 |0.11     |
|No charge   |434  |0.003    |
|Dispute     |128  |0.0      |
|Unknown     |1    |0.0      |
+------------+-----+---------+

```

<!-- sail-output:end -->

## How it works

1. **Read a file directly.** ``FROM parquet.`path` `` queries a Parquet file without an import step.
2. **Give files names.** Temporary views let later queries refer to `trips` and `zones`. The CSV view declares its types and skips the header.
3. **Join and summarize.** A pickup-zone ID connects each trip to a place name. Grouping counts the trips there.
4. **Compare payment types.** The script translates payment codes into names and calculates recorded tip shares. The source records card tips but excludes cash tips, so a zero cash-tip share does not mean people never tip in cash.

## Use your own files

Change the input paths. A path can be a folder or a glob such as
`data/2024-*.parquet` to read several files as one table.

The preview counts the supplied records as they are. It does not remove implausible
trips. The [taxi ETL guide](../../pipelines/taxi-etl-job/README.md) shows how to
validate records and retain rejects.

## Sail and Spark details

Declaring CSV types keeps results consistent between Sail and Apache Spark.
With `inferSchema`, Sail reads whole-number columns as `BIGINT`, while Spark reads
them as `INT`. A direct ``csv.`path` `` query keeps the header as a data row;
use the declared view shown in the script for this file.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
green taxi trips for January 2024 and the taxi zone lookup. Field definitions
are in the [green taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf).
The preview shows the `top_zones` output from the pinned example.
