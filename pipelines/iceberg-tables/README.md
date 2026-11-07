---
title: Manage an Apache Iceberg table and read old snapshots
id: iceberg-tables
description: Use when you keep data in an Apache Iceberg table on Sail and need to create it with a declared partitioned schema, insert, update, delete and merge rows, and read any earlier snapshot by its id.
tier: pipelines
level: advanced
tags: [iceberg, table-format, snapshots, time-travel, merge, update, delete, nyc-taxi, sail-only]
modality: tabular
engines: sail-only
ordered: true
float_tolerance: 1e-9
timeout: 180
needs_key: false
needs_gpu: false
prompt: Create an Apache Iceberg table of daily taxi trips and fares per borough partitioned by month on Sail, insert January and February 2024 NYC green taxi data, update and delete rows, merge a late correction, then read every snapshot by its id and compare with the row counts in the table's metadata.
---

# Manage an Apache Iceberg table and read old snapshots

Apache Iceberg keeps a table as data files plus metadata that records every change as a snapshot. This example creates an Iceberg table on Sail and takes it through inserts, an update, a delete and a merge. It then reads each snapshot back by its id. The table holds daily taxi trips and fares per pickup borough for January and February 2024, built from real NYC green taxi records.

## Run it

```bash
uv run main.py
```

The first run downloads two months of trip records and the zone lookup (2.7 MB) into `.data/` next to the script. Sail reads and writes Iceberg without extra packages.

The table goes through five snapshots. The first holds January's 182 rows, and the current one holds 293. Every snapshot, read back by its id, has the row count the metadata recorded for it.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
snapshots
+--------+---------+-------------+
|snapshot|rows_read|rows_recorded|
+--------+---------+-------------+
|1       |182      |182          |
|2       |353      |353          |
|3       |353      |353          |
|4       |293      |293          |
|5       |293      |293          |
+--------+---------+-------------+

by_borough
+-------------+-----+----+
|borough      |trips|days|
+-------------+-----+----+
|Bronx        |1599 |60  |
|Brooklyn     |15372|60  |
|EWR          |5    |5   |
|Manhattan    |64544|60  |
|Outside NYC  |66   |41  |
|Queens       |28493|60  |
|Staten Island|7    |7   |
+-------------+-----+----+

february_29
+-----+-----------+-----+--------+
|month|borough    |trips|fares   |
+-----+-----------+-----+--------+
|2    |Bronx      |28   |752.23  |
|2    |Brooklyn   |308  |7364.59 |
|2    |EWR        |1    |144.0   |
|2    |Manhattan  |1500 |20000.0 |
|2    |Outside NYC|2    |63.41   |
|2    |Queens     |618  |11672.17|
+-----+-----------+-----+--------+

```

<!-- sail-output:end -->

## How it works

| Snapshot | Change | SQL |
|---|---|---|
| 1, 2 | Insert January, then February | `INSERT INTO daily_trips SELECT ... GROUP BY ...` |
| 3 | Relabel `N/A` as `Outside NYC` | `UPDATE ... SET borough = ... WHERE ...` |
| 4 | Remove the `Unknown` borough | `DELETE FROM ... WHERE ...` |
| 5 | A late correction for February 29 | `MERGE INTO ... WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...` |

- **Declare the table.** `CREATE TABLE ... (columns) USING iceberg PARTITIONED BY (month) LOCATION ...` writes the table's first metadata file at that path.
- **Read old snapshots.** The table's newest `metadata/v<N>.metadata.json` lists every snapshot with its id and a summary. `spark.read.format("iceberg").option("snapshot-id", id)` reads one, and `SELECT ... FROM t VERSION AS OF <id>` does the same in SQL.
- **Merge with explicit columns.** The correction has no `month` column, so the `MERGE` assigns columns by name instead of using `UPDATE SET *`.

## How it is checked

Apache Iceberg does not yet publish a Spark runtime for Apache Spark 4.2, the version these examples are checked against, so this example is checked on Sail only. `reference()` computes the same snapshot row counts and final table with pyarrow alone, from the same files, and the checks compare Sail's results with it.

## On Sail

- This example reads snapshot ids from the table's metadata files. Iceberg's metadata tables, such as `daily_trips.snapshots` and `daily_trips.history`, list them too.
- Sail lists the partition column last in the table's schema. Select columns by name rather than by position.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January and February 2024, and the taxi zone lookup table.
