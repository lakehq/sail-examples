---
title: Manage a Delta Lake table through its life
id: delta-table-lifecycle
description: Use when you maintain a Delta Lake table over time (declare it, append, add a column, rebuild one partition, update, delete, merge late corrections) and want to check time travel across the versions this example creates.
tier: pipelines
level: advanced
tags: [delta-lake, schema-evolution, merge, update, delete, replace-where, time-travel, nyc-taxi]
modality: tabular
engines: both
spark_extras: [delta]
ordered: true
float_tolerance: 1e-9
timeout: 240
needs_key: false
needs_gpu: false
prompt: Create a Delta Lake table of daily taxi trips per borough partitioned by month, append three months of NYC green taxi data, add a tips column with mergeSchema, rebuild one month with replaceWhere, update and delete rows, merge late corrections, then read every version with versionAsOf and compare a day before and after the corrections.
---

# Manage a Delta Lake table through its life

A table changes as data arrives, a new column appears, a month gets rebuilt, labels are fixed, rows are removed, and late corrections come in. This example takes a Delta Lake table through each of those changes and reads all eight versions it creates. The table holds daily trips per pickup borough, built from real NYC green taxi records for January to March 2024.

## Run it

```bash
uv run main.py
```

The first run downloads three months of trip records and the zone lookup (4 MB) into `.data/` next to the script. Sail reads and writes Delta Lake without extra packages.

The table ends at version 7. Reading version 3 shows March 31 before the corrections: Manhattan with 740 trips, and the borough labels `N/A` and `Unknown` that later versions relabeled or removed.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
versions
+-------+----+
|version|rows|
+-------+----+
|0      |0   |
|1      |182 |
|2      |353 |
|3      |531 |
|4      |531 |
|5      |531 |
|6      |440 |
|7      |441 |
+-------+----+

by_month
+-----+-----+--------------+
|month|trips|rows_with_tips|
+-----+-----+--------------+
|1    |56437|151           |
|2    |53448|0             |
|3    |57575|148           |
+-----+-----+--------------+

march_31
+---------+-------------+-----+--------+
|version  |borough      |trips|fares   |
+---------+-------------+-----+--------+
|current  |Bronx        |17   |347.64  |
|current  |Brooklyn     |267  |6939.9  |
|current  |Manhattan    |1000 |15000.0 |
|current  |Outside NYC  |1    |69.0    |
|current  |Queens       |446  |8526.5  |
|current  |Staten Island|1    |20.0    |
|version 3|Bronx        |17   |347.64  |
|version 3|Brooklyn     |267  |6939.9  |
|version 3|Manhattan    |740  |10492.56|
|version 3|N/A          |1    |69.0    |
|version 3|Queens       |446  |8526.5  |
|version 3|Unknown      |4    |129.8   |
+---------+-------------+-----+--------+

boroughs
+-------------+
|borough      |
+-------------+
|Bronx        |
|Brooklyn     |
|EWR          |
|Manhattan    |
|Outside NYC  |
|Queens       |
|Staten Island|
+-------------+

```

<!-- sail-output:end -->

## How it works

| Version | Change | How |
|---|---|---|
| 0 | Declare the table | `CREATE TABLE ... (columns) USING delta PARTITIONED BY (month) LOCATION ...` |
| 1, 2 | Append January, then February | `df.write.format("delta").mode("append")` |
| 3 | Append March with a new `tips` column | `.option("mergeSchema", "true")`: older rows read `tips` as NULL |
| 4 | Rebuild January, now with tips | `.mode("overwrite").option("replaceWhere", "month = 1")` replaces only January |
| 5 | Relabel `N/A` as `Outside NYC` | `UPDATE daily_trips SET ... WHERE ...` |
| 6 | Remove the `Unknown` borough | `DELETE FROM daily_trips WHERE ...` |
| 7 | Late corrections for March 31 | `MERGE INTO ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |

- **Time travel.** `option("versionAsOf", v)` reads version `v`, and SQL can do the same with `VERSION AS OF`. The `versions` output counts the rows in each version, and `march_31` puts one day from version 3 next to the current table.
- **Declare first.** Creating the table with its columns, types and partitioning before writing makes the schema a deliberate decision. Columns declared this way are nullable.

## On Apache Spark

The same code runs on Apache Spark with the `delta-spark` package, its session extension (`io.delta.sql.DeltaSparkSessionExtension`) and its catalog (`org.apache.spark.sql.delta.catalog.DeltaCatalog` as `spark_catalog`). The checks run it on Spark 4.2 with `delta-spark` 4.4.1 and compare every result with Sail's.

## On Sail

- On Sail, reads of some older versions after a Delta checkpoint can fail. This example checks versions 0 through 7 before the first checkpoint. Verify reads of the specific versions you need in longer table histories.
- To add a column with an append and `mergeSchema`, do it the way this example does: a table created with `CREATE TABLE ... (columns) USING delta`, then an append whose new column is computed (here `sum(tip_amount)`). `ALTER TABLE ... ADD COLUMNS` also works.
- Sail does not support `RESTORE`. Read old versions with `versionAsOf`. `DESCRIBE HISTORY`, `OPTIMIZE` and `VACUUM` work.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January to March 2024, and the taxi zone lookup table.
