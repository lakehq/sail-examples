---
title: Reproduce a report from saved Iceberg snapshots
id: reproduce-iceberg-report
facts: [iceberg-support, default-catalog-name]
summary: Recreate an earlier report after its source tables have changed.
description: Use when a sales report joins changing Iceberg tables and you need to record its exact input snapshot pair, then replay the original logical rows in another process.
tier: analytics
level: advanced
tags: [iceberg, reports, snapshots, reproducibility, joins, data-quality, sail-only]
modality: tabular
engines: sail-only
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Build a sales report on Sail from local Iceberg orders and product lookup tables. Save both table identities and snapshot IDs, schemas, report parameters, query and code hashes, runtime versions and the original results. Append, update and delete source rows, change a product category, and replay the original report in a fresh process. Reject changed code, a different table or missing snapshot history instead of reading current data.
---

# Reproduce a report from saved Iceberg snapshots

A report can change when either the facts or a lookup table changes. This example saves an explicitly selected pair of Iceberg snapshots for an orders-and-products report, changes both source tables, then reproduces the original logical rows. Sail performs the table writes, joins, aggregations and snapshot reads.

The data is a small generated fixture. Each order contains its original unit price in USD cents. The product table supplies its category. The report counts orders and sums units and sales by category for January 2026, including January 1 and excluding February 1. It compares complete output rows, not only row counts or totals across all categories.

The `QUERY` constant near the top of [analytics/reproduce-iceberg-report/main.py](main.py) contains the report SQL. `rows_at()` reads each selected snapshot before executing it.

## Run it

This example requires a POSIX system and a local directory for its file lock.
Follow the [repository setup](../../README.md#your-first-result), then run all commands below from the repository root (`sail-examples/`):

```bash
uv run analytics/reproduce-iceberg-report/main.py
```

The default demonstration uses temporary local tables and prints the original, current and replayed reports. The fixture needs no data downloads, credentials or service setup; uv installs the pinned packages on first run. Temporary state is removed on normal process exit.

To retain the state and replay across separate program invocations, run these commands from the repository root (`sail-examples/`):

```bash
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report \
  --phase prepare
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report \
  --phase change
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report \
  --phase replay
```

`prepare` requires a new directory. It creates the tables and saves `report-manifest.json`, its checksum and `original-report.json`. `change` appends an order, updates an existing order, deletes another and moves a product to a different category. It records the resulting snapshot IDs and current report in `changes.json`. `replay` opens the saved snapshot IDs and verifies exact equality with the original report.

The original report has Accessories with one order, three units and 2,400 cents, and Components with two orders, three units and 5,500 cents. The changed report has Components with one order, three units and 4,500 cents, and Hardware with two orders, three units and 7,500 cents. Replaying the saved pair restores the original two rows.

For a larger optional run, use the new state directory
`analytics/reproduce-iceberg-report/.data/sales-report-larger/`:

```bash
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report-larger \
  --phase prepare \
  --copies 3000
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report-larger \
  --phase change
uv run analytics/reproduce-iceberg-report/main.py \
  --state analytics/reproduce-iceberg-report/.data/sales-report-larger \
  --phase replay
```

This starts with 12,000 orders whose IDs are independent across copies. Report counts and monetary totals scale with the copy count. The bound is 1 to 5,000 copies; `--copies` applies only to preparation.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
original
+-----------+------+-----+-----------+
|category   |orders|units|sales_minor|
+-----------+------+-----+-----------+
|Accessories|1     |3    |2400       |
|Components |2     |3    |5500       |
+-----------+------+-----+-----------+

current
+----------+------+-----+-----------+
|category  |orders|units|sales_minor|
+----------+------+-----+-----------+
|Components|1     |3    |4500       |
|Hardware  |2     |3    |7500       |
+----------+------+-----+-----------+

replayed
+-----------+------+-----+-----------+
|category   |orders|units|sales_minor|
+-----------+------+-----+-----------+
|Accessories|1     |3    |2400       |
|Components |2     |3    |5500       |
+-----------+------+-----+-----------+

```

<!-- sail-output:end -->

## What the manifest binds

The saved manifest records both inputs, rather than using a saved orders snapshot with the current product lookup:

- Canonical local table paths, Iceberg table UUIDs and selected snapshot IDs.
- The metadata file and checksum that described each selected snapshot, with its schema and field IDs.
- The SQL template and complete script hashes, date window, currency and explicit session configuration.
- Python, Sail, Python client and Arrow versions, plus the saved result checksum.

Before replay, the program checks these values and requires the selected snapshots to remain in each table's current history. It also checks that the selected schemas are unchanged. A different table at the same path, missing metadata or snapshot history, changed code, changed parameters, changed runtime or altered saved results rejects replay. The program never substitutes a current snapshot. Missing underlying data or manifest files cause the actual snapshot read to fail.

Checksums detect disagreement with the saved files. They do not authenticate state supplied by another party. The state directory is trusted local input and is bound to its canonical path, so moving or copying it elsewhere requires preparing a new state.

## Scope of the local protocol

Both table versions are selected deliberately after the initial fixture writes. This pair is a report input selection, not a coordinated transaction or business-time snapshot across independently updated tables.

This example uses only its own local, single-writer tables. Its metadata resolver requires `metadata/version-hint.text` and the exact `v<N>.metadata.json` file selected by that hint. It rejects missing or unsupported hints and checks for metadata changes during report execution. It does not pick the largest filename in a directory or implement a general catalog resolver. For a catalog-backed table, obtain the committed metadata location from that catalog.

All three commands take the same local state lock. An external writer that does not use that lock is outside this protocol. A change-started marker prevents retrying partially applied mutations. If a change fails, inspect that state or prepare a new directory; the program does not claim atomic multi-table mutation or automatic recovery. The saved original report can still be replayed when its required history and files remain available.

Saving snapshot IDs does not pin metadata or data files or prevent expiration. Per the [Apache Iceberg maintenance documentation](https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots), snapshot expiration removes the ability to time travel to expired snapshots and can delete files no longer needed by retained snapshots. Arrange retention for the required table history and files separately. The manifest is not an expiration policy.

## How it is checked

`reference()` returns independent literal expectations with explicit Arrow types. The harness compares all three complete reports against it. Focused checks cover every append, update and delete snapshot, changed table identity, changed metadata and manifest fields, missing history, invalid scalar types, and preparation, mutation and replay in separate processes.

This example is checked on Sail against those independent results. The maintenance harness does not qualify its Iceberg path on Apache Spark 4.2. The example does not establish cluster, remote catalog or concurrent-writer behavior.
