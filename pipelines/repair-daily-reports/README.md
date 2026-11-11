---
title: Repair daily reports after corrected order records
id: repair-daily-reports
facts: [delta-lake-support, cache-does-not-keep-results]
summary: Repair past sales totals when orders are corrected or deleted.
description: Use when corrected orders, cancellations and deletes change already reported store totals, and a retry must also repair the old store and date after an interrupted source update.
tier: pipelines
level: advanced
tags: [delta, merge, incremental, recovery, data-quality, aggregation]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
spark_extras: [delta]
prompt: Maintain versioned order replacements in Delta, retain a pending batch with old and new store/date groups before the source commit, repair affected daily totals from current state, and recover an interrupted update against an independent full rebuild.
---

# Repair daily reports after corrected order records

Keep a daily store report aligned with corrected order records. An order moved to
a different store or business date must disappear from its old group as well as
appear in its new group. If the process stops after updating orders, replay alone
can lose the old group. This example saves that information before either table
changes.

## Run it

This example requires a POSIX system and a local directory for its file lock.
Follow the [repository setup](../../README.md#your-first-result), then run all commands below from the repository root (`sail-examples/`):

```bash
uv run pipelines/repair-daily-reports/main.py
```

The example creates temporary Delta tables, applies three batches and replays the
last batch. It prints `orders` and `daily`. Initially north has two orders totaling
1500 minor units, and south has one totaling 700. A correction moves an order to
south on the next date, a cancellation clears north's old group, and a delete
clears south's old group. A later batch restores the cancelled order with a higher
version while an older correction is ignored. Final daily rows contain 600 for
north and 1200 for south, both on `2026-01-02`.

Each final daily row has `order_count=1`. The `orders` table contains active
order `a` at version 2, active order `b` at version 3, and a retained deleted row
for order `c` at version 2. Temporary state is removed on normal process exit.

To follow [pipelines/repair-daily-reports/main.py](main.py), start with `apply_batch()` for orchestration,
`prepare()` and `affected_groups()` for accepted changes and old/new report
keys, then `recover_locked()` for the source MERGE and report repair. `run()`
supplies the small demonstration batches.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
orders
+--------+--------------+-------+--------+-------------+------------+
|order_id|source_version|status |store_id|business_date|amount_minor|
+--------+--------------+-------+--------+-------------+------------+
|a       |2             |active |south   |2026-01-02   |1200        |
|b       |3             |active |north   |2026-01-02   |600         |
|c       |2             |deleted|NULL    |NULL         |NULL        |
+--------+--------------+-------+--------+-------------+------------+

daily
+--------+-------------+-----------+------------+
|store_id|business_date|order_count|amount_minor|
+--------+-------------+-----------+------------+
|north   |2026-01-02   |1          |600         |
|south   |2026-01-02   |1          |1200        |
+--------+-------------+-----------+------------+

```

<!-- sail-output:end -->

## Input and version contract

Each batch is a JSON object with `batch_id` and `records`. Each record contains
exactly these fields:

```json
{
  "batch_id": "orders-17",
  "records": [
    {"order_id":"order-8","source_version":2,"operation":"upsert","status":"active","store_id":"north","business_date":"2026-01-02","amount_minor":1200}
  ]
}
```

- Versions are positive source-issued integers, not ingestion times. A higher
  version replaces the entire order. Lower versions are ignored. Equal versions
  with identical values replay; equal versions with different values reject the
  entire new batch before its journal or table changes. Previously pending work
  is recovered first, which can change the tables before the new batch is checked.
- Upserts require `active` or `cancelled`, a store, a valid `YYYY-MM-DD` date and
  a nonnegative integer amount. Deletes require null status, store, date and
  amount. Deleted rows remain as version barriers. A higher version may restore
  a cancelled or deleted order.
- The report counts and sums active orders only. Amounts use minor units in one
  currency chosen by the caller. There is no currency conversion, revenue
  recognition or accounting treatment. Dates are supplied business dates; the
  example does not derive them from timestamps or time zones.
- Each batch contains at most 512 records and at most one distinct replacement
  per order. Exact repeated records collapse. Order does not affect batch identity.
  All fields are required; unknown fields reject. IDs use ASCII letters, digits,
  dots, underscores and hyphens, start with a letter or digit, and have at most
  80 characters. Versions and amounts are capped at one billion. At most 20,000
  order rows, including retained deletes, are allowed in the workspace.
- A batch ID remains bound to its normalized content after acknowledgement.
  Reusing the ID with different content rejects. Keep receipt files for as long
  as the batch IDs can be retried.

## Why the pending journal matters

One local POSIX writer lock covers recovery and application. For a new batch, the
example reads only matching order preimages into Python, checks versions, and
atomically publishes `pending.json`. That immutable pending body retains the full
normalized batch, its digest, changed old/new order images, and both sets of daily
keys. It is published before the source MERGE.

The source and report then commit separately. Report repair reads current orders,
recomputes only affected store/date groups with DataFrame aggregation, and MERGEs
their totals. A group with no active orders is deleted from the report. It does
not subtract remembered totals or increment a replayed amount.

Recovery always finishes pending work before accepting a new batch. It verifies
the workspace identity, journal integrity and stored source images. A source row
must match either its recorded old image or its intended replacement. The old
daily keys remain available even when source changes already committed. Recovery
repairs those groups again, atomically publishes the batch receipt, and removes
the pending file. A process stopped after receipt publication can safely finish
that final removal.

This coordinates two table commits; it is not a transaction spanning them.
Between the source and report commits, direct readers can see different stages.
Readers that require a completed pair must acquire the same writer lock and
require no pending batch. The returned DataFrames and console are ordinary reads,
not a cross-table snapshot against a concurrent later writer.

The recovery boundary covers process interruption between completed operations,
including uncertain acknowledgement of a committed source or report. It does not
promise power-loss durability, arbitrary failures inside the storage system,
distributed writer coordination or safety from direct table/file edits. Use an
owned local POSIX directory and this writer for all mutations. Invalid workspace
metadata, malformed receipts and pending-journal integrity failures are rejected.
An initialized workspace rejects missing or empty Delta transaction logs. A new workspace records its
initialization phase so interruption while creating the two empty tables can be
resumed. Do not delete a pending journal to bypass
recovery or prune order tombstones without a separate retention policy.

## Retain state across commands

Save the complete JSON object above as `pipelines/repair-daily-reports/orders.json`, then run:

```bash
uv run pipelines/repair-daily-reports/main.py \
  --state-dir pipelines/repair-daily-reports/.data/order-report \
  --batch pipelines/repair-daily-reports/orders.json
uv run pipelines/repair-daily-reports/main.py \
  --state-dir pipelines/repair-daily-reports/.data/order-report \
  --recover
```

The first command retains `orders` and `daily` Delta tables, the workspace
identity, and batch receipts. Subsequent batch commands also recover pending work
first. `--recover` needs no input batch. Input JSON files are limited to four
million bytes. Console output previews 40 rows; the retained tables contain the
full bounded result.

An independent Python event fold rebuilds the complete report for verification.
Checks compare the tables after corrections and after interruptions before and
after each table commit and after acknowledgement, including recovery in a new
process after the source commit. They cover stale deletes, batch-content reuse,
empty groups, an unchanged unrelated group, and the writer lock. Source capture,
streaming and scheduling are outside this example.

## Optional larger qualification

```bash
uv run pipelines/repair-daily-reports/main.py \
  --state-dir pipelines/repair-daily-reports/.data/report-qualification \
  --qualify-batches 18
```

The state directory `pipelines/repair-daily-reports/.data/report-qualification/` must be new and empty. This optional longer run loads 18 batches of 512
orders, for 9,216 retained source rows in one skewed store/date group, then
applies a correction batch with moves, cancellations and deletes. It interrupts
after that source commit and recovers from the journal. The final tables must
equal the independent full rebuild, both Delta tables must have checkpoint
markers, and no pending batch may remain. It prints a JSON qualification summary.
The option accepts 12 to 24 load batches and is separate from the short default.
