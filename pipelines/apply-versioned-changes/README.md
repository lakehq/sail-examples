---
title: Apply replayed changes without resurrecting deleted records
id: apply-versioned-changes
description: Use when captured change batches contain retries, stale updates and deletes, and you need a current-state table that retains version barriers and quarantines conflicting equal-version payloads.
tier: pipelines
level: advanced
tags: [delta, merge, idempotent, tombstones, data-quality, change-batches]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
spark_extras: [delta]
prompt: Apply versioned replacement records to a Delta table, reduce each batch to one candidate per entity, retain tombstones after deletes, quarantine equal-version conflicts, and verify that replays and stale updates preserve current state against an independent event fold.
---

# Apply replayed changes without resurrecting deleted records

Maintain current entity values from already captured change batches. A source
supplies a positive integer version for each entity. An older update must not
restore an entity after a newer delete, and a retry must not resolve a conflicting
version by choosing whichever payload arrives first.

## Run it

```bash
uv run main.py
```

The default applies four small batches, replays the last batch and then replays
an old batch. It prints current state and per-input outcomes. Its local state is
temporary and is removed when the process exits.

## The contract

Each input has `change_id`, `entity_id`, `source_version`, `operation` and `value`.
Versions are source-issued positive integers, not event timestamps. Upserts
replace the whole string value. Deletes require a null value. `change_id` is a
diagnostic label, not a delivery checkpoint or a global deduplication key.

- Invalid keys, versions, operations or payloads are rejected with their inputs.
- The highest valid version per entity in a batch supplies one MERGE candidate.
  Older valid input versions receive `superseded_in_batch`.
- A higher version replaces stored state. A lower version is `stale`.
- An equal version with the same operation/value is `duplicate`.
- Distinct payloads at the same version produce `conflict`. The stored payload
  becomes null, and the entity is excluded from the active view. Replaying either
  payload cannot clear the conflict. A strictly newer version can resolve it.
- A delete inserts or retains a `deleted` row, including for an unknown entity.
  This tombstone retains the version needed to reject a later stale update.

Exact repeated tuples inside one batch collapse to one candidate. Their original
input occurrences remain in diagnostics. `accepted` means an input contributes
to an accepted candidate; it does not count individual physical table writes.

The final table has four active entities and one retained tombstone. After the
second batch, two entities are quarantined as conflicts. A plain Python
event-by-event fold checks each checkpoint and the final values. Diagnostic
outcomes for the fixed batches have independent expected values.

## Why materialize before MERGE

The input is materialized once so decisions and diagnostics use the same rows.
The decision table compares incoming candidates with the old state. It is written
to temporary Parquet before MERGE, along with diagnostics. Re-reading these
files prevents later lazy actions from recalculating decisions against the new
state. A local filesystem lock excludes another writer using this example's
state path.

The Delta state table is authoritative. Diagnostics are a separate output, not
part of the same transaction. An application failure after commit but before
acknowledgement can require replay. No exactly-once delivery or external-side-
effect guarantee follows from this example.

## Apply your own batch

Create a JSON Lines file with the five input fields, for example:

```json
{"change_id":"delivery-17","entity_id":"account-8","source_version":2,"operation":"upsert","value":"gold"}
{"change_id":"delivery-18","entity_id":"account-9","source_version":4,"operation":"delete","value":null}
```

```bash
uv run main.py --changes changes.jsonl --state-dir .data/customer-state
```

The command retains the Delta state across invocations. Complete per-run
outcomes are written to a unique Parquet
directory beside the state, under `customer-state-outcomes`; `--outcomes-dir`
chooses another root. The console is a preview. This separate export happens
after the state commit, so an export failure can leave committed state without
that run's outcome files; replay is still needed to obtain a new outcome report.

Input must fit the declared string/integer schema; malformed JSON or unrepresentable values fail
the read. Rule rejections apply to rows read successfully. Use one writer and
a local POSIX filesystem. Retained tombstones and conflict records must remain
in the authoritative table even though an application queries only active rows.

This applies captured replacement changes. Source capture, streaming, arbitrary
mid-commit crash recovery and distributed writer coordination are outside its
scope. Diagnostic traces can differ when batches are regrouped; current state
follows the highest-version/conflict contract.
