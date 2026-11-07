---
title: Normalize nested order events without losing rejected records
id: normalize-order-events
description: Use when a bounded JSONL snapshot needs strict order and child validation, separate clean order and line tables, and original rejected records with source identity.
tier: pipelines
level: intermediate
tags: [jsonl, nested-data, validation, quarantine, parquet, mapInArrow]
modality: tabular
engines: both
ordered: true
float_tolerance: 0.0
timeout: 120
needs_key: false
needs_gpu: false
prompt: On Sail, read a bounded JSONL snapshot through a source manifest, validate schema version 1 without coercing scalar types, reject an entire order if any child is invalid, quarantine repeated order IDs, and produce separate order and line Parquet tables. Retain every rejected physical record, exact source bytes and deterministic reasons. Preserve empty item arrays as zero-line orders and keep unknown additive fields in original evidence.
---

# Normalize nested order events without losing rejected records

Read JSONL files, validate complete orders, then use Spark SQL and DataFrame code
on Sail to separate order headers from line items. Every rejected occurrence
keeps its original bytes, source filename, source SHA-256 and physical line
number. This makes an invalid child's effect on its whole order visible.

## Run it

Follow the [repository setup](../../README.md#run-an-example), then start from
the repository root:

```bash
cd pipelines/normalize-order-events
uv run main.py
```

Run the remaining commands from this example directory.

The input is an authored [15-record JSONL snapshot](inputs/orders.jsonl).
The program copies it into temporary input files, reads those actual files
inside `mapInArrow`, writes typed Parquet outputs and reads them back. The fixture
needs no data download, key or GPU. uv installs the pinned packages on first run.
The default run prints the `orders`, `lines` and `rejects` tables: three clean
orders, three clean line items and twelve rejected source occurrences.

For a compact count check:

```bash
uv run main.py --json
```

Expected output:

```json
{"lines": 3, "orders": 3, "rejects": 12}
```

For a larger local check, create independent copies with different order IDs:

```bash
uv run main.py --copies 100 --json
```

This processes 1,500 physical records and returns counts of 300 orders, 300 line
items and 1,200 rejects. The limit is 1,000 copies. This exercises repeated file
ingestion and normalization, not production throughput or arbitrary event data.

To read your own files instead, supply one or more `--input` arguments:

```bash
uv run main.py --input orders.jsonl --input more-orders.jsonl --output-dir normalized-orders --json
```

These files must follow the supported contract below. `--input` cannot be
combined with `--copies`. `--output-dir` retains the complete `orders/`, `lines/`
and `rejects/` Parquet datasets after process exit. The directory must not already
exist. Writes are not an atomic publication protocol: failure can leave partial
output, so inspect it and choose a new directory when retrying. Without this
option, output datasets are temporary and removed at process exit.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
orders
+----------------+----------------------------------------------------------------+-----------+------------+-----------+--------+----------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+
|       source_id|                                                   source_sha256|line_number|    order_id|customer_id|currency|item_count|gross_minor|                                                               raw_record_base64|                                                                        raw_json|
+----------------+----------------------------------------------------------------+-----------+------------+-----------+--------+----------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          1|ORDER-A-0000| CUSTOMER-1|     USD|         2|       3000|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1BLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-A-0000","customer_id":"CUSTOMER-1","cur...|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          2|ORDER-B-0000| CUSTOMER-2|     GBP|         0|          0|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1CLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-B-0000","customer_id":"CUSTOMER-2","cur...|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         15|ORDER-Z-0000| CUSTOMER-1|     USD|         1|          0|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1aLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-Z-0000","customer_id":"CUSTOMER-1","cur...|
+----------------+----------------------------------------------------------------+-----------+------------+-----------+--------+----------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+

lines
+----------------+----------------------------------------------------------------+-----------+------------+-------+------+--------+----------------+-----------+
|       source_id|                                                   source_sha256|line_number|    order_id|line_id|   sku|quantity|unit_price_minor|gross_minor|
+----------------+----------------------------------------------------------------+-----------+------------+-------+------+--------+----------------+-----------+
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          1|ORDER-A-0000|     L1|WIDGET|       2|            1250|       2500|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          1|ORDER-A-0000|     L2|   BAG|       1|             500|        500|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         15|ORDER-Z-0000|   FREE|SAMPLE|       1|               0|          0|
+----------------+----------------------------------------------------------------+-----------+------------+-------+------+--------+----------------+-----------+

rejects
+----------------+----------------------------------------------------------------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+-----------------------------------+
|       source_id|                                                   source_sha256|line_number|                                                               raw_record_base64|                                                                        raw_json|                            reasons|
+----------------+----------------------------------------------------------------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+-----------------------------------+
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          3|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1DLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-C-0000","customer_id":"CUSTOMER-1","cur...|        [items[0].invalid_quantity]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          4|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1ELTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-D-0000","customer_id":"CUSTOMER-1","cur...|       [items[1].duplicate_line_id]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          5|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1FLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-E-0000","customer_id":"CUSTOMER-1","cur...|                    [missing_items]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          6|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1GLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-F-0000","customer_id":"CUSTOMER-1","cur...|                       [null_items]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          7|eyJzY2hlbWFfdmVyc2lvbiI6Miwib3JkZXJfaWQiOiJPUkRFUi1HLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":2,"order_id":"ORDER-G-0000","customer_id":"CUSTOMER-1","cur...|       [unsupported_schema_version]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          8|                eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1ILTAwMDAiLAo=|                                {"schema_version":1,"order_id":"ORDER-H-0000",\n|                   [malformed_json]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|          9|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1JLTAwMDAiLCJvcmRlcl9pZCI6I...|{"schema_version":1,"order_id":"ORDER-I-0000","order_id":"OTHER-0000","custom...|            [duplicate_json_member]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         10|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1KLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-J-0000","customer_id":"CUSTOMER-1","cur...|[items[0].invalid_unit_price_minor]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         11|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1YLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-X-0000","customer_id":"CUSTOMER-1","cur...|               [duplicate_order_id]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         12|eyJzY2hlbWFfdmVyc2lvbiI6MSwib3JkZXJfaWQiOiJPUkRFUi1YLTAwMDAiLCJjdXN0b21lcl9pZ...|{"schema_version":1,"order_id":"ORDER-X-0000","customer_id":"CUSTOMER-1","cur...|               [duplicate_order_id]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         13|                                                                            W10K|                                                                            []\n|             [top_level_not_object]|
|orders-000.jsonl|baffe6dcc7fc6ae30fa2c561e323c1daa1de9180f5c4b1a13a5277a4ce166783|         14|                                                                            Cg==|                                                                              \n|                     [empty_record]|
+----------------+----------------------------------------------------------------+-----------+--------------------------------------------------------------------------------+--------------------------------------------------------------------------------+-----------------------------------+

```

<!-- sail-output:end -->

## The supported contract

Only integer `schema_version: 1` is supported. Missing, null, boolean, string and
other versions are rejected. The required header fields are `order_id`,
`customer_id`, `currency` and `items`. IDs use 1 to 64 ASCII letters, digits,
underscores or hyphens, beginning with a letter or digit. Currency is explicitly
`USD` or `GBP`; no exchange rates or cross-currency totals are computed.

Each child requires `line_id`, `sku`, integer `quantity` from 1 to 1,000,000 and
integer `unit_price_minor` from 0 to 1,000,000,000. Booleans, numeric strings and
fractional numbers are not coerced to integers. Line IDs must be unique inside
an order; the same SKU on different line IDs is allowed. There are at most 100
children. These limits keep exact integer multiplication and per-order sums
within signed 64-bit range. Zero-price items are accepted.

Missing `items`, null `items`, non-array `items`, invalid children and repeated
child IDs reject the whole order. An empty array is deliberately accepted as an
order with zero children and gross total zero. This is a normalization contract,
not a claim that an order was paid, fulfilled or valid under a business policy.

Duplicate object members at any nesting level, nonstandard `NaN`/`Infinity`
constants, malformed JSON, non-object roots, blank records and invalid UTF-8 are
rejected. Numbers beyond the parser's decimal representation produce an explicit
`numeric_representation_limit` reject, including in unknown fields. Integer
tokens are limited to 4,300 digits before field-specific limits are applied. Validation
emits deterministic ordered reasons for the fields it can
inspect. Syntax, encoding and structural failures produce a single boundary
reason. Unknown additive fields are allowed and retained in the original
evidence; they do not silently become modeled columns. Adding another schema
version requires a new explicit contract.

## Separate parent and child tables

Start with `normalize()` in [main.py](main.py) for the DataFrame transformation.
`run()` handles file ingestion and output readback; `parse_record()` defines the
strict record-validation policy.

1. A manifest has a logical filename, SHA-256 and worker-readable path per file.
   Each physical LF-delimited record keeps its 1-based line number. LF or CRLF
   terminators remain in `raw_record_base64`, including malformed records.
   `raw_json` contains the original UTF-8 text when decoding was attempted and
   succeeded. Base64 remains the authoritative byte evidence.
2. A bounded Python parser validates the complete order inside `mapInArrow`.
   Parsed rows are persisted before later actions. No invalid order contributes
   a child to the clean tables.
3. Under this fixed-snapshot policy, every repeated `order_id` among otherwise
   valid records is quarantined, including exact repeats and repeats across
   files. Invalid records retain their validation reasons and do not reserve an
   order ID. This is not replay handling or change-data capture.
4. The DataFrame transform explodes accepted children once. It aggregates line
   totals to source-occurrence/order keys before joining them to headers. The
   left join preserves accepted empty orders without inventing a child. Gross
   is quantity times unit price only; taxes, shipping and discounts are not
   included by this contract.
5. Orders, lines and rejects are written to separate Parquet datasets and read
   back with explicit schemas. Typed empty datasets are created when required.
   The program checks that every physical source occurrence appears exactly
   once in orders or rejects. Lines reference their accepted parent occurrence.

The source manifest supports at most 100 files with unique basenames, each at
most 1 MiB and 10,000 physical records. An unreadable file, file above either
limit or changed source SHA fails
the job; the program does not pretend it ingested a partial file. Individual
records above 64 KiB, including their LF or CRLF terminator when present, are
rejected with complete byte evidence, within the file
limit. Input paths must be readable by workers. The generated demonstration
uses temporary local paths and removes them when the process exits.

This is bounded snapshot normalization, not a general schema-evolution system,
transactional publication protocol or distributed storage example. Source data,
including unknown fields, is retained in clean-order evidence and reject
evidence; apply appropriate access controls and retention to real inputs.

## Verification

Maintenance checks compare both engines against an independently assembled
reference: authored literal accept/reject classifications, literal expected
order and line counts, and separate Python integer arithmetic. They check exact
original bytes, additive fields, duplicate barriers, invalid child types,
malformed JSON, empty inputs and typed output readback. The reference does not
call the production parser to decide whether an input is accepted.
