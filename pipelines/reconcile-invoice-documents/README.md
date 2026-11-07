---
title: Reconcile invoice documents against purchase orders
id: reconcile-invoice-documents
facts: [load-files-through-a-path-table, udf-partitions-are-threads]
summary: Read scanned invoices and check them against purchase orders.
description: Use when a small set of known invoice layouts needs real CPU OCR followed by supplier, duplicate, currency and gross purchase-order checks, with explicit exceptions and a structured-input baseline.
tier: pipelines
level: advanced
tags: [invoices, pdf, ocr, reconciliation, data-quality, parquet, mapInArrow]
modality: document
engines: both
ordered: true
float_tolerance: 0.0
timeout: 120
needs_key: false
needs_gpu: false
prompt: On Sail, OCR four pinned controlled-layout invoice PDFs on the CPU, extract supplier identity, invoice number and gross total, persist canonical rows before joins, and reconcile against supplier and purchase-order dimensions. Retain every intake ID, flag every duplicate alias, reject ambiguous extraction, compare integer minor units with explicit ISO currency, and publish a new local Parquet report only after validation. Apply the same relational rules to a structured-input baseline.
---

# Reconcile invoice documents against purchase orders

Turn invoice documents into canonical rows, then apply purchasing checks with
Spark SQL and DataFrame code on Sail. The document path renders PDFs and runs
RapidOCR on the CPU. The structured baseline starts with authored invoice rows.
Both use the same reconciliation function.

This is a controlled-layout extraction example. It recognizes a registered
supplier name in the first eight OCR lines, including a name wrapped over two
lines, then reads an invoice-number label and its next line and a `TOTAL` label
and its next line. Repeated or missing labels, or a field value occupying another
field's label position, are exceptions. It does not parse
arbitrary invoice layouts or infer fields with a language model.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run pipelines/reconcile-invoice-documents/main.py
```

The first run downloads four small, pinned PDF data files from the public
[invoice extraction corpus](https://huggingface.co/datasets/drew-ipp/invoice-extraction-benchmark).
No key or GPU is needed. The files contain printable text, but this path renders
the pages and reads the pixels with OCR rather than extracting the text layer.

The printed business report includes document ID, invoice ID, explicit ISO
currency, gross total in integer minor units, status and extraction error.
Invoice and total evidence are also retained in the Parquet rows. It does not
print unrelated address, contact or banking text.

The purchasing context is authored for this example: intake assigns supplier ID,
PO association and ISO currency. The bare `$` printed on a page does not prove
USD. The extractor accepts `$` only when intake explicitly supplies `USD`, and
`£` only for `GBP`. Both currencies use exactly two fractional digits here.
Amounts are parsed with `Decimal`, without floating-point rounding or a tolerance.

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
documents
+--------------+----------------+--------+-----------+-----------------+-------------------------------------------+
|document_id   |invoice_id      |currency|gross_minor|status           |error                                      |
+--------------+----------------+--------+-----------+-----------------+-------------------------------------------+
|doc-acme      |ACM-100145      |USD     |37249      |duplicate_invoice|NULL                                       |
|doc-acme-alias|ACM-100145      |USD     |37249      |duplicate_invoice|NULL                                       |
|doc-blank     |NULL            |USD     |NULL       |extraction_error |blank                                      |
|doc-corrupt   |NULL            |USD     |NULL       |extraction_error |read_or_ocr_failed:PdfiumError             |
|doc-mersey    |NULL            |GBP     |NULL       |extraction_error |invoice_or_total_label_missing_or_ambiguous|
|doc-northwind |NWL-20240318-002|USD     |159943     |amount_mismatch  |NULL                                       |
|doc-pennine   |IN0049923       |GBP     |2161390    |missing_po       |NULL                                       |
+--------------+----------------+--------+-----------+-----------------+-------------------------------------------+

structured
+-----------------------+----------+--------+-----------+-----------------+------------------+
|document_id            |invoice_id|currency|gross_minor|status           |error             |
+-----------------------+----------+--------+-----------+-----------------+------------------+
|structured-duplicate-a |STRUCT-002|GBP     |9000       |duplicate_invoice|NULL              |
|structured-duplicate-b |STRUCT-002|GBP     |9000       |duplicate_invoice|NULL              |
|structured-invalid     |NULL      |USD     |NULL       |extraction_error |missing_invoice_id|
|structured-match       |STRUCT-001|USD     |5000       |match            |NULL              |
|structured-missing-po  |STRUCT-003|USD     |7000       |missing_po       |NULL              |
|structured-wrong-amount|STRUCT-004|USD     |4999       |amount_mismatch  |NULL              |
+-----------------------+----------+--------+-----------+-----------------+------------------+

```

<!-- sail-output:end -->

## Read the output

The console output has separate `documents` and `structured` reports.
[Saved console captures](outputs.json) include the Sail version.
The document inputs exercise duplicate invoices, a one-cent PO mismatch, a missing
PO, and extraction failures. In the Sail 0.7.3 capture, Mersey also returns
`extraction_error`: OCR did not produce unambiguous invoice and total labels.

The six structured inputs include one match, two duplicate aliases, a missing
PO, a wrong amount and a missing invoice ID. A `match` means only that these
programmed supplier, duplicate, currency and gross-total checks passed. It is
not payment approval, evidence of delivery or fraud detection. The PO comparison
is one invoice gross total against one PO gross amount. Partial invoices,
credits, zero-value invoices, line-item matching, tax validation and invoices
covering several POs are outside this example.

## Data flow and local state

1. A manifest carries paths and SHA-256 digests rather than PDF binary columns.
   Each document is limited to 5 MB and exactly one page. The whole multi-page
   document is rejected. Page dimensions are checked before rendering, with a
   fixed scale of 1.5 and a maximum of 2 million rendered pixels.
2. `mapInArrow` creates one OCR engine per partition and emits one bounded output
   batch per input document. Every input ID remains a row, including blank,
   corrupt, oversized and unsupported documents. Model setup failures fail the
   job instead of publishing a report.
3. A small local cache stores at most 512 bounded OCR lines per PDF. Cache keys
   bind source bytes, this script and OCR configuration. Every hit verifies the
   current source SHA and the cached identity and line digest. Invalid entries
   are recomputed. A per-key file lock and temporary-file replacement coordinate
   writers. Raw lines remain in this local cache; treat it as invoice data when
   adapting the example to private documents.
4. Canonical extraction rows are written once to a new run's `ocr.parquet` and
   read back before supplier/PO joins and duplicate detection. This prevents
   later report actions from rerunning an OCR expression.
5. Supplier and PO keys must be unique before joining. Extraction errors take
   priority. Duplicate grouping uses successfully extracted supplier/invoice
   keys, excludes missing IDs, and flags every alias. No cross-currency sum or
   duplicate-inclusive payable total is produced.
6. One local writer holds the report lock. Each run gets new Parquet paths under
   `pipelines/reconcile-invoice-documents/.data/runs/`; existing runs are not overwritten by this program. After both
   reports retain exactly one row per unique intake ID, an atomic `CURRENT`
   replacement publishes the complete generation. A failure before publication
   keeps the previous report readable. `current_run()` validates the pointer's
   manifest digest. Previous runs remain for inspection.

The state layout is an example for one machine and a local filesystem. It is not
a distributed commit protocol, an exactly-once guarantee or a power-loss recovery
claim. The cache and report files have no automatic retention policy. For a
shared deployment, choose storage, locking, access controls and retention for
that environment. Install the OCR dependencies wherever the engine's Python
workers run; every manifest path must be readable there.

## On Sail

In local mode, UDF partitions can run as threads of one Sail process. Every
PDFium call, including close, uses `process_lock("pdfium")` stored in
`sys.modules`, shared with the other document examples. Rendering is serialized
inside that process; OCR runs outside the lock. Persisting OCR to Parquet is
explicit because downstream actions must consume the same extracted rows.

## Sources and verification

The four PDFs are from `drew-ipp/invoice-extraction-benchmark` at revision
`446bab08662790cb39457792243bff5704373b95`, under `corpus/v1/documents/`.
[The pinned corpus](https://huggingface.co/datasets/drew-ipp/invoice-extraction-benchmark/tree/446bab08662790cb39457792243bff5704373b95/corpus/v1)
describes the invoices as synthetic and declares [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
for the data. Credit:
drew-ipp, invoice-extraction-benchmark. The source PDFs are unchanged; this example
renders them, extracts selected fields and adds separate authored purchasing
context. [sources.json](fixtures/sources.json) pins each PDF by SHA-256.

The source's expected supplier, invoice number, currency and gross total are
transcribed separately in [source-gold.json](fixtures/source-gold.json). Tests
read that answer key after extraction; runtime OCR and reconciliation never read
it. The maintenance checks compare real OCR fields to that independent source
gold and compare final states to literal expected outcomes. Separate cases exercise ambiguous
labels, null IDs, all duplicate aliases, dimension-key duplication, cache
invalidation and failure before publication. Four selected layouts do not
establish accuracy on other documents.
