---
title: Reconcile orders, captures and refunds
id: reconcile-orders-payments
description: Use when separate order and payment feeds contain repeated deliveries, split captures, partial refunds or orphan transactions, and you need an explainable reconciliation at an explicit cutoff.
tier: analytics
level: intermediate
tags: [reconciliation, joins, aggregation, data-quality, payments]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Reconcile order, capture and refund snapshots with Spark SQL and DataFrame code. Quarantine conflicting IDs before joining, collapse duplicate deliveries, aggregate split captures and partial refunds, isolate currencies and return order summaries, transaction classifications, payload-preserving exceptions and exact money-conservation totals at an explicit cutoff.
---

# Reconcile orders, captures and refunds

An order can have several captures, and each capture can have several refunds.
Joining all three raw feeds multiplies amounts when deliveries repeat or a
capture has several refunds. This example validates and deduplicates each feed,
classifies transactions against unique parents, then aggregates before joining
order totals.

## Run it

```bash
uv run main.py
```

The small, generated fixture needs no credentials or downloads. It returns four
tables:

- `orders`: one row per accepted order, with captured, refunded and net amounts,
  transaction counts, balance and status.
- `ledger`: one row per accepted logical order, capture or refund, including its
  first eligible receipt time, eligible delivery count and classification.
- `exceptions`: rejected payloads, conflicting variants, unmatched transactions
  and an alert for each capture whose attributed refunds exceed its amount.
- `accounting`: eligible, unambiguous capture and refund totals separately, by
  currency, proving `total_minor = attributed_minor + unmatched_minor`. Invalid,
  not-yet-eligible and conflicting-ID payloads remain in `exceptions`, outside
  this identity. Totals count logical transactions once; `deliveries` counts
  their eligible source deliveries.

For example, two captures of 600 and 400 settle the 1,000-unit `paid` order even
though the 600-unit capture arrives twice. A 200-unit refund leaves `refund`
with a net 800. A 600-unit refund against a 500-unit capture remains visible as
an over-refund, with net -100. Amounts are never clipped to hide a discrepancy.

## Reconciliation contract

Call `reconcile(orders, captures, refunds, cutoff=...)` with the exact schemas in
`INPUT_SCHEMAS`. IDs are globally unique within each feed, not within an order.
Each feed is a complete bounded delivery snapshot, not an incremental update.

1. **Validate first.** IDs and parent IDs must contain only ASCII letters, digits,
   dots, underscores, colons or hyphens, with at least one character. Whitespace
   and Unicode IDs become exceptions; currency codes must match three uppercase
   ASCII letters. Amounts are
   positive integer minor units, at most 1,000,000,000,000 per record. Each feed
   is limited to 20,000 deliveries, keeping all sums inside signed 64-bit range.
   Unknown columns and wrong types fail the whole call; invalid values become
   exceptions. Currency syntax validation does not verify an ISO registry.
2. **Use two times.** A delivery is eligible only when both `posted_at` and
   `received_at` are strictly before the cutoff. Values use `TIMESTAMP_NTZ`;
   normalize source timestamps to UTC wall-clock values before calling. A late
   delivery cannot revise an earlier as-known result. Excluded deliveries still
   appear in exceptions, with receipt-time exclusion taking precedence.
3. **Resolve identity before joining.** Equal IDs with equal parent, currency,
   amount and posting time collapse to one transaction. Receipt time is delivery
   metadata: retain its minimum and count eligible deliveries. Different valid,
   eligible payloads under one ID quarantine every such variant. Invalid deliveries
   remain exceptions without invalidating an otherwise accepted transaction. Never
   select a winner by arrival order. A parent quarantined this way cannot attract
   child money.
4. **Classify without guessing.** Captures require an accepted order of the same
   currency. Refunds require an accepted capture of the same currency, a posting
   time at or after the capture, and an attributed capture. Otherwise they stay
   unmatched with a reason. No currency conversion or cross-currency netting
   occurs. Units follow each source currency; the example never assumes every
   currency has two decimal places.
5. **Aggregate before summarizing.** Only attributed transactions enter order
   totals. Refunds take status precedence over under/over-capture; the numeric
   columns retain both discrepancies. `balance_minor` is original order amount
   minus net captures, not a statement of a customer's legal amount due.

Reordering the same deliveries leaves every result unchanged. Replaying them
leaves logical money, transaction counts and order statuses unchanged, while
delivery counts increase. An identical rejected delivery is represented by one
exception row and its count. A conflicting eligible variant retains its
business payload, first receipt and count. Over-refund alerts overlap attributed
transactions, so summing the exception table is not a conservation calculation.

## Correctness and limits

`reference()` uses a separate plain-Python ledger to check the default result.
Focused tests also use literal expected totals, cutoff boundaries, conflicts in
parent IDs, repeated deliveries, empty inputs and a larger skewed order. Actual
join cardinality guards reject accidental multiplication.

This is a fixed-snapshot reconciliation pattern. It does not implement a general
accounting ledger, settlement fees, chargebacks, tax, foreign exchange, source
revision sequences or durable incremental state. A correction with a changed
payload needs a source-specific revision policy rather than this example's
conflict quarantine. Input DataFrames should describe stable snapshots while
their returned results are evaluated.
