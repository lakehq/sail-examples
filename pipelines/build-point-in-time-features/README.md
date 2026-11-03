---
title: Build features as they were known at each decision
id: build-point-in-time-features
description: Use when reconstructing demand-planning inputs from order revisions and product history, so late arrivals and later corrections cannot leak into earlier decisions.
tier: pipelines
level: advanced
tags: [point-in-time, temporal-joins, demand-planning, data-quality, windows]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Reconstruct order-history and product features using both event time and availability time, resolve source revisions before window filtering, reject ambiguous product intervals, and compare with a naive event-time-only join and an independent Python oracle.
---

# Build features as they were known at each decision

Reconstruct the information a planner could use when deciding how much stock to
order. An order can arrive late, and a later revision can correct its quantity,
product or event time. Product attributes also have effective intervals and
availability times. A join on event time alone can include information that the
planner did not yet have.

## Run it

```bash
uv run main.py
```

The example prints four tables: `features`, `naive_features`, `outcomes` and
`rejected`. It uses generated records, requires no credentials and trains no
model.

The early tea decision has 12 known units across three orders. Ignoring
availability produces 25 units: it includes a late arrival and a later quantity
correction. At the later decision, a new effective product interval supplies a
different lead time. The coffee decision has zero observed historical orders
and an unavailable future outcome. Missing and overlapping product intervals
produce rejected requests rather than arbitrary attribute choices.

## Input contract

`build_features(spark, requests, orders, products, coverage, evaluation_at)`
accepts small lists of complete records. It returns deferred DataFrames and
does not create or stop the session. All timestamps describe UTC instants;
the query uses `TIMESTAMP_NTZ` UTC values so session time zones cannot change
the boundaries. Python callers may pass naive UTC `datetime` values. JSON
timestamps require `Z` or an explicit zero UTC offset.

| Input | Fields |
| --- | --- |
| Requests | `request_id`, `product_id`, `history_start`, `decision_at`, `horizon_end` |
| Order revisions | `order_id`, `revision`, `product_id`, `event_at`, `available_at`, `units` |
| Product interval revisions | `interval_id`, `revision`, `product_id`, `valid_from`, `valid_to`, `available_at`, `category`, `lead_days` |
| Coverage watermarks | `product_id`, `observed_through`, `available_at` |

Each request ID is unique, with `history_start < decision_at < horizon_end`.
Source revisions are positive integers, ordered within an order or interval ID.
They are not arrival sequence numbers. Higher revisions replace the whole
record, including its product and time fields. Units are nonnegative integers;
zero cancels an order. Orders cannot be available before their event time.
Product intervals are half-open, `[valid_from, valid_to)`, with a lead time from
0 to 365 days.

Exact repeated source records collapse. Distinct records at the same source ID
and revision fail the whole call, including distinct availability times. This
requires a source-assigned immutable revision and a stable first availability
time. Availability must mean the record was usable by the planning system,
including ingestion delays. Fabricated or backdated availability defeats this
check.

Fields cannot be null or omitted; unexpected fields, invalid values and
conflicting revisions raise `ValueError` with the offending input before any
query is built. Inputs are bounded to 128 requests and 5,000 rows per other
table. Keys and categories are nonblank strings of at most 200 characters;
units and revisions are capped at one billion. This keeps the local input and
per-request joins bounded. Larger ingestion pipelines should validate and load
their own tables rather than collecting them into these lists.

## Selection rules

For each request, retain records with `available_at <= decision_at`, then choose
the highest available source revision per order or product interval. Apply the
product and event-time filters **after** selecting revisions. Otherwise an old
quantity can reappear when a correction moves an order outside the lookback or
to another product.

Historical orders must lie in `[history_start, decision_at)`. Availability at
the decision is allowed; an event at the decision belongs to the future horizon.
Cancelled orders contribute neither units nor order count. A request with no
qualifying orders receives zero units and count, with a null last-order time.

Product attributes must match the requested product and be effective at the
decision. Exactly one selected interval must match. No match yields
`missing_product_interval`; multiple matches yield
`ambiguous_product_intervals`. Rejected requests retain their input fields and
do not enter feature or outcome tables. The example rejects ambiguity even if
overlapping intervals happen to carry identical attributes.

`naive_features` deliberately uses the latest order revision in the entire
supplied history and ignores order availability. It retains the same product
selection so the difference isolates order leakage. It is a diagnostic output,
not an input for a model.

## Outcomes and checks

Future demand is separate from features. `outcomes` sums order units in
`[decision_at, horizon_end)` using revisions available by `evaluation_at`.
A target is available only if the horizon has ended and an available coverage
watermark reaches its end. A watermark asserts that the product's order feed
is complete through that event time as known at evaluation; it does not promise
that future corrections will never occur. Incomplete coverage produces a null
target with `unavailable`, never a fabricated zero. Complete coverage with no
orders produces an observed zero.

An independent Python implementation enumerates revisions separately for each
request and checks every output through `reference()`. Additional checks cover
boundary instants, cancellations, equal-time revisions, interval corrections,
empty inputs and delayed collection of earlier query plans. Appending valid
records available strictly after an old decision leaves that decision's
features unchanged. Outcomes and the deliberately naive diagnostic can change.

## Use your own records

Put the four input lists and `evaluation_at` in a JSON object. Use the field
names above, integer revisions and units, and timestamps such as
`"2026-01-05T00:00:00Z"`. Empty lists are supported.

For example, this complete input has one known order and an unavailable outcome:

```json
{
  "requests": [{"request_id":"plan-1","product_id":"tea","history_start":"2026-01-01T00:00:00Z","decision_at":"2026-01-05T00:00:00Z","horizon_end":"2026-01-07T00:00:00Z"}],
  "orders": [{"order_id":"order-1","revision":1,"product_id":"tea","event_at":"2026-01-03T00:00:00Z","available_at":"2026-01-04T00:00:00Z","units":4}],
  "products": [{"interval_id":"tea-jan","revision":1,"product_id":"tea","valid_from":"2026-01-01T00:00:00Z","valid_to":"2026-02-01T00:00:00Z","available_at":"2026-01-01T00:00:00Z","category":"pantry","lead_days":3}],
  "coverage": [],
  "evaluation_at": "2026-01-08T00:00:00Z"
}
```

```bash
uv run main.py --input planning-inputs.json
```

The console shows all bounded request results. No database or output directory
is mutated. This reconstructs a batch feature table; source capture, streaming,
model training and online serving are outside its scope.
