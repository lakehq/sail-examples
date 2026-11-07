---
title: Route support tickets with a model in SQL (Jev)
id: jev-sql-routing
description: Use when you want to classify table rows with a model from plain SQL on Sail, using the built-in Jev functions, and call the model once per distinct value.
tier: ai
level: intermediate
tags: [jev, llm, sql, classification, distinct, sail-only]
modality: text
engines: sail-only
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Use Sail's jev_choice SQL function to route each support ticket to the billing, bug or how-to team, calling the model once per distinct ticket text.
---

# Route support tickets with a model in SQL (Jev)

Sail has SQL functions that call TypeSafe's Jev models; Apache Spark has none of them. This example routes 2,000 support tickets to one of three teams with `jev_choice`, calling the model 24 times, once per distinct ticket text.

## Run it

```bash
uv run main.py
```

By default the example runs against the local TypeSafe stand-in that ships in pysail for Sail's own tests (`pysail.testing.jev.JevMock`). It needs no account, and nothing leaves the machine. The stand-in picks a team with a keyword rule.

To call the real service, set `JEV_LIVE=1` and `TYPESAFE_API_KEY` in the environment of the process that runs Sail. Ticket texts are then sent to TypeSafe, and the answers come from the Jev model.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
+-------+-------+
|   team|tickets|
+-------+-------+
|billing|    667|
|    bug|    667|
| how-to|    666|
+-------+-------+

+-------+--------------+--------+
|tickets|distinct_texts|requests|
+-------+--------------+--------+
|   2000|            24|      24|
+-------+--------------+--------+

```

<!-- sail-output:end -->

## How it works

- **One SQL query asks the model.** `jev_choice(state, instructions, criteria)` returns a struct with the chosen `choice`, its `confidence` and per-choice `probabilities`. Here, `criteria` is a `map` from each team name to a description of the team.
- **Distinct first.** The query runs `jev_choice` over `SELECT DISTINCT text`, so the model answers 24 questions, not 2,000. Without `DISTINCT`, Sail asks one question per row: adjacent rows with the same text share a request, but each row is still its own question, and repeats that are not adjacent are sent again.
- **Save, then join.** The answers are written to Parquet once, and the summary joins them back onto every ticket.

## On Sail

- Jev functions need `TYPESAFE_API_KEY`, and read the service address from `TYPESAFE_BASE_URL`, in the environment of the Sail server. On a cluster, set them on every worker that runs Jev calls (per the Sail Jev docs).
- With an in-process server, as here, setting the variables before the first Jev query works. For `sail spark server`, set them before you start the server.
- `cache()` does not keep results on Sail, so the answers are saved to Parquet: in one run, each distinct text is asked once.
