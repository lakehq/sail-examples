---
title: Turn spoken requests into intent analytics
id: support-intent-analytics
facts: [load-files-through-a-path-table, udf-setup-per-partition, mapinarrow-arrow-types]
summary: Turn spoken support requests into a report of customer needs.
description: Use when you want to transcribe short spoken requests, compare intent predictions with labels, and aggregate the results with SQL.
tier: analytics
level: advanced
tags: [audio, inference, classification, evaluation, sql]
modality: audio
engines: both
ordered: true
float_tolerance: 1.0e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Turn a bounded set of spoken banking requests into transcripts and intent counts, keeping failed and unknown requests in the evaluation denominator.
---

# Turn spoken requests into intent analytics

Transcribe a small set of English banking requests with Whisper, apply a visible
keyword baseline, and use SQL to report intent counts and classification errors.
The same baseline also scores the dataset's supplied ASR text, so the two input
paths can be compared on the same requests and independent intent labels.

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run analytics/support-intent-analytics/main.py
```

The default downloads the pinned English-US MINDS-14 shard (34.2 MB) and the
tiny.en speech model if they are not cached. It selects at most 12 requests and
180 seconds of decoded audio. Requests follow a fixed hash order; selection
stops when the next request would exceed either bound. It does not select by
intent, predicted correctness, or transcription quality. The selected audio is
decoded into mono PCM WAVE and staged with opaque filenames before inference.
The source uses G.711 mu-law WAVE. Decoding checks sample counts and duration,
with a separate 90-second ceiling per clip.
The pinned default selects 12 recordings totaling 159.168 seconds of audio.
Its catalog timeout is 120 seconds.

For a longer, still bounded evaluation, request up to 96 clips and 600 seconds:

```sh
uv run analytics/support-intent-analytics/main.py --max-clips 96 --max-seconds 600 --json
```

`--json` prints all result tables, including per-request statuses and predictions.
The CLI rejects more than 563 clips, more than 3,600 seconds, or nonpositive or
nonfinite bounds before starting the engine. The larger run may take longer than
the default example. Inspect test-split metrics and class coverage independently
for both text sources; a successful run alone does not establish routing quality.

<!-- sail-output:start variant=default -->

## Example output

Recorded output · Sail 0.7.3.

The example has changed since this capture. This output comes from the earlier script.

```text
+------------+-----+-----------------+----------------+----------------+------------+----------------+--------+--------+-------------------+
|text_source |split|selected_requests|correct_requests|unknown_requests|input_errors|observed_intents|accuracy|coverage|macro_f1_all_14    |
+------------+-----+-----------------+----------------+----------------+------------+----------------+--------+--------+-------------------+
|supplied_asr|test |8                |5               |3               |0           |7               |0.625   |0.625   |0.3333333333333333 |
|supplied_asr|train|4                |2               |1               |0           |4               |0.5     |0.75    |0.14285714285714285|
|whisper     |test |8                |5               |3               |0           |7               |0.625   |0.625   |0.3333333333333333 |
|whisper     |train|4                |2               |1               |0           |4               |0.5     |0.75    |0.14285714285714285|
+------------+-----+-----------------+----------------+----------------+------------+----------------+--------+--------+-------------------+

+------------+-----+---------------+------------------+--------+
|text_source |split|reporting_group|predicted_intent  |requests|
+------------+-----+---------------+------------------+--------+
|supplied_asr|test |synthetic-0    |abroad            |1       |
|supplied_asr|test |synthetic-0    |high_value_payment|1       |
|supplied_asr|test |synthetic-0    |unknown           |1       |
|supplied_asr|test |synthetic-1    |app_error         |1       |
|supplied_asr|test |synthetic-1    |card_issues       |1       |
|supplied_asr|test |synthetic-1    |pay_bill          |1       |
|supplied_asr|test |synthetic-1    |unknown           |2       |
|supplied_asr|train|synthetic-1    |app_error         |1       |
|supplied_asr|train|synthetic-1    |balance           |1       |
|supplied_asr|train|synthetic-1    |cash_deposit      |1       |
|supplied_asr|train|synthetic-1    |unknown           |1       |
|whisper     |test |synthetic-0    |abroad            |1       |
|whisper     |test |synthetic-0    |high_value_payment|1       |
|whisper     |test |synthetic-0    |unknown           |1       |
|whisper     |test |synthetic-1    |app_error         |1       |
|whisper     |test |synthetic-1    |card_issues       |1       |
|whisper     |test |synthetic-1    |pay_bill          |1       |
|whisper     |test |synthetic-1    |unknown           |2       |
|whisper     |train|synthetic-1    |app_error         |1       |
|whisper     |train|synthetic-1    |balance           |1       |
+------------+-----+---------------+------------------+--------+
only showing top 20 rows

```

<!-- sail-output:end -->

## The workflow

1. Verify the dataset SHA256, group byte-identical audio or normalized supplied text,
   and assign entire duplicate groups to a fixed train/test split.
2. Pass only request IDs, staged audio paths, and decode status to `mapInArrow`.
   Load tiny.en once per partition and retain each failed or empty result.
3. Write transcripts to Parquet, then read them back before classification and
   aggregation. Subsequent queries reuse those observations.
4. Score each text source separately with the same rules. Every matching regular
   expression contributes one vote. Empty text, no matches, and tied winners
   produce `unknown`. An audio failure produces `unknown` on the Whisper path;
   the supplied ASR path can still classify its available text.
5. Join labels after inference and aggregate with SQL expressions to report
   counts, accuracy, prediction coverage, a confusion table, and macro-F1 averaged
   over all 14 intents. Missing classes
   contribute zero to that macro average; `observed_intents` shows class coverage.

The rules are written in `RULES` and do not fit parameters or use labels. The
train partition is available for future rule development; use the test partition
for evaluation and keep it out of rule tuning. Hash grouping prevents byte-identical audio
and normalized-text duplicates crossing that boundary. It does not establish
speaker independence or remove all semantically similar requests. Different
encodings or headers of the same speech need not form one duplicate group.
`run(spark, max_clips=..., max_seconds=...)` also accepts explicit larger bounds
for a separate evaluation run.

Returned tables are `requests`, `predictions`, `summary`, `topics`, and `confusion`.
There is one prediction per selected request **per text source**. Keep the
`text_source` dimension when aggregating; summing both paths would double count
requests. Failed inputs and abstentions remain in the accuracy denominator.
An invalid audio file contributes a decode error and zero known duration,
instead of disappearing. Only valid, duration-checked PCM audio reaches Whisper.
Model initialization or download failures stop the run rather than producing
an apparently successful all-error evaluation. A run also fails if no selected
request produces a successful Whisper transcript.

`reporting_group` is generated and explicitly named `synthetic-0` or
`synthetic-1`. These groups illustrate aggregation, not actual queues, dates,
business trends, or representative intent prevalence. The short selection may
cover only some intents. The baseline is a transparent starting point with
lexical ambiguity and negation limitations; evaluate its printed results before
using it for routing. No rule output is substituted for a speech-model result.
The unit fixtures check rule mechanics and aggregation arithmetic separately;
they do not establish classification accuracy on MINDS-14.

## Focused checks

From the repository root, run the input, native-codec, and accounting controls:

```sh
uv run pytest tests/test_support_intent_analytics.py
```

The explicit longer phase runs the pinned real data and Whisper on both engines,
checks the fixed selection independently from the source audio headers, and
recomputes the reported metrics. It allows 900 seconds per engine:

```sh
SUPPORT_INTENT_CHECK=longer uv run pytest tests/test_support_intent_analytics.py -k intent_analytics_preserves
```

Those checks validate IDs, bounds, and accounting. Assess classification quality
separately from the reported labeled errors and abstentions.

## Data and model

[MINDS-14](https://huggingface.co/datasets/PolyAI/minds14) by PolyAI is released
under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
This example uses the `en-US/train-00000-of-00001.parquet` file at revision
`28a8ee8911625fe71b2e0462c3dc14132d1058fa`, verified with SHA256
`37004471dc896ce20771b3fdda0ee8fb33ec7a030fb4e2fd047c561ec0a1ee30`.
Selection, opaque filenames, duplicate grouping, and reporting groups are changes
made by this example. Cite Gerz et al.,
[Multilingual and Cross-Lingual Intent Detection from Spoken Data](https://aclanthology.org/2021.emnlp-main.591/)
(EMNLP 2021).

The recordings are crowdsourced utterances elicited from intent descriptions and
examples, not recordings of actual customer support calls. The authors generated
the supplied transcriptions with Google ASR. Those strings are a comparison input,
**not human transcription gold**; this example does not report word error rate.
Intent labels are the target for the classification evaluation. The English-US
release supplies only a `train` split; this example's split is its own fixed split.

[Whisper tiny.en](https://github.com/openai/whisper) is MIT licensed. The
[CTranslate2 conversion](https://huggingface.co/Systran/faster-whisper-tiny.en)
is pinned to `0d3d19a32d3338f10357c0889762bd8d64bbdeba`. It runs on the CPU
with greedy decoding, a fixed temperature, and one model thread. Cluster workers
need the Python packages and access to the staged paths; model files are fetched
where inference runs.
