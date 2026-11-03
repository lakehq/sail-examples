---
title: Sessions, funnels and cohort retention with SQL
id: sessions-funnels-retention
description: Use when you analyze event or clickstream data and need sessions from exact time gaps, an ordered session-scoped conversion funnel, or weekly cohort retention that distinguishes observed inactivity from future weeks.
tier: analytics
level: intermediate
tags: [sql, window-functions, sessionization, funnel, retention, cohorts, pivot, product-analytics]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Given a clickstream table of user_id, timestamp and event, use Spark SQL to split events into sessions at gaps strictly over 30 minutes, count ordered funnel steps at strictly increasing timestamps within each session, and build a weekly retention matrix with zero for observed inactivity and null for future weeks.
---

# Sessions, funnels and cohort retention with SQL

Three questions every product team asks of event data, each answered with SQL window functions:
- How do events group into visits?
- Where do users drop out on the way to a purchase?
- How many users come back in the weeks after they first appear?

The input is a clickstream of 1,500 users over eight weeks: one row per event (page view, product view, add to cart, checkout, purchase). It is generated with a fixed seed, so every run gets the same events.

## Run it

```bash
uv run main.py
```

It prints a session summary, the ordered funnel and the retention matrix. Funnel counts are sessions, not users or individual purchases. Retention counts distinct users, not events.

## How it works

1. **Sessions from exact gaps.** Distinct `(user_id, ts)` pairs receive session numbers with `lag` and a running `sum`. Comparing timestamps with a 30-minute interval keeps an exact 30-minute gap in the session and splits a gap even one microsecond longer. Joining back preserves all events at a timestamp, including identical rows, with the same session number.
2. **An ordered funnel within each session.** The query finds the earliest first step, then the earliest next step at a strictly later timestamp, continuing through the configured steps. Equal-time events cannot prove an order. Repeated events and intervening unrelated events do not reset progress; each session counts at most once per step. A purchase before checkout does not complete the funnel unless another purchase follows it. Progress does not cross sessions. Indexed stages also allow a funnel such as `A`, `B`, `A`. The first conversion rate, and a rate whose previous step has zero sessions, are null.
3. **Cohort retention over an explicit period.** The period includes `LAUNCH` and excludes the timestamp `WEEKS` calendar weeks later. A user's cohort is their first observed active week in this period, not a proven acquisition date. Distinct user/week pairs prevent repeated events from inflating activity. Every observed cohort/week cell is present, with zero active users and zero retention when nobody returns.
4. **A cohort matrix.** `groupBy(...).pivot("weeks_later", ...)` turns the long table into one row per cohort and one column per week. A cohort's future weeks remain null. For example, over three observed weeks a user seen only in week zero has retention `1, 0, 0`; a user first seen in week one has `1, 0, null`.

## Use your own events

Pass your event DataFrame to `analyze_events`, with non-null `user_id`, `ts` (`TIMESTAMP_NTZ`) and `event` columns. Set `steps`, `launch`, `weeks` and `gap_minutes` for your analysis. The launch must be a timezone-naive midnight. The example treats timestamps as wall-clock values; normalize your source time zones before calling it.

The period must be fully observed: do not infer its end from the last event, because a quiet week is still an observed week. These calculations analyze a fixed input snapshot; they do not infer missing events or first activity before the period. Duplicate event rows remain in session event counts, but do not inflate funnel sessions or distinct-user retention.

Each call uses its own temporary views so creating another analysis cannot
replace an earlier result's input. The views remain until the session ends,
allowing the returned DataFrames to be evaluated later. For repeated analyses
in a long-lived application, manage that session and view lifetime explicitly.

## On Sail

- Write the pivot with the DataFrame API (`groupBy(...).pivot(...)`). Sail does not support the SQL `PIVOT` clause.
- Number sessions with `lag` and a running `sum`, as here, rather than with `session_window`, which Sail does not support.
