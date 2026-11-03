# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Product analytics with window functions: sessions, a conversion funnel and cohort retention.

The input is a clickstream: one row per user event (view a page, view a product, add to cart,
check out, buy). Sessions come from exact time gaps, funnel steps must occur in order, and
retention distinguishes observed inactivity from weeks outside the observation period.
"""

import datetime as dt
import random
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STEPS = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]
SESSION_GAP_MINUTES = 30  # a pause longer than this starts a new session
LAUNCH = dt.datetime(2024, 1, 1)  # a Monday; weeks are counted from here
WEEKS = 8  # the period the clickstream covers


def make_events(users: int = 1500, weeks: int = WEEKS, seed: int = 7) -> list[tuple[int, dt.datetime, str]]:
    """A reproducible clickstream: users join over time, visit in sessions and drift away."""
    rng = random.Random(seed)
    events = []
    for user in range(users):
        joined = LAUNCH + dt.timedelta(days=rng.randrange(weeks * 7 - 7), minutes=rng.randrange(24 * 60))
        loyalty = rng.random()  # a higher value means more return visits
        visit = joined
        while visit < LAUNCH + dt.timedelta(weeks=weeks):
            clock = visit
            for step in STEPS:
                for _ in range(rng.randint(1, 3) if step in ("page_view", "product_view") else 1):
                    events.append((user, clock, step))
                    clock += dt.timedelta(seconds=rng.randint(20, 300))
                if rng.random() > 0.55:  # most sessions stop before buying
                    break
            if rng.random() > 0.35 + 0.6 * loyalty:  # less loyal users stop returning sooner
                break
            visit = clock + dt.timedelta(hours=rng.randint(2, 24 * 9))
    return events


def analyze_events(
    events: DataFrame,
    *,
    launch: dt.datetime = LAUNCH,
    weeks: int = WEEKS,
    steps: list[str] = STEPS,
    gap_minutes: int = SESSION_GAP_MINUTES,
) -> dict[str, DataFrame]:
    """Analyze a complete half-open period of non-null, wall-clock event timestamps."""
    if launch.tzinfo is not None or launch.time() != dt.time():
        raise ValueError("launch must be a timezone-naive midnight for calendar-week retention")
    if not isinstance(weeks, int) or weeks < 1 or not isinstance(gap_minutes, int) or gap_minutes < 0:
        raise ValueError("weeks must be positive and gap_minutes must be nonnegative integers")
    if not steps or any(not isinstance(step, str) or not step for step in steps):
        raise ValueError("steps must contain nonempty event names")
    # Caller metadata must not collide with columns introduced by the analysis.
    events = events.select("user_id", "ts", "event")
    if events.where(F.col("user_id").isNull() | F.col("ts").isNull() | F.col("event").isNull()).take(1):
        raise ValueError("user_id, ts and event must not be null")
    spark = events.sparkSession
    end = launch + dt.timedelta(weeks=weeks)
    events = events.where(
        (F.col("ts") >= F.lit(launch.isoformat(" ")).cast("timestamp_ntz"))
        & (F.col("ts") < F.lit(end.isoformat(" ")).cast("timestamp_ntz"))
    )
    suffix = uuid.uuid4().hex
    event_view = f"events_{suffix}"
    session_view = f"sessionized_{suffix}"
    offset_view = f"retention_offsets_{suffix}"
    # Connect resolves view references lazily. Keep each call's views independent.
    events.createOrReplaceTempView(event_view)

    # Assign each distinct timestamp once, then join back without dropping repeated events.
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW {session_view} AS
        WITH times AS (SELECT DISTINCT user_id, ts FROM {event_view}),
             gaps AS (
                 SELECT *, CASE WHEN ts <= lag(ts) OVER (PARTITION BY user_id ORDER BY ts)
                                                   + INTERVAL {gap_minutes} MINUTES
                                THEN 0 ELSE 1 END AS new_session
                 FROM times),
             assigned AS (
                 SELECT user_id, ts,
                        sum(new_session) OVER (PARTITION BY user_id ORDER BY ts
                                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number
                 FROM gaps)
        SELECT e.*, a.session_number FROM {event_view} e JOIN assigned a
        ON e.user_id = a.user_id AND e.ts = a.ts
    """)
    sessions = spark.sql(f"""
        SELECT count(*) AS sessions,
               count(DISTINCT user_id) AS users,
               round(avg(events), 2) AS avg_events_per_session,
               percentile(minutes, 0.5) AS median_session_minutes
        FROM (SELECT user_id, session_number, count(*) AS events,
                     timestampdiff(MINUTE, min(ts), max(ts)) AS minutes
              FROM {session_view} GROUP BY user_id, session_number)
    """)

    # Earliest eligible occurrences form the longest ordered prefix in each session.
    # Indexed CTEs allow repeated configured names, such as A -> B -> A.
    stages = [
        f"""
        stage_0 AS (
            SELECT user_id, session_number, min(CASE WHEN event = :step_0 THEN ts END) AS reached_at
            FROM {session_view} GROUP BY user_id, session_number)
    """
    ]
    for index in range(1, len(steps)):
        stages.append(f"""
            stage_{index} AS (
                SELECT s.user_id, s.session_number, min(e.ts) AS reached_at
                FROM stage_{index - 1} s LEFT JOIN {session_view} e
                ON e.user_id = s.user_id AND e.session_number = s.session_number
                   AND e.event = :step_{index} AND e.ts > s.reached_at
                GROUP BY s.user_id, s.session_number)
        """)
    stage_rows = " UNION ALL ".join(
        f"SELECT {index + 1} AS step_number, :step_{index} AS step, count(reached_at) AS sessions "
        f"FROM stage_{index}"
        for index in range(len(steps))
    )
    funnel = spark.sql(
        f"""
        WITH {", ".join(stages)}, steps AS ({stage_rows})
        SELECT step_number, step, sessions,
               CASE WHEN lag(sessions) OVER (ORDER BY step_number) > 0
                    THEN round(sessions / lag(sessions) OVER (ORDER BY step_number), 4)
               END AS share_of_previous_step
        FROM steps ORDER BY step_number
    """,
        args={f"step_{index}": step for index, step in enumerate(steps)},
    )

    # Scaffold only observed weeks. An inactive observed week is zero; a future week stays absent.
    spark.range(weeks).withColumnRenamed("id", "weeks_later").createOrReplaceTempView(offset_view)
    retention_long = spark.sql(f"""
        WITH activity AS (
                 SELECT DISTINCT user_id, datediff(date(ts), DATE'{launch:%Y-%m-%d}') DIV 7 AS week FROM {event_view}),
             cohorts AS (SELECT user_id, min(week) AS cohort_week FROM activity GROUP BY user_id),
             sizes AS (SELECT cohort_week, count(*) AS cohort_users FROM cohorts GROUP BY cohort_week),
             counts AS (
                 SELECT c.cohort_week, a.week - c.cohort_week AS weeks_later, count(*) AS active_users
                 FROM activity a JOIN cohorts c ON a.user_id = c.user_id GROUP BY c.cohort_week, weeks_later),
             observed AS (
                 SELECT s.*, o.weeks_later FROM sizes s CROSS JOIN {offset_view} o
                 WHERE s.cohort_week + o.weeks_later < {weeks})
        SELECT o.cohort_week, o.weeks_later, o.cohort_users,
               coalesce(c.active_users, 0L) AS active_users,
               round(coalesce(c.active_users, 0L) / o.cohort_users, 4) AS retention
        FROM observed o LEFT JOIN counts c
        ON o.cohort_week = c.cohort_week AND o.weeks_later = c.weeks_later
        ORDER BY o.cohort_week, o.weeks_later
    """)
    # The same numbers as a cohort matrix: one row per cohort, one column per week since joining.
    retention_matrix = (
        retention_long.groupBy("cohort_week", "cohort_users")
        .pivot("weeks_later", list(range(weeks)))
        .agg(F.first("retention"))
        .orderBy("cohort_week")
    )
    return {"sessions": sessions, "funnel": funnel, "retention": retention_long, "matrix": retention_matrix}


def run(spark: SparkSession) -> dict[str, DataFrame]:
    return analyze_events(
        spark.createDataFrame(make_events(), "user_id long, ts timestamp_ntz, event string")
    )


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    for name in ("sessions", "funnel", "matrix"):
        results[name].show(truncate=False)
    spark.stop()
    server.stop()
