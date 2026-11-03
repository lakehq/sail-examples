---
title: Explore a year of taxi trips with SQL
id: explore-taxi-trips
description: Use when you want to analyze a real dataset on your laptop with SQL (read a folder of Parquet files as one table, filter out bad records, trend by month, find peak hours, compute exact medians and percentiles, rank within groups).
tier: analytics
level: beginner
tags: [sql, parquet, analytics, window-functions, percentiles, data-cleaning, nyc-taxi]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 180
needs_key: false
needs_gpu: false
prompt: Download the twelve 2024 NYC green taxi Parquet files and the zone lookup, read the folder as one table with Spark SQL, drop implausible trips, then report trips and fares by month with the change from the previous month, the busiest hours of the week, median fare and 90th percentile distance per borough, and the top two pickup zones in each borough.
---

# Explore a year of taxi trips with SQL

This example analyzes every NYC green taxi trip record from 2024, 660,218 records in twelve monthly Parquet files (16 MB). It reads the folder as one table, keeps the plausible trips, and answers the questions an analyst asks first, each with one SQL query.

## Run it

```bash
uv run main.py
```

The first run downloads the files into `.data/` next to the script.

Of the 660,218 records, 635,426 pass the checks. Trips peak on weekday evenings: the busiest hour of the week is Thursday at 5 pm. Manhattan has the most pickups, and East Harlem North is its busiest zone.

## How it works

1. **A folder is a table.** `CREATE TEMPORARY VIEW raw_trips USING parquet OPTIONS (path 'trips')` reads all twelve files as one table.
2. **Clean before you count.** A second view keeps trips picked up in 2024 that last 1 to 180 minutes, cover at most 100 miles, and have no negative fare. Real records include a trip of 201,421 miles and pickups dated 2008 and 2025, so averages without this step mislead. The `cleaning` result shows how many records the checks kept and dropped.
3. **Trend with a window function.** Trips and fares are grouped by month, and `lag(trips) OVER (ORDER BY month)` gives the previous month, so each row shows its change.
4. **Peak hours.** Trips are grouped by day of the week and hour. `dayofweek()` numbers Sunday 1 to Saturday 7, and `date_format(ts, 'EEEE')` gives the day's name.
5. **Exact statistics.** `median()` and `percentile()` compute exact values per borough, and a `CASE` inside `avg` gives the share of card payments (payment type 1 in the TLC data dictionary).
6. **Rank within groups.** `rank() OVER (PARTITION BY borough ORDER BY count(*) DESC)` numbers the zones inside each borough, and the outer query keeps the top two.

The zone lookup includes the boroughs `Unknown` and `N/A` (outside NYC), so they appear in the results too.

## On Sail

- Use `dayofweek()` for the day of the week. `EXTRACT(DOW FROM ts)` numbers Sunday 0 on Sail and 1 on Apache Spark.
- Use `percentile()` and `median()` when results must be exact. `percentile_approx` can return different approximations on Sail and on Apache Spark.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January to December 2024, and the taxi zone lookup table. Codes are defined in the TLC's [data dictionary for green taxi trips](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf).
