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

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
cleaning
+-------+------+-------+
|records|kept  |dropped|
+-------+------+-------+
|660218 |635426|24792  |
+-------+------+-------+

monthly
+-----+-----+----------+--------------------------+
|month|trips|fares     |change_from_previous_month|
+-----+-----+----------+--------------------------+
|1    |54592|916368.74 |NULL                      |
|2    |51757|876578.54 |-0.0519                   |
|3    |55509|956792.12 |0.0725                    |
|4    |54423|952515.09 |-0.0196                   |
|5    |58725|1082830.56|0.079                     |
|6    |52693|986915.97 |-0.1027                   |
|7    |49719|913957.82 |-0.0564                   |
|8    |49608|966984.12 |-0.0022                   |
|9    |52249|1052502.82|0.0532                    |
|10   |54070|1013853.08|0.0349                    |
|11   |50275|903758.94 |-0.0702                   |
|12   |51806|923518.21 |0.0305                    |
+-----+-----+----------+--------------------------+

busiest_hours
+---------+----------+----+-----+
|day      |day_number|hour|trips|
+---------+----------+----+-----+
|Thursday |5         |17  |8921 |
|Wednesday|4         |17  |8559 |
|Friday   |6         |17  |8236 |
|Thursday |5         |18  |8235 |
|Tuesday  |3         |17  |7949 |
+---------+----------+----+-----+

boroughs
+-------------+------+-----------+---------+----------+
|borough      |trips |median_fare|p90_miles|card_share|
+-------------+------+-----------+---------+----------+
|Manhattan    |381224|12.8       |4.91     |0.7397    |
|Queens       |155060|15.6       |7.85     |0.5691    |
|Brooklyn     |89781 |17.7       |7.78     |0.735     |
|Bronx        |8622  |22.5       |9.55     |0.5264    |
|Unknown      |416   |13.75      |6.04     |0.5601    |
|N/A          |296   |50.0       |21.23    |0.4088    |
|Staten Island|23    |33.84      |20.37    |0.4783    |
|EWR          |4     |137.0      |0.63     |0.25      |
+-------------+------+-----------+---------+----------+

top_zones
+-------------+---------------------------+------+----+
|borough      |zone                       |trips |rank|
+-------------+---------------------------+------+----+
|Bronx        |West Concourse             |1872  |1   |
|Bronx        |Melrose South              |651   |2   |
|Brooklyn     |Fort Greene                |21087 |1   |
|Brooklyn     |Downtown Brooklyn/MetroTech|18648 |2   |
|EWR          |Newark Airport             |4     |1   |
|Manhattan    |East Harlem North          |147612|1   |
|Manhattan    |East Harlem South          |90116 |2   |
|N/A          |Outside of NYC             |296   |1   |
|Queens       |Forest Hills               |31792 |1   |
|Queens       |Elmhurst                   |29515 |2   |
|Staten Island|Saint George/New Brighton  |10    |1   |
|Staten Island|Arrochar/Fort Wadsworth    |2     |2   |
+-------------+---------------------------+------+----+
only showing top 12 rows

```

<!-- sail-output:end -->

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
