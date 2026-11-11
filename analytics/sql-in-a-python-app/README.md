---
title: Use SQL inside a Python app with pandas
id: sql-in-a-python-app
facts: [start-and-connect]
summary: Query pandas data with SQL and bring the results back to Python.
description: Use when a Python program (a script, a service, a notebook) keeps data in pandas and you want to query it with SQL, safely parameterized, and get pandas or Arrow results back.
tier: analytics
level: beginner
tags: [sql, pandas, arrow, parameters, sql-injection, embedded, python]
modality: tabular
engines: both
ordered: true
float_tolerance: 1e-9
timeout: 120
needs_key: false
needs_gpu: false
prompt: Load NYC taxi trips and the zone lookup into pandas, register them as views in a Spark session, write a function that runs a parameterized SQL query (borough and date range as parameters) and returns a pandas DataFrame, and return a daily summary as an Arrow table.
---

# Use SQL inside a Python app with pandas

Your app already has its data in pandas. This example uses Sail as the SQL engine inside the program: it registers pandas DataFrames as views, answers questions with parameterized SQL, and returns pandas DataFrames or Arrow tables to the rest of the code. The data is real: NYC green taxi trips from January 2024 and the taxi zone lookup, published by the NYC Taxi and Limousine Commission (TLC).

## Run it

Run all commands below from the repository root (`sail-examples/`):

```bash
uv run analytics/sql-in-a-python-app/main.py
```

The first run downloads the two files (1.4 MB) into `analytics/sql-in-a-python-app/.data/`.

It prints the busiest Manhattan pickup zones in the first week of January 2024. East Harlem North leads with 2,578 trips.

<!-- sail-output:start variant=default -->

## Example output

Current output · Sail 0.7.3.

```text
Busiest Manhattan pickup zones, January 1 to 7, 2024:
               zone  trips  median_fare
  East Harlem North   2578         12.8
  East Harlem South   1516         10.7
     Central Harlem    631         11.4
Morningside Heights    524         13.5
       Central Park    462         11.4
```

<!-- sail-output:end -->

## How it works

- **pandas in.** `spark.createDataFrame(pandas_df)` turns each pandas DataFrame into a Spark DataFrame, and `createOrReplaceTempView` names it for SQL. The trip dates become a `DATE` column. pandas reads the payment type as `float64`, because the column has missing values, so it is cast to pandas' nullable `Int64` first and arrives as a `BIGINT` column. The zone lookup is read with `keep_default_na=False`, so its literal `N/A` labels stay text instead of becoming missing values.
- **Parameterized SQL.** `zone_report` passes the borough, the dates and the row limit to `spark.sql(query, args={...})`. The `:borough`, `:first_day`, `:last_day` and `:top` markers in the query are filled in as values, never as SQL text. Input written to change the query (`Queens' OR '1'='1`) is just a borough name that matches no rows.
- **pandas out.** `.toPandas()` returns the result as a pandas DataFrame for the rest of the program.
- **Arrow out.** `.toArrow()` returns a `pyarrow.Table`, which libraries that read Arrow can use directly.

## Use it in your app

Start one session when the app starts and reuse it for every query. The in-process server in `__main__` needs no Java and no separate service. To run Sail as a separate service instead, start `sail spark server` (it listens on `127.0.0.1:50051` by default, per the Sail docs) and connect with `SparkSession.builder.remote("sc://localhost:50051")`.

## Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), green taxi trips for January 2024, and the taxi zone lookup table.
