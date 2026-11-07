FACT = {
    "id": "query-files-directly",
    "title": "Query files without loading a database",
    "category": "data-sources",
    "text": """Query files in place with `` FROM parquet.`path` ``, `` csv.`path` `` or `` json.`path` ``, where the
        path is a file, a folder, or a glob such as `data/2024-*.parquet` that reads every matching file as one
        table. A CSV file queried this way is read without a header: its first line is a data row and the
        columns are `_c0`, `_c1`, and so on. For a header and declared types, create a view:
        `CREATE TEMPORARY VIEW v (a INT, b STRING) USING csv OPTIONS (path '...', header 'true')`.""",
    "engines": ["sail", "spark"],
}

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def check(spark, engine):
    root = Path(tempfile.mkdtemp())
    (root / "people.csv").write_text("name,age\nada,36\n")
    (root / "people.json").write_text('{"name": "ada", "age": 36}\n')
    months = root / "months"  # one Parquet file per month, directly in the folder
    months.mkdir()
    for month, trips in (("2024-01", 3), ("2024-02", 4), ("2025-01", 5)):
        pq.write_table(pa.table({"month": [month], "trips": [trips]}), months / f"{month}.parquet")

    csv_rows = [tuple(r) for r in spark.sql(f"SELECT * FROM csv.`{root / 'people.csv'}`").collect()]
    assert csv_rows == [("name", "age"), ("ada", "36")], f"csv.`path` gave {csv_rows}"
    csv_columns = spark.sql(f"SELECT * FROM csv.`{root / 'people.csv'}`").columns
    assert csv_columns == ["_c0", "_c1"], f"csv.`path` columns are {csv_columns}"
    json_rows = [
        r.asDict() for r in spark.sql(f"SELECT name, age FROM json.`{root / 'people.json'}`").collect()
    ]
    assert json_rows == [{"name": "ada", "age": 36}], f"json.`path` gave {json_rows}"

    def months_in(path: str) -> list[str]:
        return sorted(r.month for r in spark.sql(f"SELECT month FROM parquet.`{path}`").collect())

    assert months_in(str(months / "2024-01.parquet")) == ["2024-01"], "a single Parquet path failed"
    assert months_in(str(months)) == ["2024-01", "2024-02", "2025-01"], "a folder path failed"
    assert months_in(f"{months}/2024-*.parquet") == ["2024-01", "2024-02"], "a glob path failed"
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY VIEW months_2024 USING parquet OPTIONS (path '{months}/2024-*.parquet')"
    )
    assert spark.table("months_2024").count() == 2, "a glob path in a view failed"

    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW people (name STRING, age INT) "
        f"USING csv OPTIONS (path '{root / 'people.csv'}', header 'true')"
    )
    people = [tuple(r) for r in spark.table("people").collect()]
    assert people == [("ada", 36)], f"the CSV view gave {people}"
    return {"csv_rows": csv_rows, "glob_months": months_in(f"{months}/2024-*.parquet")}
