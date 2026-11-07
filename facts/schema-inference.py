FACT = {
    "id": "schema-inference",
    "title": "Declare schemas when types or column order matter",
    "category": "data-sources",
    "core": True,
    "text": """Inferred schemas differ from Apache Spark's in three ways: CSV `inferSchema` reads whole
        numbers as `BIGINT` (Spark: `INT`); JSON keeps columns in file order (Spark sorts them by name); and
        partition columns of a partitioned directory come back as strings (Spark infers numbers and dates).
        Pass an explicit schema (`spark.read.schema(...)`) when types or column order matter.""",
    "engines": ["sail", "spark"],
    "order": 2,
}

import tempfile
from pathlib import Path


def check(spark, engine):
    directory = Path(tempfile.mkdtemp())
    (directory / "t.csv").write_text("i,f,s\n1,1.5,x\n2,2.5,y\n")
    (directory / "t.json").write_text('{"i": 1, "f": 1.5, "s": "x"}\n')
    spark.createDataFrame([(1.5, 1, "2024-01-01")], "x double, i int, d string").selectExpr(
        "x", "i", "CAST(d AS DATE) AS d"
    ).write.partitionBy("i", "d").parquet(str(directory / "parts"))

    csv = spark.read.csv(str(directory / "t.csv"), header=True, inferSchema=True).schema.simpleString()
    json = spark.read.json(str(directory / "t.json")).schema.simpleString()
    parts = spark.read.parquet(str(directory / "parts")).schema.simpleString()
    explicit = spark.read.schema("x double, i int, d date").parquet(str(directory / "parts")).schema
    assert explicit.simpleString() == "struct<x:double,i:int,d:date>", f"explicit schema gave {explicit}"
    observed = {"csv": csv, "json": json, "partitioned": parts}

    if engine == "spark":
        expected = {
            "csv": "struct<i:int,f:double,s:string>",
            "json": "struct<f:double,i:bigint,s:string>",
            "partitioned": "struct<x:double,i:int,d:date>",
        }
    else:
        expected = {
            "csv": "struct<i:bigint,f:double,s:string>",
            "json": "struct<i:bigint,f:double,s:string>",
            "partitioned": "struct<x:double,i:string,d:string>",
        }
    assert observed == expected, f"inferred schemas changed on {engine}: {observed}"
    return observed
