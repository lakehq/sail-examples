FACT = {
    "id": "decimal-division",
    "category": "silent-behavior",
    "core": True,
    "text": """Dividing DECIMAL values gives a different result type on Sail than on Apache Spark
        (sometimes fewer, sometimes more decimal places), and Sail truncates where Spark rounds. `7 / 60.0` is 0.1166 on Sail and 0.116667 on Spark; `CAST(2 AS DECIMAL(10,0)) /
        CAST(3 AS DECIMAL(10,0))` is 0.6666 and 0.66666666667. A literal with a decimal point, such as `60.0`,
        is a DECIMAL. For the same results on both, divide DOUBLE values (`x / 60.0D`,
        `CAST(x AS DOUBLE) / y`) or integers (`x / 60`, which returns a DOUBLE on both), and round
        explicitly when you need a fixed number of digits.""",
    "engines": ["sail", "spark"],
    "order": 5,
}

SQL = """SELECT 7 / 60.0 AS literal_quotient, typeof(7 / 60.0) AS literal_type,
                CAST(2 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)) AS two_thirds,
                typeof(CAST(1 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2))) AS narrow_type,
                typeof(CAST(1 AS DECIMAL(38,10)) / CAST(3 AS DECIMAL(38,10))) AS wide_type,
                7 / 60 AS integer_quotient, typeof(7 / 60) AS integer_type,
                7 / 60.0D AS double_quotient, round(7 / 60.0D, 4) AS rounded"""

EXPECTED = {
    "spark": {
        "literal_quotient": "0.116667",
        "literal_type": "decimal(8,6)",
        "two_thirds": "0.66666666667",
        "narrow_type": "decimal(23,13)",
        "wide_type": "decimal(38,6)",
        "average": "0.0388890000",
    },
    "sail": {
        "literal_quotient": "0.1166",
        "literal_type": "decimal(15,4)",
        "two_thirds": "0.6666",
        "narrow_type": "decimal(16,6)",
        "wide_type": "decimal(38,14)",
        "average": "0.03883333",
    },
}


def check(spark, engine):
    row = spark.sql(SQL).first()
    # The DOUBLE and integer forms agree on both engines.
    assert row.integer_type == "double" and abs(row.integer_quotient - 7 / 60) < 1e-15, row
    assert abs(row.double_quotient - 7 / 60) < 1e-15, row
    assert row.rounded == 0.1167, f"round(7 / 60.0D, 4) gave {row.rounded}"
    average = spark.sql("SELECT avg(x / 60.0) FROM VALUES (1L), (2L), (4L) AS t(x)").first()[0]
    observed = {
        "literal_quotient": str(row.literal_quotient),
        "literal_type": row.literal_type,
        "two_thirds": str(row.two_thirds),
        "narrow_type": row.narrow_type,
        "wide_type": row.wide_type,
        "average": str(average),
    }
    assert observed == EXPECTED[engine], f"decimal division changed on {engine}: {observed}"
    return observed
