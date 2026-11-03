FACT = {
    "id": "regex-character-classes",
    "category": "silent-behavior",
    "core": True,
    "text": """In regular expressions (`regexp_replace`, `regexp_extract`, `rlike`, `split`), Sail's `\\s`,
        `\\d` and `\\w` match Unicode characters (an ideographic space, an Arabic-Indic digit, `é`), while
        Apache Spark's match ASCII only, so the same pattern can return different results on non-ASCII text.
        For the same results on both, write the class you mean: `[ \\t\\n\\r]`, `[0-9]` and `[A-Za-z0-9_]`
        for ASCII, or `[\\p{Z}\\t\\n\\r]`, `\\p{Nd}` and `[\\p{L}\\p{Nd}_]` for Unicode. The `(?U)` flag is
        not a portable switch: Spark reads it as Unicode classes, Sail as lazy quantifiers.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

from pyspark.sql import functions as F

SAMPLE = "a　b c x٣7 é_a"  # ideographic space, Arabic-Indic digit three, e with acute
MIXED = "a\nb　c_1 é\tz"  # newline, ideographic space, underscore, ASCII digit, accented letter, tab


def check(spark, engine):
    df = spark.createDataFrame([(SAMPLE, "a   b", MIXED)], "s string, spaces string, mixed string")
    row = df.select(
        F.regexp_replace("s", r"\s", "_").alias("space"),
        F.regexp_extract("s", r"(\d+)", 1).alias("digits"),
        F.regexp_replace("s", r"\w", "W").alias("word"),
        F.regexp_replace("s", r"\p{Z}", "_").alias("unicode_space"),
        F.regexp_replace("s", r"[ \t\n\r]", "_").alias("ascii_space"),
        F.regexp_replace("s", r"\p{Nd}", "D").alias("unicode_digit"),
        F.regexp_replace("s", r"\p{L}", "L").alias("unicode_letter"),
        F.regexp_replace("spaces", r"(?U)\s+", "_").alias("u_flag"),
        F.size(F.split("s", r"\s+")).alias("split_parts"),
        F.regexp_replace("mixed", r"[\p{Z}\t\n\r]+", "_").alias("mixed_unicode_space"),
        F.regexp_replace("mixed", r"[ \t\n\r]+", "_").alias("mixed_ascii_space"),
        F.regexp_replace("mixed", r"[0-9]", "D").alias("mixed_ascii_digit"),
        F.regexp_replace("mixed", r"[A-Za-z0-9_]", "W").alias("mixed_ascii_word"),
        F.regexp_replace("mixed", r"[\p{L}\p{Nd}_]", "W").alias("mixed_unicode_word"),
        F.col("s").rlike(r"^a\sb").alias("rlike_space"),
    ).first()

    # Explicit classes give the same answer on both engines.
    assert row.unicode_space == "a_b_c_x٣7_é_a", f"\\p{{Z}} gave {row.unicode_space!r}"
    assert row.ascii_space == "a　b_c_x٣7_é_a", f"[ \\t\\n\\r] gave {row.ascii_space!r}"
    assert row.unicode_digit == "a　b c xDD é_a", f"\\p{{Nd}} gave {row.unicode_digit!r}"
    assert row.unicode_letter == "L　L L L٣7 L_L", f"\\p{{L}} gave {row.unicode_letter!r}"
    assert row.mixed_unicode_space == "a_b_c_1_é_z", f"[\\p{{Z}}\\t\\n\\r]+ gave {row.mixed_unicode_space!r}"
    assert row.mixed_ascii_space == "a_b　c_1_é_z", f"[ \\t\\n\\r]+ gave {row.mixed_ascii_space!r}"
    assert row.mixed_ascii_digit == "a\nb　c_D é\tz", f"[0-9] gave {row.mixed_ascii_digit!r}"
    assert row.mixed_ascii_word == "W\nW　WWW é\tW", f"[A-Za-z0-9_] gave {row.mixed_ascii_word!r}"
    assert row.mixed_unicode_word == "W\nW　WWW W\tW", f"[\\p{{L}}\\p{{Nd}}_] gave {row.mixed_unicode_word!r}"

    if engine == "sail":
        assert row.space == "a_b_c_x٣7_é_a", f"Sail \\s is no longer Unicode: {row.space!r}"
        assert row.digits == "٣7", f"Sail \\d is no longer Unicode: {row.digits!r}"
        assert row.word == "W　W W WWW WWW", f"Sail \\w is no longer Unicode: {row.word!r}"
        assert row.u_flag == "a___b", f"Sail (?U) is no longer lazy: {row.u_flag!r}"
        assert (row.split_parts, row.rlike_space) == (5, True), f"Sail split/rlike: {row}"
    else:
        assert row.space == "a　b_c_x٣7_é_a", f"Spark \\s matched non-ASCII: {row.space!r}"
        assert row.digits == "7", f"Spark \\d matched non-ASCII: {row.digits!r}"
        assert row.word == "W　W W W٣W éWW", f"Spark \\w matched non-ASCII: {row.word!r}"
        assert row.u_flag == "a_b", f"Spark (?U) changed: {row.u_flag!r}"
        assert (row.split_parts, row.rlike_space) == (4, False), f"Spark split/rlike: {row}"
    return {"space": row.space, "digits": row.digits, "word": row.word, "u_flag": row.u_flag}
