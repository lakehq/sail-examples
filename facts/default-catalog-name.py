FACT = {
    "id": "default-catalog-name",
    "title": "The default catalog is named sail",
    "category": "configure",
    "core": True,
    "text": """With no catalog configured, the current catalog is named `sail`, not `spark_catalog`.
        Avoid hard-coding `spark_catalog` in three-part table names.""",
    "engines": ["sail", "spark"],
    "order": 3,
}


def check(spark, engine):
    current = spark.catalog.currentCatalog()
    expected = "sail" if engine == "sail" else "spark_catalog"
    assert current == expected, f"expected {expected!r}, got {current!r}"
    return {"current_catalog": current}
