FACT = {
    "id": "catalogs-from-env",
    "category": "configure",
    "core": True,
    "text": """Configure catalogs with the `SAIL_CATALOG__LIST` environment variable (a TOML array) and
        `SAIL_CATALOG__DEFAULT_CATALOG` before the server starts; the Sail catalog docs cover Iceberg REST,
        Unity, Glue and Hive Metastore entries. JVM-style `spark.sql.catalog.<name>` settings do not
        create catalogs.""",
    "engines": ["sail"],
    "order": 2,
    "env": {
        "SAIL_CATALOG__LIST": '[{name="c1", type="memory", initial_database=["default"]}, '
        '{name="c2", type="memory", initial_database=["default"]}]',
        "SAIL_CATALOG__DEFAULT_CATALOG": "c1",
    },
}


def check(spark, engine):
    names = {c.name for c in spark.catalog.listCatalogs()}
    assert {"c1", "c2"} <= names, f"catalogs from SAIL_CATALOG__LIST missing: {names}"
    current = spark.catalog.currentCatalog()
    assert current == "c1", f"default catalog should be c1, got {current!r}"

    spark.conf.set("spark.sql.catalog.extra", "org.apache.iceberg.spark.SparkCatalog")
    after = {c.name for c in spark.catalog.listCatalogs()}
    assert "extra" not in after, f"spark.sql.catalog.* created a catalog: {after}"
    return {"catalogs": sorted(after), "current": current}
