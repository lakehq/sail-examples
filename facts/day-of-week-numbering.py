FACT = {
    "id": "day-of-week-numbering",
    "category": "silent-behavior",
    "text": """Use `dayofweek(d)` (Sunday 1 to Saturday 7) for the day of the week: it agrees on Sail and
        Apache Spark. The other forms differ on Sail, for dates and timestamps alike: `EXTRACT(DOW FROM d)`
        and `date_part('dow', d)` count from Sunday = 0 (Spark: 1), `weekday(d)` returns -1 for Sunday
        (Spark: 6; Monday to Saturday agree, 0 to 5), and `EXTRACT(DAYOFWEEK FROM d)` and
        `EXTRACT(DOW_ISO FROM d)` are not supported.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

# Sunday 2024-01-07 to Saturday 2024-01-13, as dates, timestamps and timestamps without a time zone.
WEEKS = {
    "date": ("DATE'2024-01-07'", "DATE'2024-01-13'", ""),
    "timestamp": ("TIMESTAMP'2024-01-07 12:00:00'", "TIMESTAMP'2024-01-13 12:00:00'", ", INTERVAL 1 DAY"),
    "timestamp_ntz": (
        "TIMESTAMP_NTZ'2024-01-07 12:00:00'",
        "TIMESTAMP_NTZ'2024-01-13 12:00:00'",
        ", INTERVAL 1 DAY",
    ),
}
SQL = """SELECT dayofweek(d) AS dayofweek, weekday(d) AS weekday, extract(DOW FROM d) AS dow,
                date_part('dow', d) AS part_dow
         FROM (SELECT explode(sequence({first}, {last}{step})) AS d) ORDER BY d"""
EXPECTED = {
    "spark": {
        "weekday": [6, 0, 1, 2, 3, 4, 5],
        "dow": [1, 2, 3, 4, 5, 6, 7],
        "part_dow": [1, 2, 3, 4, 5, 6, 7],
    },
    "sail": {
        "weekday": [-1, 0, 1, 2, 3, 4, 5],
        "dow": [0, 1, 2, 3, 4, 5, 6],
        "part_dow": [0, 1, 2, 3, 4, 5, 6],
    },
}


def error_for(spark, sql: str) -> str | None:
    try:
        spark.sql(sql).collect()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    observed = {}
    for kind, (first, last, step) in WEEKS.items():
        rows = spark.sql(SQL.format(first=first, last=last, step=step)).collect()
        values = {name: [r[name] for r in rows] for name in ("dayofweek", "weekday", "dow", "part_dow")}
        assert values["dayofweek"] == [1, 2, 3, 4, 5, 6, 7], (
            f"dayofweek on a {kind} gave {values['dayofweek']}"
        )
        values.pop("dayofweek")
        assert values == EXPECTED[engine], f"on a {kind}, {engine} gave {values}: update this fact"
        unsupported = {
            field: error_for(spark, f"SELECT extract({field} FROM {first})")
            for field in ("DAYOFWEEK", "DOW_ISO")
        }
        if engine == "spark":
            assert not any(unsupported.values()), f"Spark rejected a date part on a {kind}: {unsupported}"
        else:
            for field, error in unsupported.items():
                assert error and field.lower() in error.lower(), (
                    f"EXTRACT({field}) on a {kind} on Sail: {error}"
                )
        observed[kind] = values
    return observed
