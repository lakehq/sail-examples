FACT = {
    "id": "jev-functions",
    "category": "ai-functions",
    "text": """Sail adds SQL functions that call TypeSafe's Jev models: `jev_noul`, `jev_choice`,
        `jev_score`, `jev_system_one` and `jev_models` (Apache Spark has none of them). They need a non-empty
        `TYPESAFE_API_KEY` in the Sail server's environment, and on a cluster in every worker that runs Jev
        calls (per the Sail Jev docs); with an empty key a call fails with an error that names the variable.
        The calls send row values to the TypeSafe API (per the Sail Jev docs).""",
    "engines": ["sail", "spark"],
    "order": 2,
    "env": {"TYPESAFE_API_KEY": ""},
}

CALLS = {
    "jev_noul": "jev_noul('state', 'instructions')",
    "jev_choice": "jev_choice('state', 'instructions', array('yes', 'no'))",
    "jev_score": "jev_score('state', 'instructions', array('low', 'high'))",
    "jev_system_one": "jev_system_one('state', map('q1', 'question'))",
    "jev_models": "jev_models()",
}


def error_for(spark, sql: str) -> str | None:
    try:
        spark.sql(f"SELECT {sql}").collect()
    except Exception as error:  # noqa: BLE001
        return " ".join(str(error).split())
    return None


def check(spark, engine):
    errors = {name: error_for(spark, sql) for name, sql in CALLS.items()}
    if engine == "spark":
        assert all(e is not None for e in errors.values()), f"a Jev function ran on Spark: {errors}"
        return {"spark": "all five unresolved"}

    for name, error in errors.items():
        assert error is not None, f"{name} ran with an empty API key; update this fact"
        assert "unknown function" not in error.lower(), f"{name} is not a known function: {error[:200]}"
    assert "TYPESAFE_API_KEY" in errors["jev_models"], f"unexpected error: {errors['jev_models'][:300]}"
    return {"key_error_names_variable": True, "known_functions": sorted(errors)}
