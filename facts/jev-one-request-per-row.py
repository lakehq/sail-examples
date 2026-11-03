FACT = {
    "id": "jev-one-request-per-row",
    "category": "ai-functions",
    "text": """A Jev function asks one question per row it evaluates. Adjacent rows with the same text
        share one request, but each row is still its own question, and the same text in rows that are not
        adjacent is sent again. Apply Jev to `SELECT DISTINCT` inputs, save the answers, and join them back
        to ask once per distinct value. The endpoint comes from `TYPESAFE_BASE_URL` (per the Sail Jev docs).""",
    "engines": ["sail"],
    "order": 3,
}

import os

JEV = "jev_choice(t, 'Route', map('billing', 'Payments', 'bug', 'Errors')).choice"


def ask(spark, mock, sql: str) -> tuple[int, int, int]:
    mock.reset()
    rows = len(spark.sql(sql).collect())
    questions = sum(len(r["body"]["questions"]) for r in mock.requests)
    return rows, mock.request_count, questions


def check(spark, engine):
    from pysail.testing.jev import JevMock

    saved = {k: os.environ.get(k) for k in ("TYPESAFE_BASE_URL", "TYPESAFE_API_KEY")}
    with JevMock() as mock:
        os.environ["TYPESAFE_BASE_URL"] = mock.url
        os.environ["TYPESAFE_API_KEY"] = "local-stand-in"
        try:
            adjacent = ask(spark, mock, f"SELECT {JEV} FROM (SELECT 'same text' AS t FROM range(5))")
            alternating = ask(
                spark,
                mock,
                f"SELECT {JEV} FROM (SELECT IF(id % 2 = 0, 'text a', 'text b') AS t FROM range(10))",
            )
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
    assert adjacent == (5, 1, 5), f"5 adjacent identical rows: (rows, requests, questions) = {adjacent}"
    assert alternating == (10, 10, 10), f"10 alternating rows: (rows, requests, questions) = {alternating}"
    return {"adjacent_identical": adjacent, "alternating_two_texts": alternating}
