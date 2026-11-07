FACT = {
    "id": "json-multiline-unsupported",
    "title": "Use JSON Lines for the JSON reader",
    "category": "data-sources",
    "text": """The JSON reader's `multiLine` option is not supported: JSON Lines files read correctly, but
        a pretty-printed JSON document fails to parse. Convert such files to JSON Lines, or read each file
        through a table of paths and parse it with `json.loads` inside a UDF.""",
    "engines": ["sail", "spark"],
    "order": 3,
}

import json
import tempfile
from pathlib import Path


def check(spark, engine):
    directory = Path(tempfile.mkdtemp())
    (directory / "doc.json").write_text(json.dumps({"a": 1, "b": [1, 2]}, indent=2))
    (directory / "lines").mkdir()
    (directory / "lines" / "rows.json").write_text('{"a": 1}\n{"a": 2}\n')

    lines = spark.read.json(str(directory / "lines")).count()
    assert lines == 2, f"JSON Lines should read 2 rows, got {lines}"
    try:
        rows = spark.read.option("multiLine", "true").json(str(directory / "doc.json")).collect()
    except Exception as error:  # noqa: BLE001
        assert engine == "sail", f"multiLine JSON failed on Spark: {error}"
        assert "not valid json" in str(error).lower(), f"failed for another reason: {error}"
        return {"json_lines_rows": lines, "multiline": type(error).__name__}
    assert engine == "spark", f"multiLine JSON now works on Sail ({len(rows)} rows); update this fact"
    return {"json_lines_rows": lines, "multiline_rows": len(rows)}
