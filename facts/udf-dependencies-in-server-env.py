FACT = {
    "id": "udf-dependencies-in-server-env",
    "category": "python-udfs",
    "core": True,
    "text": """`spark.addArtifacts()` is not supported, so Python files and packages cannot be shipped
        from the client at run time. Install every library your UDFs import into the Python environment the
        Sail server runs in.""",
    "engines": ["sail"],
    "order": 5,
}

import tempfile
from pathlib import Path


def check(spark, engine):
    module = Path(tempfile.mkdtemp()) / "helper_module.py"
    module.write_text("VALUE = 1\n")
    try:
        spark.addArtifacts(str(module), pyfile=True)
    except Exception as error:  # noqa: BLE001
        assert "add artifacts" in str(error).lower(), f"failed for another reason: {str(error)[:300]}"
        return {"error": type(error).__name__}
    raise AssertionError("addArtifacts now works; update this fact")
