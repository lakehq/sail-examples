"""Every example declares a Python version its pinned packages support, and pins every package.

An example that pins a package the Sail engine also pins uses the same version, so a release bump in
tests/engines.toml cannot leave an example behind. Metadata that names Spark extras must name ones that
tests/engines.toml defines.
"""

import re
import tomllib
from pathlib import Path

import pytest
from catalog import check_spark_extras, discover_examples

EXAMPLES = discover_examples()
REQUIRES_PYTHON = ">=3.12"  # pandas 3 and pyarrow 26 need 3.11, numpy 2.5 needs 3.12
ENGINES = tomllib.loads((Path(__file__).resolve().parent / "engines.toml").read_text())


def package(dep: str) -> str:
    return re.split(r"[=<>!~\[; ]", dep, maxsplit=1)[0].lower()


SAIL_PINS = {package(dep): dep for dep in ENGINES["sail"]["deps"]}


HEADER = re.compile(r"^# /// script\n((?:#.*\n)*?)# ///$", re.MULTILINE)


def header(path):
    match = HEADER.search(path.read_text())
    assert match, f"{path} has no PEP 723 header"
    body = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in match.group(1).splitlines())
    return tomllib.loads(body)


def scripts(example) -> list[Path]:
    """The example's main.py and any other script beside it with its own PEP 723 header."""
    return [path for path in sorted(example.path.parent.glob("*.py")) if HEADER.search(path.read_text())]


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_header_python_and_pins(example):
    meta = header(example.path)
    assert meta.get("requires-python") == REQUIRES_PYTHON, (
        f"requires-python is {meta.get('requires-python')!r}"
    )
    unpinned = [dep for dep in meta.get("dependencies", []) if "==" not in dep]
    assert not unpinned, f"unpinned dependencies: {unpinned}"


@pytest.mark.parametrize("example", EXAMPLES, ids=[e.id for e in EXAMPLES])
def test_header_matches_engine_pins(example):
    drifted = [
        f"{path.name}: {dep}"
        for path in scripts(example)
        for dep in header(path).get("dependencies", [])
        if package(dep) in SAIL_PINS and dep != SAIL_PINS[package(dep)]
    ]
    assert not drifted, f"{drifted} differ from tests/engines.toml {sorted(SAIL_PINS.values())}"


@pytest.mark.parametrize(
    "extras",
    [["nope"], "delta", [1]],
    ids=["unknown-name", "not-a-list", "not-a-name"],
)
def test_unknown_spark_extras_are_rejected(extras):
    with pytest.raises(ValueError, match="spark_extras"):
        check_spark_extras(Path("README.md"), {"spark_extras": extras})


def test_known_spark_extras_are_accepted():
    check_spark_extras(Path("README.md"), {"spark_extras": ["delta"]})
    check_spark_extras(Path("README.md"), {})
