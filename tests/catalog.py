"""Discover examples and facts, and read their metadata without importing them.

An example is a directory with a `main.py` and a `README.md` whose YAML front
matter describes it. A fact is a `facts/<id>.py` file with a module-level
`FACT` dict literal and a `check(spark, engine)` function.
"""

import ast
import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TIERS = ("quickstart", "analytics", "pipelines", "python", "ai", "multimodal", "migrate", "agents")
LEVELS = ("beginner", "intermediate", "advanced")
ENGINE_PACKAGES = re.compile(r"^(pysail|pyspark(-client|\[connect\])?|pyarrow|pandas)\b")
SPARK_EXTRAS = set(
    tomllib.loads((Path(__file__).resolve().parent / "engines.toml").read_text())["spark"].get("extras", {})
)

EXAMPLE_KEYS = {
    "title",
    "id",
    "description",
    "tier",
    "level",
    "tags",
    "modality",
    "engines",
    "ordered",
    "float_tolerance",
    "timeout",
    "needs_key",
    "needs_gpu",
    "prompt",
    "spark_extras",
}
FACT_KEYS = {
    "id",
    "category",
    "text",
    "engines",
    "timeout",
    "env",
    "order",
    "private",
    "core",
    "spark_extras",
}
FACT_CATEGORIES = {
    "run-and-connect",
    "configure",
    "python-udfs",
    "data-sources",
    "missing-features",
    "silent-behavior",
    "ai-functions",
}


@dataclass
class Example:
    path: Path
    meta: dict
    extra_deps: list[str] = field(default_factory=list)

    @property
    def id(self) -> str:
        return self.meta["id"]

    @property
    def engines(self) -> tuple[str, ...]:
        return ("sail",) if self.meta.get("engines") == "sail-only" else ("sail", "spark")


@dataclass
class Fact:
    path: Path
    meta: dict
    extra_deps: list[str] = field(default_factory=list)

    @property
    def id(self) -> str:
        return self.meta["id"]

    @property
    def engines(self) -> tuple[str, ...]:
        return tuple(self.meta.get("engines", ("sail",)))


def check_spark_extras(source: Path, meta: dict) -> None:
    """Every `spark_extras` entry must name a [spark.extras.<name>] section of engines.toml."""
    if "spark_extras" not in meta:
        return
    extras = meta["spark_extras"]
    if not isinstance(extras, list) or not all(isinstance(name, str) for name in extras):
        raise ValueError(f"{source}: spark_extras must be a list of names, not {extras!r}")
    unknown = sorted(set(extras) - SPARK_EXTRAS)
    if unknown:
        raise ValueError(
            f"{source}: spark_extras {unknown} not found in tests/engines.toml "
            f"(known: {sorted(SPARK_EXTRAS)}); add a [spark.extras.<name>] section or fix the name"
        )


def read_front_matter(readme: Path) -> dict:
    text = readme.read_text()
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    if not match:
        raise ValueError(f"{readme} has no YAML front matter")
    return yaml.safe_load(match.group(1))


def script_dependencies(script: Path) -> list[str]:
    """Return the PEP 723 dependencies of a script, minus the engine packages."""
    match = re.search(r"^# /// script\n((?:#.*\n)*?)# ///$", script.read_text(), re.MULTILINE)
    if not match:
        return []
    body = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in match.group(1).splitlines())
    deps = tomllib.loads(body).get("dependencies", [])
    return [dep for dep in deps if not ENGINE_PACKAGES.match(dep)]


def discover_examples(root: Path = ROOT) -> list[Example]:
    examples = []
    for tier in TIERS:
        for readme in sorted((root / tier).glob("*/README.md")):
            meta = read_front_matter(readme)
            unknown = set(meta) - EXAMPLE_KEYS
            if unknown:
                raise ValueError(f"{readme}: unknown front matter keys {sorted(unknown)}")
            if meta.get("tier") != tier:
                raise ValueError(f"{readme}: tier {meta.get('tier')!r} does not match folder {tier!r}")
            if meta.get("level") not in LEVELS:
                raise ValueError(f"{readme}: level must be one of {LEVELS}, not {meta.get('level')!r}")
            check_spark_extras(readme, meta)
            main = readme.parent / "main.py"
            examples.append(Example(path=main, meta=meta, extra_deps=script_dependencies(main)))
    return examples


def discover_facts(facts_dir: Path = ROOT / "facts") -> list[Fact]:
    facts = []
    for path in sorted(facts_dir.glob("*.py")):
        tree = ast.parse(path.read_text())
        literal = next(
            (
                node.value
                for node in tree.body
                if isinstance(node, ast.Assign)
                and any(getattr(t, "id", None) == "FACT" for t in node.targets)
            ),
            None,
        )
        if literal is None:
            raise ValueError(f"{path} has no FACT dict")
        meta = ast.literal_eval(literal)
        unknown = set(meta) - FACT_KEYS
        if unknown:
            raise ValueError(f"{path}: unknown FACT keys {sorted(unknown)}")
        if meta["category"] not in FACT_CATEGORIES:
            raise ValueError(f"{path}: unknown category {meta['category']!r}")
        if meta["id"] != path.stem:
            raise ValueError(f"{path}: FACT id must equal the file name")
        check_spark_extras(path, meta)
        if meta.get("private") and os.path.samefile(facts_dir, ROOT / "facts"):
            raise ValueError(f"{path}: private facts must not live in the public facts/ directory")
        facts.append(Fact(path=path, meta=meta, extra_deps=script_dependencies(path)))
    return facts
