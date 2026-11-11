"""Select runtime checks from a PR diff; unknown shared inputs select the full suite."""

import argparse
import json
import os
import subprocess
from pathlib import Path

from catalog import ROOT, RUNTIME_KEYS, discover_examples, discover_facts, parse_front_matter
from markdown_it import MarkdownIt


def runnable_diffs(text):
    return [
        (token.info, token.content)
        for token in MarkdownIt().parse(text)
        if token.type == "fence" and token.info.startswith("diff ") and " variant=" in token.info
    ]


def readme_runtime_inputs(text, source):
    metadata = parse_front_matter(text, source) if text.startswith("---\n") else {}
    return {key: value for key, value in metadata.items() if key in RUNTIME_KEYS}, runnable_diffs(text)


def select_changes(paths, examples, facts, read_before, root=ROOT):
    selected = {"full": False, "examples": set(), "facts": set(), "tests": set()}
    directories = {example.path.parent.relative_to(root).as_posix(): example.id for example in examples}
    fact_paths = {fact.path.relative_to(root).as_posix(): fact.id for fact in facts}
    for path in paths:
        if path in fact_paths:
            selected["facts"].add(fact_paths[path])
            continue
        directory = next((name for name in directories if path.startswith(name + "/")), None)
        if directory:
            if path == directory + "/README.md":
                current = (root / path).read_text() if (root / path).exists() else ""
                if readme_runtime_inputs(current, path) == readme_runtime_inputs(read_before(path), path):
                    continue
            selected["examples"].add(directories[directory])
        elif path.startswith("tests/test_") and path.endswith(".py") and (root / path).is_file():
            selected["tests"].add(path)
        elif (
            path.startswith(("site/", "tools/", ".github/skills/"))
            or path.endswith(".md")
            or path in {"package.json", "package-lock.json", ".gitignore", "llms.txt"}
        ):
            continue
        else:
            selected["full"] = True
    return {key: sorted(value) if isinstance(value, set) else value for key, value in selected.items()}


def selection_since(base, root=ROOT):
    revision = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{base}^{{commit}}"], cwd=root, text=True
    ).strip()
    changed = subprocess.check_output(
        ["git", "diff", "--no-renames", "--name-only", "-z", revision, "--"], cwd=root
    )
    paths = [path for path in changed.decode().split("\0") if path]

    def read_before(path):
        result = subprocess.run(
            ["git", "show", f"{revision}:{path}"], cwd=root, capture_output=True, text=True, check=False
        )
        return result.stdout if result.returncode == 0 else ""

    return select_changes(paths, discover_examples(root), discover_facts(root / "facts"), read_before, root)


def needs_runtime(selection):
    return selection["full"] or any(selection[key] for key in ("examples", "facts", "tests"))


def select_item(item, selection):
    if selection["full"]:
        return True
    path = item.path.relative_to(ROOT).as_posix()
    if path in selection["tests"]:
        return True
    parameters = getattr(getattr(item, "callspec", None), "params", {})
    if "example" in parameters and hasattr(parameters["example"], "id"):
        return parameters["example"].id in selection["examples"]
    if "fact" in parameters:
        return parameters["fact"].id in selection["facts"]
    owners = item.get_closest_marker("examples")
    if owners:
        return bool(set(owners.args) & set(selection["examples"]))
    # Unmarked checks still run, so missing ownership never silently skips coverage.
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="PR base commit; omit for a full run")
    args = parser.parse_args()
    selection = (
        selection_since(args.base) if args.base else {"full": True, "examples": [], "facts": [], "tests": []}
    )
    print(json.dumps(selection, indent=2))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a") as stream:
            stream.write(f"runtime={'true' if needs_runtime(selection) else 'false'}\n")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as stream:
            stream.write(
                "\n### Runtime check selection\n\n```json\n" + json.dumps(selection, indent=2) + "\n```\n"
            )


if __name__ == "__main__":
    main()
