"""PR selection preserves relevant checks without executing unrelated examples."""

from types import SimpleNamespace

import pytest
from catalog import ROOT, Example, Fact
from change_selection import needs_runtime, select_changes, select_item


@pytest.fixture
def catalog(tmp_path):
    examples = []
    for name in ("first", "second"):
        folder = tmp_path / "quickstart" / name
        folder.mkdir(parents=True)
        (folder / "main.py").write_text("pass\n")
        (folder / "README.md").write_text("# Example\n")
        examples.append(Example(folder / "main.py", {"id": name}))
    fact = tmp_path / "facts" / "behavior.py"
    fact.parent.mkdir()
    fact.write_text("pass\n")
    return tmp_path, examples, [Fact(fact, {"id": "behavior"})]


def selection(catalog, paths, before=""):
    root, examples, facts = catalog
    return select_changes(paths, examples, facts, lambda path: before, root)


def test_one_example_selects_only_that_example(catalog):
    result = selection(catalog, ["quickstart/first/main.py", "quickstart/first/input.csv"])
    assert result == {"full": False, "examples": ["first"], "facts": [], "tests": []}
    assert needs_runtime(result)


def test_fact_change_does_not_select_examples(catalog):
    assert selection(catalog, ["facts/behavior.py"])["facts"] == ["behavior"]
    assert not selection(catalog, ["facts/behavior.py"])["examples"]


@pytest.mark.parametrize(
    "path", ["README.md", "quickstart/first/README.md", "site/site.css", "tools/build_site.py"]
)
def test_prose_and_website_changes_do_not_run_examples(catalog, path):
    assert not needs_runtime(selection(catalog, [path]))


def test_runnable_readme_diff_changes_select_the_example(catalog):
    root, _, _ = catalog
    diff = "```diff main.py variant=try-it\n-pass\n+print('result')\n```\n"
    (root / "quickstart/first/README.md").write_text("# Better explanation\n\n" + diff)
    assert selection(catalog, ["quickstart/first/README.md"])["examples"] == ["first"]
    assert not needs_runtime(
        selection(catalog, ["quickstart/first/README.md"], "# Old explanation\n\n" + diff)
    )
    assert selection(catalog, ["quickstart/first/README.md"], diff.replace("result", "changed"))[
        "examples"
    ] == ["first"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("engines", "sail-only"),
        ("ordered", "true"),
        ("float_tolerance", "0.001"),
        ("timeout", "600"),
        ("needs_key", "API_KEY"),
        ("needs_gpu", "true"),
        ("spark_extras", "[delta]"),
    ],
)
def test_runtime_metadata_changes_select_example(catalog, field, value):
    root, _, _ = catalog
    path = "quickstart/first/README.md"
    (root / path).write_text(f"---\ntitle: Example\n{field}: {value}\n---\n\n# Example\n")
    assert selection(catalog, [path], "---\ntitle: Example\n---\n")["examples"] == ["first"]


def test_reader_metadata_alone_does_not_select_runtime(catalog):
    root, _, _ = catalog
    path = "quickstart/first/README.md"
    (root / path).write_text("---\ntitle: Better title\nsummary: Clearer summary\n---\n")
    assert not needs_runtime(selection(catalog, [path], "---\ntitle: Example\n---\n"))


@pytest.mark.parametrize(
    "path",
    [
        ".python-version",
        "pyproject.toml",
        "uv.lock",
        "tests/harness.py",
        "tests/engines.toml",
        ".github/workflows/checks.yml",
        "shared/new_helper.py",
        "facts/deleted.py",
    ],
)
def test_shared_and_unknown_runtime_changes_select_everything(catalog, path):
    assert selection(catalog, [path])["full"]


def test_added_test_file_runs_without_selecting_all_examples(catalog):
    root, _, _ = catalog
    path = root / "tests/test_new_example.py"
    path.parent.mkdir()
    path.write_text("def test_example(): pass\n")
    result = selection(catalog, ["tests/test_new_example.py"])
    assert result["tests"] == ["tests/test_new_example.py"]
    assert not result["full"]
    assert needs_runtime(result)


def item(*, example=None, fact=None, owners=None, path="tests/test_custom.py"):
    params = {}
    if example:
        params["example"] = SimpleNamespace(id=example)
    if fact:
        params["fact"] = SimpleNamespace(id=fact)
    marker = SimpleNamespace(args=owners) if owners else None
    return SimpleNamespace(
        path=ROOT / path,
        callspec=SimpleNamespace(params=params),
        get_closest_marker=lambda name: marker,
    )


def test_collection_keeps_entrypoints_references_and_related_controls(catalog):
    selected = selection(catalog, ["quickstart/first/main.py"])
    assert select_item(item(example="first"), selected)
    assert not select_item(item(example="second"), selected)
    assert not select_item(item(fact="behavior"), selected)
    assert select_item(item(owners=["first"]), selected)
    assert not select_item(item(owners=["second"]), selected)
    assert select_item(item(), selected), "Unmarked checks must remain safe to add"


def test_changed_test_overrides_example_ownership():
    selected = {"full": False, "examples": [], "facts": [], "tests": ["tests/test_custom.py"]}
    assert select_item(item(owners=["second"]), selected)


def test_full_run_keeps_everything():
    assert select_item(item(example="second"), {"full": True})


def test_invalid_base_fails_instead_of_skipping_checks(catalog):
    import subprocess

    from change_selection import selection_since

    with pytest.raises(subprocess.CalledProcessError):
        selection_since("not-a-real-commit", ROOT)


def test_real_pytest_collection_scopes_entrypoints_and_custom_runtime_checks():
    import json
    import subprocess
    import sys

    program = """
import json, sys
sys.path.insert(0, 'tests')
import change_selection
import pytest
change_selection.selection_since = lambda base: {
    'full': False, 'examples': ['maintain-document-embeddings'], 'facts': [], 'tests': []
}
class Collected:
    def pytest_collection_finish(self, session):
        print('SELECTED=' + json.dumps([item.nodeid for item in session.items]))
raise SystemExit(pytest.main([
    '--collect-only', '-q', '--changed-since', 'simulated-pr',
    'tests/test_entry_points.py', 'tests/test_examples.py', 'tests/test_facts.py',
    'tests/test_embedding_maintenance.py', 'tests/test_robot_examples.py',
    'tests/test_inference_examples.py'
], plugins=[Collected()]))
"""
    result = subprocess.run(
        [sys.executable, "-c", program], cwd=ROOT, capture_output=True, text=True, check=True
    )
    line = next(line for line in result.stdout.splitlines() if line.startswith("SELECTED="))
    selected = json.loads(line.removeprefix("SELECTED="))
    assert "tests/test_entry_points.py::test_example_entry_point[maintain-document-embeddings]" in selected
    assert "tests/test_examples.py::test_example[maintain-document-embeddings]" in selected
    assert any(node.startswith("tests/test_embedding_maintenance.py::") for node in selected)
    assert all(
        node.startswith("tests/test_embedding_maintenance.py::") or "[maintain-document-embeddings]" in node
        for node in selected
    )
