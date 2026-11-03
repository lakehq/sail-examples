"""The generated agent context must match its sources, stay within budget, and stay public."""

import subprocess
import sys

import pytest
from catalog import ROOT, discover_facts

BUILD = [sys.executable, str(ROOT / "tools" / "build_context.py")]
PRIVATE_FACT = (
    'FACT = {"id": "secret-trap", "category": "data-sources", "text": "x"}\n'
    "def check(spark, engine):\n    return None\n"
)


def build(*args):
    return subprocess.run([*BUILD, "--check", *args], capture_output=True, text=True, check=False)


def test_generated_context_is_up_to_date():
    result = build()
    assert result.returncode == 0, result.stdout + result.stderr + "\nRun: uv run tools/build_context.py"


def test_public_facts_are_not_private():
    assert not [f.id for f in discover_facts() if f.meta.get("private")]


@pytest.fixture
def private_dir(tmp_path):
    facts = tmp_path / "private-facts"
    facts.mkdir()
    (facts / "secret-trap.py").write_text(PRIVATE_FACT)
    return facts


def test_private_facts_need_a_private_root(private_dir):
    result = build("--private-facts-dir", str(private_dir), "--out-dir", str(ROOT))
    assert result.returncode == 2 and "refusing" in result.stdout, result.stdout + result.stderr


def test_private_facts_are_refused_outside_the_private_root(private_dir, tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    result = build(
        "--private-facts-dir", str(private_dir), "--private-root", str(root), "--out-dir", str(ROOT)
    )
    assert result.returncode == 2 and "refusing" in result.stdout, result.stdout + result.stderr


def test_private_root_check_ignores_path_case(private_dir, tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    variant = str(ROOT).swapcase()  # same directory on a case-insensitive disk, missing on a sensitive one
    result = build("--private-facts-dir", str(private_dir), "--private-root", str(root), "--out-dir", variant)
    assert result.returncode == 2 and "refusing" in result.stdout, result.stdout + result.stderr


def test_private_facts_are_allowed_inside_the_private_root(private_dir, tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    result = subprocess.run(
        [
            *BUILD,
            "--private-facts-dir",
            str(private_dir),
            "--private-root",
            str(root),
            "--out-dir",
            str(root),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Known issues" in (root / "agent-context.md").read_text()
