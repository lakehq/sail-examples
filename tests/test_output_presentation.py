"""Generated README sections preserve prose and never accept execution results."""

import hashlib
import json
import subprocess
import sys

import pytest
from catalog import ROOT, discover_examples
from harness import record_output, record_output_failure
from markdown_it import MarkdownIt
from output_comparison import compare_output
from output_presentation import (
    output_blocks,
    readme_with_outputs,
    update_readme_outputs,
    variant_script,
    variant_sources,
)


@pytest.fixture
def example(tmp_path):
    script = tmp_path / "main.py"
    script.write_text('print("before")\n')
    readme = tmp_path / "README.md"
    readme.write_text(
        "# Example\n\n## Run it\n\nKeep this prose.\n\n## Try a change\n\n"
        '```diff main.py variant=after\n-print("before")\n+print("after")\n```\n\n'
        "<!-- sail-output:start variant=after -->\n\n<!-- sail-output:end -->\n\n"
        "## Explanation\n\nKeep this too.\n"
    )
    return script


def capture(script, output, version="0.7.3", variant="default"):
    source = script.read_bytes() if variant == "default" else variant_sources(script)[variant].encode()
    record_output(
        script.with_name("outputs.json"),
        {"output": output, "run": {"sail": version, "script_sha256": hashlib.sha256(source).hexdigest()}},
        variant,
    )


def test_generated_sections_preserve_prose_and_only_show_latest_output(example):
    capture(example, "old\n", "0.7.2")
    capture(example, "before\n")
    capture(example, "after\n", variant="after")
    history = example.with_name("outputs.json").read_bytes()
    readme = example.with_name("README.md")
    original = readme.read_text()
    assert update_readme_outputs(example, check=True)
    assert readme.read_text() == original
    assert update_readme_outputs(example)
    result = readme.read_text()
    assert "Keep this prose." in result and "Keep this too." in result
    assert "```text\nbefore\n```" in result and "```text\nafter\n```" in result
    assert "old\n" not in result
    assert "Current output · Sail 0.7.3" in result
    assert not update_readme_outputs(example, check=True)
    assert not update_readme_outputs(example)
    assert example.with_name("outputs.json").read_bytes() == history


def test_output_cannot_break_out_of_code_fence_or_create_markers(example):
    output = "```\n<script>alert(1)</script>\n<!-- sail-output:end -->\n````\n"
    capture(example, output)
    body = readme_with_outputs(example)
    fences = [token.content for token in MarkdownIt().parse(body) if token.type == "fence"]
    assert output in fences
    assert len(output_blocks(body)) == 2
    example.with_name("README.md").write_text(body)
    assert readme_with_outputs(example) == body


def test_stale_and_failed_captures_never_appear_current(example):
    capture(example, "before\n")
    example.write_text('print("new")\n')
    # Remove the now-invalid suggested diff to check the unchanged-script presentation.
    example.with_name("README.md").write_text("# Example\n\n## Run it\n\nRun the script.\n")
    body = readme_with_outputs(example)
    assert "Recorded output · Sail 0.7.3" in body
    assert "earlier script" in body
    assert "Current output" not in body
    record_output_failure(example.with_name("outputs.json"), "0.7.4")
    assert "Output capture failed on Sail 0.7.4" in readme_with_outputs(example)
    assert "before\n" in readme_with_outputs(example)


def test_revisions_show_only_current_readme_output_and_preserve_website_history(example):
    from output_presentation import output_presentation

    capture(example, "before\n")
    revised = {
        "output": "after\n",
        "run": {"sail": "0.7.3", "script_sha256": hashlib.sha256(example.read_bytes()).hexdigest()},
    }
    record_output(example.with_name("outputs.json"), revised, revision_reason="Reviewed corrected output")
    body = readme_with_outputs(example)
    assert "Current output · Sail 0.7.3 (revision 2)" in body
    assert "```text\nbefore\n```" not in body
    _, sections = output_presentation(example)
    assert sections[1]["title"] == "Earlier output · Sail 0.7.3 (revision 1)"
    assert sections[1]["output"] == "before\n"


def test_missing_capture_does_not_require_contributor_output(example):
    body = readme_with_outputs(example)
    assert "No output has been captured for this change yet" in body
    assert "## Example output" not in body
    assert not example.with_name("outputs.json").exists()


@pytest.mark.parametrize(
    "body",
    [
        "<!-- sail-output:start variant=default -->\n",
        "<!-- sail-output:end -->\n",
        "<!-- sail-output:start variant=../private -->\n",
        "<!-- sail-output:start variant=default -->\n\n<!-- sail-output:start variant=other -->\n",
        (
            "<!-- sail-output:start variant=default -->\n\n<!-- sail-output:end -->\n\n"
            "<!-- sail-output:start variant=default -->\n\n<!-- sail-output:end -->\n"
        ),
    ],
)
def test_malformed_markers_fail_without_rewriting_prose(example, body):
    readme = example.with_name("README.md")
    readme.write_text(body)
    with pytest.raises(ValueError, match="marker"):
        update_readme_outputs(example)
    assert readme.read_text() == body


def test_unknown_variant_is_rejected(example):
    readme = example.with_name("README.md")
    readme.write_text("<!-- sail-output:start variant=unknown -->\n\n<!-- sail-output:end -->\n")
    with pytest.raises(ValueError, match="no runnable diff"):
        readme_with_outputs(example)


def test_empty_output_markers_can_be_adjacent(example):
    capture(example, "before\n")
    example.with_name("README.md").write_text(
        "# Example\n\n<!-- sail-output:start variant=default -->\n<!-- sail-output:end -->\n"
    )
    assert "```text\nbefore\n```" in readme_with_outputs(example)


def test_variant_executes_displayed_patch_and_leaves_original_untouched(example):
    original = example.read_bytes()
    with variant_script(example, "after") as patched:
        result = subprocess.run([sys.executable, str(patched)], capture_output=True, text=True, check=True)
        assert result.stdout == "after\n"
        assert example.read_bytes() == original
    assert not patched.exists()
    assert example.read_bytes() == original


def test_variant_failure_cleans_temporary_copy(example):
    with pytest.raises(RuntimeError), variant_script(example, "after") as patched:
        raise RuntimeError("execution failed")
    assert not patched.exists()
    assert example.read_text() == 'print("before")\n'


def test_variant_refuses_symlinked_files(example, tmp_path):
    (tmp_path / "linked.py").symlink_to(example)
    with pytest.raises(ValueError, match="symbolic links"), variant_script(example, "after"):
        pytest.fail("must refuse the copied link before execution")


@pytest.mark.parametrize("filename", ["../private.py", "/private.py"])
def test_variant_refuses_files_outside_example(example, filename):
    readme = example.with_name("README.md")
    readme.write_text(readme.read_text().replace("diff main.py", f"diff {filename}"))
    with pytest.raises(ValueError, match="inside its directory"):
        variant_sources(example)


def test_variant_history_and_comparison_are_separate_from_default(example, tmp_path):
    capture(example, "before\n")
    default = json.loads(example.with_name("outputs.json").read_text())["snapshots"]
    capture(example, "after\n", variant="after")
    record_output_failure(example.with_name("outputs.json"), "0.7.4", "after")
    history = json.loads(example.with_name("outputs.json").read_text())
    assert history["snapshots"] == default
    assert "failed_versions" not in history
    assert history["variants"]["after"]["failed_versions"] == ["0.7.4"]
    result = compare_output(example, "after\n", "0.7.3", tmp_path / "report", "after")
    assert result["status"] == "matched" and not result["source_changed"]
    assert compare_output(example, "before\n", "0.7.3", tmp_path / "report", "after")["status"] == "changed"
    with pytest.raises(ValueError, match="already has a different capture"):
        capture(example, "changed\n", variant="after")


def test_repository_readme_outputs_match_accepted_captures():
    stale = [example.id for example in discover_examples() if update_readme_outputs(example.path, check=True)]
    assert not stale, f"Run uv run python tests/harness.py --render-readmes: {stale}"


def test_readme_check_command_does_not_execute_examples():
    result = subprocess.run(
        [sys.executable, str(ROOT / "tests/harness.py"), "--render-readmes", "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "0 changed" in result.stdout
