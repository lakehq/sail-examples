"""Present accepted captures in READMEs and guides without maintaining output twice."""

import hashlib
import json
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

from harness import capture_key, release_key
from markdown_it import MarkdownIt

VARIANT_NAME = r"[a-z][a-z0-9-]*"


def change_source(script, code, filename):
    source = (script.parent / (filename or script.name)).resolve()
    if not source.is_relative_to(script.parent.resolve()) or source.suffix != ".py":
        raise ValueError(f"{script}: a change example must name a Python file inside its directory")
    diff = code.splitlines(keepends=True)
    if not diff or any(line[0] not in " +-" for line in diff):
        raise ValueError(f"{source}: use context, added and removed lines in the change example")
    before = [line[1:].rstrip("\r\n") for line in diff if not line.startswith("+")]
    original = source.read_text().splitlines(keepends=True)
    lines = [line.rstrip("\r\n") for line in original]
    matches = [
        index for index in range(len(lines) - len(before) + 1) if lines[index : index + len(before)] == before
    ]
    if not before or len(matches) != 1:
        raise ValueError(
            f"{source}: diff context must match one location; update it or include more surrounding lines"
        )
    start = matches[0]
    after = [line[1:] for line in diff if not line.startswith("-")]
    patched = "".join(original[:start] + after + original[start + len(before) :])
    return source, start + 1, patched


def variant_sources(script):
    readme = script.with_name("README.md")
    variants = {}
    if not readme.exists():
        return variants
    for token in MarkdownIt().parse(readme.read_text()):
        if token.type != "fence" or not token.info.startswith("diff "):
            continue
        fields = token.info.split()
        if len(fields) == 2:
            continue
        if len(fields) != 3 or not re.fullmatch(rf"variant={VARIANT_NAME}", fields[2]):
            raise ValueError(f"{readme}: use 'diff main.py variant=name' for a runnable change")
        name = fields[2].removeprefix("variant=")
        if name == "default" or name in variants:
            raise ValueError(f"{readme}: reserved or repeated output variant {name!r}")
        source, _, patched = change_source(script, token.content, fields[1])
        if source != script.resolve():
            raise ValueError(f"{readme}: runnable changes must edit the example's main.py")
        variants[name] = patched
    return variants


@contextmanager
def variant_script(script, variant):
    if variant == "default":
        yield script
        return
    patched = variant_sources(script)[variant]
    with tempfile.TemporaryDirectory(prefix="sail-variant-") as temporary:
        destination = Path(temporary) / "example"
        shutil.copytree(
            script.parent,
            destination,
            symlinks=True,
            ignore=shutil.ignore_patterns(".data", ".venv", ".git", "__pycache__", ".pytest_cache"),
        )
        if any(path.is_symlink() for path in destination.rglob("*")):
            raise ValueError(f"{script}: variant copies must not contain symbolic links")
        target = destination / script.name
        target.write_text(patched)
        yield target


def output_presentation(script, variant="default"):
    path = script.with_name("outputs.json")
    if not path.exists():
        return [], []
    history = json.loads(path.read_text())
    if variant != "default":
        history = history.get("variants", {}).get(variant, {"snapshots": []})
        source = variant_sources(script)[variant].encode()
    else:
        source = script.read_bytes()
    snapshots = history["snapshots"]
    failed = sorted(history.get("failed_versions", []), key=release_key)
    versions = [capture_key(run) for snapshot in snapshots for run in snapshot["runs"]]
    if versions != sorted(set(versions)):
        raise ValueError(f"{path}: Sail release/revision pairs must be unique and ordered")
    source_hash = hashlib.sha256(source).hexdigest()
    latest_failure = max((release_key(version) for version in failed), default=())
    sections = []
    for index, snapshot in enumerate(reversed(snapshots)):
        latest = snapshot["runs"][-1]
        stale = latest["script_sha256"] != source_hash
        current = index == 0 and not stale and release_key(latest["sail"]) > latest_failure
        label = "Current output" if current else "Recorded output" if index == 0 else "Output through"
        title = f"{label} · Sail {latest['sail']}" if index == 0 else f"{label} Sail {latest['sail']}"
        if index and latest["sail"] == snapshots[-1]["runs"][-1]["sail"]:
            title = f"Earlier output · Sail {latest['sail']}"
        if latest.get("revision", 1) > 1 or title.startswith("Earlier output"):
            title += f" (revision {latest.get('revision', 1)})"
        sections.append(
            {
                "title": title,
                "output": snapshot["output"],
                "latest": latest,
                "verified": ", ".join(dict.fromkeys(run["sail"] for run in snapshot["runs"])),
                "stale": index == 0 and stale,
            }
        )
    return failed, sections


def output_blocks(body):
    """Find markers outside code fences, including fences inside generated output."""
    blocks = []
    opened = None
    seen = set()
    markers = [
        (token.map[0] + index, line.strip())
        for token in MarkdownIt("commonmark").parse(body)
        if token.type == "html_block"
        for index, line in enumerate(token.content.splitlines())
        if line.startswith("<!-- sail-output:")
    ]
    for line_number, marker in markers:
        match = re.fullmatch(rf"<!-- sail-output:start variant=({VARIANT_NAME}) -->", marker)
        if match:
            name = match[1]
            if opened is not None or name in seen:
                raise ValueError("Nested or repeated sail-output markers; use one block per variant")
            seen.add(name)
            opened = (line_number, name)
        elif marker == "<!-- sail-output:end -->" and opened is not None:
            start, name = opened
            blocks.append((start, line_number + 1, name))
            opened = None
        else:
            raise ValueError("Invalid sail-output marker; use a start marker and a matching end marker")
    if opened is not None:
        raise ValueError("Missing sail-output end marker; close the generated block")
    return blocks


def render_readme_output(script, variant):
    failed, sections = output_presentation(script, variant)
    text = ""
    if failed:
        text += (
            f"Output capture failed on Sail {', '.join(failed)}. No output from the failed run is shown.\n\n"
        )
    if sections:
        section = sections[0]
        text += section["title"] + ".\n\n"
        if section["stale"]:
            text += (
                "The example has changed since this capture. This output comes from the earlier script.\n\n"
            )
        output = section["output"]
        longest = max((len(match[0]) for match in re.finditer(r"`+", output)), default=0)
        fence = "`" * max(3, longest + 1)
        text += f"{fence}text\n{output}"
        if not output.endswith("\n"):
            text += "\n"
        text += f"{fence}\n"
    elif not failed:
        text = "No output has been captured for this change yet.\n"
    return text


def readme_with_outputs(script):
    readme = script.with_name("README.md")
    body = readme.read_text()
    blocks = output_blocks(body)
    variants = variant_sources(script)
    for _, _, variant in blocks:
        if variant != "default" and variant not in variants:
            raise ValueError(f"{readme}: no runnable diff for output variant {variant!r}")
    failed, sections = output_presentation(script)
    if (failed or sections) and not any(name == "default" for _, _, name in blocks):
        run = re.search(r"(?m)^## Run it\s*$", body)
        following = re.search(r"(?m)^## ", body[run.end() :]) if run else None
        position = run.end() + following.start() if following else len(body)
        placeholder = (
            "<!-- sail-output:start variant=default -->\n\n## Example output\n\n<!-- sail-output:end -->\n\n"
        )
        body = body[:position].rstrip() + "\n\n" + placeholder + body[position:]
        blocks = output_blocks(body)
    lines = body.splitlines(keepends=True)
    for start, end, variant in reversed(blocks):
        heading = "## Example output\n\n" if variant == "default" else ""
        content = render_readme_output(script, variant)
        replacement = (
            f"<!-- sail-output:start variant={variant} -->\n\n{heading}{content}\n<!-- sail-output:end -->\n"
        )
        lines[start:end] = [replacement]
    return "".join(lines)


def update_readme_outputs(script, *, check=False):
    readme = script.with_name("README.md")
    expected = readme_with_outputs(script)
    changed = expected != readme.read_text()
    if changed and not check:
        readme.write_text(expected)
    return changed
