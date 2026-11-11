"""README links and images must name existing files inside the product tree.

External URLs and fragment-only links are skipped. Fragments on file links are
ignored: this checks file destinations, not heading anchors or remote servers.
"""

import os
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
from catalog import ROOT
from markdown_it import MarkdownIt

MARKDOWN = MarkdownIt("commonmark").enable("table")


class HTMLLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets = []

    def handle_starttag(self, tag, attrs):
        attribute = {"a": "href", "img": "src"}.get(tag)
        self.targets.extend(value for key, value in attrs if key == attribute and value is not None)

    handle_startendtag = handle_starttag


def link_targets(text):
    text = re.sub(r"\A---\n.*?\n---\n", "", text, count=1, flags=re.DOTALL)
    pending = list(MARKDOWN.parse(text))
    while pending:
        token = pending.pop(0)
        if token.type == "link_open":
            yield token.attrGet("href")
        elif token.type == "image":
            yield token.attrGet("src")
        elif token.type in {"html_inline", "html_block"}:
            parser = HTMLLinks()
            parser.feed(token.content)
            yield from parser.targets
        pending.extend(token.children or [])


def missing_targets(readme, root):
    missing = []
    for target in link_targets(readme.read_text()):
        url = urlsplit(target)
        if url.scheme or url.netloc or not url.path:
            continue
        path = (readme.parent / unquote(url.path)).resolve()
        if not path.is_relative_to(root.resolve()) or not path.exists():
            missing.append(target)
    return missing


def readmes():
    found = []
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = [
            name
            for name in dirs
            if not name.startswith(".")
            and name not in {"__pycache__", "spark-warehouse", "node_modules"}
            and not (directory == str(ROOT) and name == "internal")
        ]
        if "README.md" in files:
            found.append(Path(directory) / "README.md")
    return sorted(found)


READMES = readmes()


@pytest.mark.parametrize("readme", READMES, ids=[str(p.relative_to(ROOT)) for p in READMES])
def test_readme_relative_file_links_resolve(readme):
    assert not (missing := missing_targets(readme, ROOT)), f"missing or outside the repository: {missing}"


@pytest.mark.parametrize(
    "text",
    [
        "[guide](missing.md)",
        "| Guide |\n| --- |\n| [guide](missing.md) |",
        "[guide][ref]\n\n[ref]: missing.md",
        "[missing.md]\n\n[missing.md]: missing.md",
        "![diagram](missing.png)",
        '<a href="missing.md">guide</a>',
        '<img src="missing.png" />',
    ],
)
def test_missing_link_syntax_is_checked(tmp_path, text):
    readme = tmp_path / "README.md"
    readme.write_text(text)
    assert len(missing_targets(readme, tmp_path)) == 1


def test_escaped_paths_and_reference_links_resolve_from_the_readme(tmp_path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "guide (intro).md").touch()
    readme = tmp_path / "docs" / "README.md"
    readme.write_text(
        '[guide](<../guide (intro).md#intro> "title")\n'
        "[again][ref]\n\n[ref]: ../guide%20(intro).md?raw=1#intro\n"
    )
    assert missing_targets(readme, tmp_path) == []


def test_existing_file_outside_the_repository_is_rejected(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (tmp_path / "private.md").touch()
    readme = root / "README.md"
    readme.write_text("[private](../private.md)")
    assert missing_targets(readme, root) == ["../private.md"]


def test_urls_fragments_and_code_are_not_file_links(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text(
        "[web](https://example.com/missing) [mail](mailto:reader@example.com) "
        "[cdn](//example.com/missing.png) [section](#intro)\n"
        "`[code](missing.md)`\n\n```markdown\n[code](missing.md)\n```\n"
        "<!-- [comment](missing.md) -->\n"
    )
    assert missing_targets(readme, tmp_path) == []
