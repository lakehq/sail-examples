"""The generated showcase works under a project subpath and links only to real local output."""

import json
import os
import re
import shlex
import subprocess
import sys
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
from catalog import ROOT, TIERS, discover_examples, discover_facts
from markdown_it import MarkdownIt


def local_target(site, page, url):
    path = unquote(urlsplit(url).path)
    if path.startswith("/"):
        base = os.environ.get("SAIL_EXAMPLES_BASE", "/")
        if not path.startswith(base):
            raise ValueError(f"Link escapes the configured site base {base}: {url}")
        target = site / path[len(base) :]
    else:
        target = page.parent / path if path else page
    if target.is_dir():
        target = target / "index.html"
    return target.resolve()


class PageLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.ids = set()
        self.examples = 0
        self.facts = 0
        self.fact_links = {}
        self.current_fact = None
        self.related_examples = []

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        if "data-example" in attrs:
            self.examples += 1
        if "data-fact" in attrs:
            self.facts += 1
            self.current_fact = attrs["id"]
            self.fact_links[self.current_fact] = []
        if tag == "a" and attrs.get("class") == "guide-related-card":
            self.related_examples.append(attrs["href"])
        for attribute in ("href", "src"):
            if attribute in attrs:
                self.links.append(attrs[attribute])
                if self.current_fact:
                    self.fact_links[self.current_fact].append(attrs[attribute])

    def handle_endtag(self, tag):
        if tag == "article":
            self.current_fact = None


class SourceCode(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files = {}
        self.current = None

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if tag == "code" and attrs.get("class") == "python-source":
            self.current = attrs["id"]
            self.files[self.current] = ""

    def handle_data(self, data):
        if self.current:
            self.files[self.current] += data

    def handle_endtag(self, tag):
        if tag == "code":
            self.current = None


@pytest.fixture(scope="module")
def site():
    subprocess.run([sys.executable, str(ROOT / "tools/build_site.py")], cwd=ROOT, check=True)
    return ROOT / "_site"


def test_every_example_has_a_guide_and_a_catalog_entry(site):
    examples = discover_examples()
    homepage = PageLinks()
    homepage.feed((site / "index.html").read_text())
    assert homepage.examples == len(examples)
    for example in examples:
        page = site / "examples" / example.id / "index.html"
        assert page.is_file(), example.id
        assert "View Python source" in page.read_text()
        assert f"examples/{example.id}/index.html" in homepage.links


def test_every_example_script_is_discovered_with_reader_metadata():
    examples = discover_examples()
    scripts = {path for tier in TIERS for path in (ROOT / tier).glob("*/main.py")}
    assert {example.path for example in examples} == scripts, "Every main.py needs a README.md"
    assert len({example.id for example in examples}) == len(examples), "Example IDs must be unique"
    for example in examples:
        for key in ("title", "summary"):
            value = example.meta.get(key)
            assert isinstance(value, str) and value.strip(), f"{example.id}: add a reader-facing {key}"
        tags = example.meta.get("tags")
        assert isinstance(tags, list) and tags, f"{example.id}: add tags for discovery and related examples"
        assert all(isinstance(tag, str) and tag.strip() for tag in tags), example.id


def guide_command_problems(body, root):
    problems = []
    tokens = MarkdownIt().parse(body)
    commands = []
    for token in tokens:
        if token.type == "fence" and token.info in {"bash", "sh", "shell"}:
            commands.append(token.content)
        for child in token.children or []:
            if child.type == "code_inline" and child.content.startswith(
                ("uv run ", "pytest ", "python ", "python3 ", "cd ")
            ):
                commands.append(child.content)
    for command in commands:
        # Tokenize for inspection only; documentation must never execute during a site build.
        words = []
        for line in command.replace("\\\n", "").splitlines():
            lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
            lexer.whitespace_split = True
            words.extend([";", *lexer])
        if any(
            word == "cd" and words[index - 1] in {";", "&&", "||", "|", "&"}
            for index, word in enumerate(words)
        ):
            problems.append("Keep commands at the repository root; remove cd from the guide")
        for word in words:
            path = word.split("::", 1)[0]
            if not path.endswith(".py"):
                continue
            target = (root / path).resolve()
            if Path(path).is_absolute() or not target.is_relative_to(root.resolve()) or not target.is_file():
                problems.append(f"Use an existing repository-root Python path: {path}")
    return problems


def test_guide_commands_use_explicit_repository_paths():
    for example in discover_examples():
        readme = example.path.with_name("README.md")
        body = re.sub(r"\A---\n.*?\n---\n", "", readme.read_text(), count=1, flags=re.DOTALL)
        first_command = re.search(r"^```(?:bash|sh|shell)\n", body, re.MULTILINE)
        assert first_command, f"{readme}: include a copyable run command"
        assert "repository root" in body[: first_command.start()], f"{readme}: state the working directory"
        expected = f"uv run {example.path.relative_to(ROOT).as_posix()}"
        assert expected in body, f"{readme}: include {expected}"
        assert not (problems := guide_command_problems(body, ROOT)), f"{readme}: {problems}"


@pytest.mark.parametrize(
    "command",
    [
        "uv run main.py",
        "uv run missing/main.py",
        "pytest test_orders.py",
        "cd sample",
        "uv run missing/main.py && cd sample",
    ],
)
@pytest.mark.parametrize("fenced", [False, True])
def test_ambiguous_commands_are_rejected(tmp_path, command, fenced):
    body = f"```bash\n{command}\n```" if fenced else f"Run `{command}`."
    assert guide_command_problems(body, tmp_path)


def test_root_commands_accept_arguments_and_line_continuations(tmp_path):
    directory = tmp_path / "sample"
    directory.mkdir()
    (directory / "main.py").write_text("")
    (directory / "test_orders.py").write_text("")
    body = (
        "```bash\nuv run sample/main.py \\\n  --input sample/orders.json\n"
        "uv run pytest sample/test_orders.py::test_orders\n```\n"
        'Rerun `uv run sample/main.py --query "cd"`.'
    )
    assert not guide_command_problems(body, tmp_path)


def test_guides_show_output_histories_when_available(site):
    for example in discover_examples():
        capture = example.path.with_name("outputs.json")
        page = (site / "examples" / example.id / "index.html").read_text()
        if not capture.exists():
            assert 'id="recorded-output"' not in page, example.id
            continue
        history = json.loads(capture.read_text())
        assert history["snapshots"] or history.get("failed_versions"), f"{capture}: record a run first"
        assert 'id="recorded-output"' in page, example.id
        for snapshot in history["snapshots"]:
            assert snapshot["output"].strip() and snapshot["runs"], capture
            assert escape(snapshot["output"]) in page, (
                f"{example.id}: show the captured console text unchanged"
            )


def test_generated_readme_outputs_are_replaced_by_one_website_presentation(site):
    from collections import Counter

    from output_presentation import variant_sources

    for example in discover_examples():
        page = (site / "examples" / example.id / "index.html").read_text()
        assert "sail-output:" not in page
        assert 'id="example-output"' not in page
        path = example.path.with_name("outputs.json")
        if not path.exists():
            continue
        history = json.loads(path.read_text())
        variants = variant_sources(example.path)
        assert page.count('class="variant-output"') == len(variants)
        outputs = []
        for name in ["default", *variants]:
            section = history if name == "default" else history["variants"][name]
            outputs.extend(escape(snapshot["output"]) for snapshot in section["snapshots"])
        for output, count in Counter(outputs).items():
            assert page.count(output) == count, example.id


def test_contributed_example_renders_without_website_assets(tmp_path, monkeypatch):
    from catalog import Example

    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    monkeypatch.setattr(build_site, "ROOT", tmp_path)
    folder = tmp_path / "quickstart" / "new-example"
    folder.mkdir(parents=True)
    script = folder / "main.py"
    script.write_text('print("hello")\n')
    readme = folder / "README.md"
    readme.write_text(
        "# New example\n\n## Usage\n\nFrom the repository root:\n\n"
        "```bash\nuv run quickstart/new-example/main.py\n```\n"
    )
    example = Example(
        script,
        {
            "id": "new-example",
            "title": "New example",
            "summary": "Try a new example.",
            "tier": "quickstart",
            "level": "beginner",
            "tags": ["sql"],
        },
    )
    html = build_site.render_guide(example, {readme: example}, tmp_path / "_site", [])
    page = PageLinks()
    page.feed(html)
    code = SourceCode()
    code.feed(html)
    assert "usage" in page.ids
    assert "python-source" in page.ids
    assert "recorded-output" not in page.ids
    assert code.files == {"source-main.py": script.read_text()}
    assert "uv run quickstart/new-example/main.py" in html


def test_every_guide_links_to_other_examples(site):
    examples = discover_examples()
    destinations = {f"../../examples/{example.id}/index.html" for example in examples}
    for example in examples:
        page = PageLinks()
        page.feed((site / "examples" / example.id / "index.html").read_text())
        links = page.related_examples
        assert len(links) == len(set(links)) == min(2, len(examples) - 1), example.id
        assert set(links) <= destinations, example.id
        assert f"../../examples/{example.id}/index.html" not in links, example.id


def test_every_public_fact_has_a_note_and_a_link_to_its_check(site):
    facts = [fact for fact in discover_facts() if not fact.meta.get("private")]
    reference = PageLinks()
    source = (site / "facts/index.html").read_text()
    reference.feed(source)
    assert reference.facts == len(facts)
    for fact in facts:
        assert fact.id in reference.ids
        assert fact.meta["title"] in source
        check_url = (
            "https://github.com/lakehq/sail-examples/blob/main/" + fact.path.relative_to(ROOT).as_posix()
        )
        assert check_url in reference.links
        assert f"{fact.id}/index.html" in reference.links
        detail = (site / "facts" / fact.id / "index.html").read_text()
        code = SourceCode()
        code.feed(detail)
        assert code.files == {"source-" + fact.path.name: fact.path.read_text()}
        assert check_url in detail
        for engine in fact.engines:
            assert f"test_fact[{fact.id}[{engine}]]" in detail
    for path in site.rglob("*.html"):
        if path.name == "404.html":
            continue  # VitePress renders its error page only on the client.
        page = PageLinks()
        page.feed(path.read_text())
        destinations = [local_target(site, path, link) for link in page.links if not urlsplit(link).scheme]
        assert site / "facts/index.html" in destinations, f"{path}: Behavior missing from navigation"


def test_local_assets_links_and_heading_anchors_resolve(site):
    pages = {}
    for path in site.rglob("*.html"):
        page = PageLinks()
        page.feed(path.read_text())
        pages[path] = page
    problems = []
    for path, page in pages.items():
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            try:
                target = local_target(site, path, link)
            except ValueError as error:
                problems.append(f"{path}: {error}")
                continue
            if not target.is_relative_to(site) or not target.is_file():
                problems.append(f"{path}: missing local target: {link}")
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:
                problems.append(f"{path}: missing heading: {link}")
    assert not problems, "\n".join(problems[:20])


def test_icons_include_their_shapes_without_external_svg_requests(site):
    class Icons(HTMLParser):
        def __init__(self):
            super().__init__()
            self.icons = []
            self.current = None
            self.external = []

        def handle_starttag(self, tag, attributes):
            attrs = dict(attributes)
            if tag == "svg" and attrs.get("class") in ("icon", "topic-icon"):
                self.current = []
            elif self.current is not None and tag in ("path", "circle", "rect", "ellipse", "line"):
                self.current.append(tag)
            if tag == "use" and not attrs.get("href", "").startswith("#"):
                self.external.append(attrs.get("href"))

        def handle_endtag(self, tag):
            if tag == "svg" and self.current is not None:
                self.icons.append(self.current)
                self.current = None

    for path in site.rglob("*.html"):
        if path.name == "404.html":
            continue  # VitePress renders its error page only on the client.
        icons = Icons()
        icons.feed(path.read_text())
        assert icons.icons, path
        assert all(icons.icons), f"{path}: an icon contains no shapes"
        assert not icons.external, f"{path}: external SVG references break local previews"


def test_related_fact_links_work_in_both_directions(site):
    reference = PageLinks()
    reference.feed((site / "facts/index.html").read_text())
    for example in discover_examples():
        guide = PageLinks()
        guide.feed((site / "examples" / example.id / "index.html").read_text())
        for fact_id in example.meta.get("facts", []):
            assert f"../../facts/{fact_id}/index.html" in guide.links
            assert f"../examples/{example.id}/index.html" in reference.fact_links[fact_id]
            detail = PageLinks()
            detail.feed((site / "facts" / fact_id / "index.html").read_text())
            assert f"../../examples/{example.id}/index.html" in detail.links


def test_embedded_python_matches_every_example_file(site):
    for example in discover_examples():
        page = (site / "examples" / example.id / "index.html").read_text()
        code = SourceCode()
        code.feed(page)
        files = list(example.path.parent.glob("*.py"))
        assert len(code.files) == len(files)
        assert page.count('class="source-line-numbers"') == len(files)
        assert next(iter(code.files)) == "source-main.py"
        for path in files:
            assert code.files["source-" + path.name] == path.read_text(), path
            source_url = (
                "https://github.com/lakehq/sail-examples/blob/main/" + path.relative_to(ROOT).as_posix()
            )
            assert source_url in page


def test_python_viewer_escapes_code_and_refuses_outside_files(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    monkeypatch.setattr(build_site, "ROOT", tmp_path)
    directory = tmp_path / "example"
    directory.mkdir()
    script = directory / "main.py"
    script.write_text('value = "</code><script>alert(1)</script>"\n')
    example = SimpleNamespace(path=script)
    html = build_site.render_python_source(example)
    assert "<script>" not in html
    code = SourceCode()
    code.feed(html)
    assert code.files["source-main.py"] == script.read_text()
    private = tmp_path / "private.py"
    private.write_text('secret = "do not publish"\n')
    (directory / "helper.py").symlink_to(private)
    with pytest.raises(ValueError, match="inside the example directory"):
        build_site.render_python_source(example)


def test_site_contains_no_lfs_pointers_or_private_sources(site):
    assert not list(site.rglob("*.py"))
    assert not list(site.rglob(".git"))
    for path in (site / "media").rglob("*"):
        if path.is_file():
            assert not path.read_bytes().startswith(b"version https://git-lfs.github.com/spec/v1"), path


def test_versioned_output_labels_history_and_escapes_output(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace

    from harness import record_output

    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    monkeypatch.setattr(build_site, "ROOT", tmp_path)
    script = tmp_path / "main.py"
    script.write_text('print("example")\n')
    source = hashlib.sha256(script.read_bytes()).hexdigest()
    history = script.with_name("outputs.json")
    for version, text in [("0.7.2", "before"), ("0.7.3", "<script>alert(1)</script>")]:
        record_output(
            history,
            {
                "output": text,
                "run": {
                    "sail": version,
                    "python": "3.12.14",
                    "script_sha256": source,
                    "captured_at": "2026-11-05",
                },
            },
        )
    example = SimpleNamespace(path=script)
    html = build_site.render_output_history(example)
    assert "Current output · Sail 0.7.3" in html
    assert "Output through Sail 0.7.2" in html
    assert html.index("0.7.3") < html.index("0.7.2")
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert html.count('class="output-snapshot" open') == 1
    script.write_text('print("new example")\n')
    html = build_site.render_output_history(example)
    assert "Current output" not in html
    assert "Recorded output · Sail 0.7.3" in html
    assert "example has changed" in html


def test_failed_output_capture_is_visible_without_fabricating_output(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace

    from harness import record_output, record_output_failure

    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    monkeypatch.setattr(build_site, "ROOT", tmp_path)
    script = tmp_path / "main.py"
    script.write_text('print("example")\n')
    example = SimpleNamespace(path=script)
    history = script.with_name("outputs.json")
    record_output_failure(history, "0.7.3")
    html = build_site.render_output_history(example)
    assert "Output capture failed on Sail 0.7.3" in html
    assert "<pre" not in html
    record_output(
        history,
        {
            "output": "old result",
            "run": {
                "sail": "0.7.2",
                "python": "3.12.14",
                "captured_at": "2026-11-05",
                "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
            },
        },
    )
    html = build_site.render_output_history(example)
    assert "Recorded output · Sail 0.7.2" in html
    assert "Current output" not in html
    assert "example has changed" not in html
    assert "old result" in html


def test_change_diff_uses_source_positions_and_tracks_insertions_and_removals(tmp_path, monkeypatch):
    import re

    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    monkeypatch.setattr(build_site, "ROOT", tmp_path)
    script = tmp_path / "main.py"
    script.write_text("header\nstart\nold\nend\n")
    diff = " start\n-old\n+replacement\n+extra\n end\n"
    html = build_site.highlight_diff(diff, "diff", "main.py", script)
    numbers = re.findall(r'data-line="(\d*)"', html)
    assert numbers == ["2", "3", "", "", "4"]
    assert html.count('class="diff-number"') == 5
    assert "#L2-L4" in html
    script.write_text("another header\nheader\nstart\nold\nend\n")
    html = build_site.highlight_diff(diff, "diff", "main.py", script)
    assert "#L3-L5" in html
    assert 'data-line="3"' in html


@pytest.mark.parametrize("source", ["different\n", "start\nold\nend\nstart\nold\nend\n"])
def test_change_diff_rejects_stale_or_ambiguous_context(tmp_path, monkeypatch, source):
    monkeypatch.syspath_prepend(str(ROOT / "tools"))
    import build_site

    script = tmp_path / "main.py"
    script.write_text(source)
    with pytest.raises(ValueError, match="diff context must match one location"):
        build_site.highlight_diff(" start\n-old\n+replacement\n end\n", "diff", "main.py", script)


class PageMetadata(HTMLParser):
    def __init__(self):
        super().__init__()
        self.meta = {}
        self.links = {}
        self.language = None

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if tag == "html":
            self.language = attrs.get("lang")
        if tag == "meta":
            key = attrs.get("property", attrs.get("name"))
            if key != "theme-color":
                assert key not in self.meta, f"Duplicate metadata: {key}"
                self.meta[key] = attrs.get("content")
        if tag == "link":
            self.links.setdefault(attrs.get("rel"), []).append(attrs.get("href"))


def test_page_metadata_is_static_and_matches_generated_discovery_files(site):
    import hashlib
    import xml.etree.ElementTree as ET

    base = os.environ.get("SAIL_EXAMPLES_BASE", "/")
    origin = os.environ.get("SAIL_EXAMPLES_ORIGIN", "https://examples.lakesail.com").rstrip("/")
    address = origin + base
    manifest = json.loads((ROOT / "site/.generated/pages.json").read_text())
    llms = (site / "llms.txt").read_text()
    canonical_urls = set()
    images = set()
    for entry in manifest:
        metadata = PageMetadata()
        metadata.feed((site / entry["url"]).read_text())
        assert metadata.language == "en-US"
        assert metadata.links["icon"] == [base + "sail.svg"]
        if entry["url"] == "404.html":
            assert metadata.meta["robots"] == "noindex"
            assert "canonical" not in metadata.links
            assert "og:image" not in metadata.meta
            continue
        canonical = address + entry["url"].removesuffix("index.html")
        canonical_urls.add(canonical)
        assert metadata.links["canonical"] == [canonical]
        assert metadata.meta["og:url"] == canonical
        assert metadata.meta["og:title"] == entry["title"] + " | Sail examples"
        assert metadata.meta["twitter:title"] == metadata.meta["og:title"]
        assert metadata.meta["og:description"] == entry["description"]
        assert metadata.meta["twitter:description"] == entry["description"]
        assert metadata.meta["twitter:card"] == "summary_large_image"
        assert metadata.meta["og:image:type"] == "image/jpeg"
        assert (metadata.meta["og:image:width"], metadata.meta["og:image:height"]) == ("1200", "630")
        image_url = metadata.meta["og:image"]
        assert image_url == metadata.meta["twitter:image"]
        assert metadata.meta["og:image:alt"] == metadata.meta["twitter:image:alt"]
        assert image_url.startswith(address + "social/")
        image_path = site / image_url.removeprefix(address)
        content = image_path.read_bytes()
        assert content.startswith(b"\xff\xd8\xff")
        assert content.endswith(b"\xff\xd9")
        digest = hashlib.sha256(content).hexdigest()[:16]
        assert image_path.name.endswith(f".{digest}.jpg")
        assert image_url not in images
        images.add(image_url)
        assert f"]({canonical})" in llms
    sitemap = ET.parse(site / "sitemap.xml")
    locations = sitemap.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
    assert {location.text for location in locations} == canonical_urls
    assert (site / "robots.txt").read_text() == f"User-agent: *\nAllow: /\n\nSitemap: {address}sitemap.xml\n"
    notices = (site / "third-party-notices.txt").read_text()
    assert "SIL OPEN FONT LICENSE Version 1.1" in notices
    assert "Copyright (c) 2026 GitHub Inc." in notices
    assert "MIT License" in notices


def test_social_preview_renderer_and_configuration(site):
    subprocess.run(["node", "--test", "tests/test_site_metadata.mjs"], cwd=ROOT, check=True)
