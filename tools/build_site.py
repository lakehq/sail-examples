"""Generate VitePress pages from the example sources and build the showcase."""

import argparse
import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from functools import cache
from html import escape
from pathlib import Path
from string import Template
from urllib.parse import quote, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))

from build_context import CATEGORY_TITLES, GALLERY_GROUPS, checked_against, one_line, ordered
from catalog import ROOT, discover_examples, discover_facts
from markdown_it import MarkdownIt
from output_presentation import change_source, output_blocks, output_presentation, variant_sources
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer

SOURCE = ROOT / "site"
EXTERNAL_ARROW = (
    '<svg class="icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 18 18 6M6 6h12v12"/></svg>'
)
REPOSITORY = "https://github.com/lakehq/sail-examples/blob/main/"
CATEGORIES = {tier: title for title, tiers in GALLERY_GROUPS.items() for tier in tiers}
TOPIC_ICONS = {
    "quickstart": "compass",
    "analytics": "chart",
    "pipelines": "pipeline",
    "ai": "sparkles",
    "multimodal": "sparkles",
    "python": "code",
    "migrate": "code",
    "agents": "sparkles",
    "run-and-connect": "compass",
    "configure": "sliders",
    "python-udfs": "code",
    "data-sources": "database",
    "missing-features": "limits",
    "silent-behavior": "compare",
    "ai-functions": "sparkles",
}


@cache
def icon_shapes():
    root = ET.parse(SOURCE / "icons.svg").getroot()
    for element in root.iter():
        element.tag = element.tag.rsplit("}", 1)[-1]
    return {
        symbol.attrib["id"]: "".join(ET.tostring(child, encoding="unicode") for child in symbol)
        for symbol in root
    }


def render_icon(name, css_class="icon"):
    return (
        f'<svg class="{css_class}" viewBox="0 0 24 24" aria-hidden="true" '
        'fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
        + icon_shapes()[name]
        + "</svg>"
    )


def topic_icon(topic):
    return render_icon(TOPIC_ICONS.get(topic, "grid"), "topic-icon")


def render_sail_logo():
    logo = ET.parse(SOURCE / "sail.svg").getroot()
    logo.attrib.update({"class": "sail-logo", "width": "38", "height": "38", "aria-hidden": "true"})
    logo.attrib.pop("role", None)
    logo.attrib.pop("aria-label", None)
    for element in logo.iter():
        element.tag = element.tag.rsplit("}", 1)[-1]
        if element.get("fill") and element.get("fill") != "none":
            element.set("fill", "currentColor")
    return ET.tostring(logo, encoding="unicode")


def highlight_diff(code, language, filename, script):
    if language != "diff":
        return ""
    fields = filename.split()
    source, start, _ = change_source(script, code, fields[0] if fields else "")
    diff = code.splitlines(keepends=True)
    source_line = start
    lines = []
    for line in diff:
        kind = "added" if line.startswith("+") else "removed" if line.startswith("-") else "context"
        number = "" if kind == "added" else str(source_line)
        lines.append(
            f'<span class="diff-line diff-{kind}" data-line="{number}">'
            f'<span class="diff-number" aria-hidden="true">{number}</span>'
            f'<span class="diff-code">{escape(line)}</span></span>'
        )
        source_line += kind != "added"
    url = REPOSITORY + quote(source.relative_to(ROOT).as_posix()) + f"#L{start}-L{source_line - 1}"
    return (
        '<pre class="code-diff"><span class="diff-columns">'
        "<span>Line</span>"
        f'<a href="{url}">{escape(source.name)} · original lines {start}–{source_line - 1}</a></span>'
        '<code class="language-diff">' + "".join(lines) + "</code></pre>"
    )


def example_fact_links(examples, facts):
    by_id = {fact.id: fact for fact in facts if not fact.meta.get("private")}
    related = {}
    backlinks = {identifier: [] for identifier in by_id}
    for example in examples:
        identifiers = example.meta.get("facts", [])
        if not isinstance(identifiers, list) or not all(isinstance(value, str) for value in identifiers):
            raise ValueError(f"{example.id}: facts must be a list of public fact IDs")
        unknown = set(identifiers) - by_id.keys()
        if unknown or len(set(identifiers)) != len(identifiers):
            raise ValueError(f"{example.id}: unknown or repeated fact IDs: {identifiers}")
        related[example.id] = [by_id[identifier] for identifier in identifiers]
        for identifier in identifiers:
            backlinks[identifier].append(example)
    return related, backlinks


def summary(example):
    return example.meta.get("summary", example.meta["description"])


def guide_url(example):
    return f"examples/{quote(example.id)}/index.html"


def fact_url(fact):
    return f"facts/{quote(fact.id)}/index.html"


def media_url(path, output, base=""):
    path = path.resolve()
    relative = path.relative_to(ROOT)
    destination = output / "media" / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, destination)
    return base + "media/" + quote(relative.as_posix())


def rewrite_link(url, readme, examples_by_readme, output, base, *, image=False):
    parsed = urlsplit(url)
    if parsed.scheme or parsed.netloc or not parsed.path:
        return url
    target = (readme.parent / unquote(parsed.path)).resolve()
    relative = target.relative_to(ROOT)
    if not target.exists():
        raise ValueError(f"{readme}: missing link {url}")
    suffix = ("?" + parsed.query if parsed.query else "") + ("#" + parsed.fragment if parsed.fragment else "")
    if image:
        return media_url(target, output, base) + suffix
    if target in examples_by_readme:
        return base + guide_url(examples_by_readme[target]) + suffix
    return REPOSITORY + quote(relative.as_posix()) + suffix


def render_shell_commands(code):
    # VitePress's copy handler expects the button, language label, and pre in this order.
    return (
        '<div class="language-bash command-block">'
        '<button type="button" class="copy" title="Copy commands" aria-label="Copy commands"></button>'
        '<span class="lang" aria-hidden="true"></span>'
        f'<pre tabindex="0"><code>{escape(code)}</code></pre></div>'
    )


def render_guide(example, examples_by_readme, output, facts):
    readme = example.path.with_name("README.md")
    body = re.sub(r"\A---\n.*?\n---\n", "", readme.read_text(), count=1, flags=re.DOTALL)
    variants = variant_sources(example.path)
    lines = body.splitlines(keepends=True)
    for start, end, variant in reversed(output_blocks(body)):
        if variant != "default" and variant not in variants:
            raise ValueError(f"{readme}: no runnable diff for output variant {variant!r}")
        replacement = "" if variant == "default" else f"```sail-output {variant}\n```\n"
        lines[start:end] = [replacement]
    body = "".join(lines)
    markdown = MarkdownIt(
        "commonmark",
        {
            "html": False,
            "highlight": lambda code, language, filename: highlight_diff(
                code, language, filename, example.path
            ),
        },
    ).enable("table")
    tokens = markdown.parse(body)
    headings = []
    used_ids = set()
    for index, token in enumerate(tokens):
        if token.type == "fence" and token.info.startswith("sail-output "):
            variant = token.info.removeprefix("sail-output ")
            if variant not in variants:
                raise ValueError(f"{readme}: unknown output variant {variant!r}")
            token.type = "html_block"
            token.content = (
                render_output_history(example, variant)
                or "<p>No output has been captured for this change yet.</p>"
            )
        if token.type == "fence" and token.info.strip() in {"bash", "sh", "shell", "shellscript", "zsh"}:
            token.type = "html_block"
            token.content = render_shell_commands(token.content)
        if token.type == "heading_open":
            text = tokens[index + 1].content
            slug = re.sub(r"[^\w\s-]", "", text.lower()).replace(" ", "-")
            heading_id = slug
            suffix = 0
            while heading_id in used_ids:
                suffix += 1
                heading_id = f"{slug}-{suffix}"
            used_ids.add(heading_id)
            token.attrSet("id", heading_id)
            if token.tag == "h2":
                headings.append((heading_id, text))
    pending = list(tokens)
    while pending:
        token = pending.pop()
        attribute = {"link_open": "href", "image": "src"}.get(token.type)
        if attribute:
            value = rewrite_link(
                token.attrGet(attribute),
                readme,
                examples_by_readme,
                output,
                "../../",
                image=token.type == "image",
            )
            token.attrSet(attribute, value)
            if token.type == "image":
                token.attrSet("loading", "lazy")
        pending.extend(token.children or [])
    rendered = markdown.renderer.render(tokens, markdown.options, {})
    rendered = rendered.replace(
        "<table>", '<div class="table-scroll" tabindex="0" role="region" aria-label="Example data"><table>'
    )
    rendered = rendered.replace("</table>", "</table></div>")
    captured_output = render_output_history(example)
    position = next(
        (index + 1 for index, (identifier, _) in enumerate(headings) if identifier == "run-it"), len(headings)
    )
    extra_headings = [("recorded-output", "Output")] if captured_output else []
    extra_headings.append(("python-source", "Python code"))
    headings[position:position] = extra_headings
    run_section = re.search(r'<h2 id="run-it">.*?(?=<h2|\Z)', rendered, re.DOTALL)
    insertion = run_section.end() if run_section else len(rendered)
    rendered = rendered[:insertion] + captured_output + render_python_source(example) + rendered[insertion:]
    if facts:
        headings.append(("related-sail-facts", "Related Sail behavior"))
        links = "".join(
            f'<li><a href="../../{fact_url(fact)}">{escape(fact.meta["title"])}</a></li>' for fact in facts
        )
        rendered += f'<section class="related-facts"><h2 id="related-sail-facts">Related Sail behavior</h2><ul>{links}</ul></section>'
    toc = "".join(
        f'<a class="toc-link" href="#{escape(identifier)}">{escape(title)}</a>'
        for identifier, title in headings
    )
    source = REPOSITORY + quote(example.path.relative_to(ROOT).as_posix())
    return (
        '<div class="guide-layout page-width"><aside class="guide-nav" aria-label="In this guide"><div>'
        '<a class="back-link" href="../../index.html#examples">← All examples</a><span class="tag">In this guide</span>'
        + toc
        + '</div></aside><article class="guide-body"><div class="guide-source">'
        + f'<span class="tag">{escape(example.meta["level"])}</span>'
        + f'<a href="{source}">View Python source {EXTERNAL_ARROW}</a></div>'
        + rendered
        + render_related_examples(example, examples_by_readme.values())
        + "</article></div>"
    )


def render_related_examples(example, examples):
    def relevance(candidate):
        shared_tags = set(example.meta["tags"]) & set(candidate.meta["tags"])
        shared_facts = set(example.meta.get("facts", [])) & set(candidate.meta.get("facts", []))
        same_group = CATEGORIES[example.meta["tier"]] == CATEGORIES[candidate.meta["tier"]]
        return (-len(shared_tags), -len(shared_facts), -same_group, candidate.id)

    candidates = sorted((item for item in examples if item.id != example.id), key=relevance)[:2]
    if not candidates:
        return ""
    cards = "".join(
        f'<a class="guide-related-card" href="../../{guide_url(candidate)}">'
        f'<span class="tag">{topic_icon(candidate.meta["tier"])} '
        f"{escape(CATEGORIES[candidate.meta['tier']])}</span>"
        f'<strong>{escape(candidate.meta["title"])} <span aria-hidden="true">→</span></strong>'
        f"<span>{escape(summary(candidate))}</span></a>"
        for candidate in candidates
    )
    return (
        '<nav class="guide-related" aria-labelledby="keep-exploring">'
        '<h2 id="keep-exploring">Keep exploring</h2>'
        f'<div class="guide-related-cards">{cards}</div></nav>'
    )


def render_python_source(example):
    files = sorted(example.path.parent.glob("*.py"), key=lambda path: (path != example.path, path.name))
    return render_source_files(files)


def render_source_files(files):
    viewers = []
    for path in files:
        if not path.resolve().is_relative_to(path.parent.resolve()):
            raise ValueError(f"{path}: source files must stay inside the example directory")
        code = path.read_text()
        line_numbers = "\n".join(str(number) for number in range(1, len(code.splitlines()) + 1))
        highlighted = highlight(code, PythonLexer(stripnl=False, ensurenl=False), HtmlFormatter(nowrap=True))
        identifier = "source-" + path.name
        source = REPOSITORY + quote(path.relative_to(ROOT).as_posix())
        viewers.append(
            '<details class="source-viewer" data-copy-container>'
            f'<summary><span>{escape(path.name)}</span><span class="source-lines">{len(code.splitlines())} lines</span></summary>'
            '<div class="source-toolbar">'
            f'<a href="{source}">View {escape(path.name)} on GitHub {EXTERNAL_ARROW}</a>'
            f'<button type="button" data-copy="{escape(identifier)}" data-copy-label="Code" hidden>Copy code</button></div>'
            f'<pre tabindex="0" aria-label="{escape(path.name)} source">'
            f'<span class="source-line-numbers" aria-hidden="true">{line_numbers}</span>'
            f'<code class="python-source" id="{escape(identifier)}">{highlighted}</code></pre>'
            '<p class="copy-status" role="status"></p></details>'
        )
    return (
        '<section aria-labelledby="python-source"><h2 id="python-source">Python code</h2>'
        + "".join(viewers)
        + "</section>"
    )


def render_output_history(example, variant="default"):
    failed_versions, snapshots = output_presentation(example.path, variant)
    if not snapshots and not failed_versions:
        return ""
    sections = []
    if failed_versions:
        releases = ", ".join(failed_versions)
        sections.append(
            f'<p class="output-note">Output capture failed on Sail {escape(releases)}. '
            "Run this example to inspect the error before recording its output. "
            "No output from the failed run is shown here.</p>"
        )
    for index, snapshot in enumerate(snapshots):
        latest = snapshot["latest"]
        stale = (
            '<p class="output-note">The example has changed since this capture. Rerun it to refresh the output.</p>'
            if snapshot["stale"]
            else ""
        )
        sections.append(
            '<details class="output-snapshot"' + (" open" if index == 0 else "") + ">"
            f"<summary>{escape(snapshot['title'])}</summary>"
            f'<p class="output-note">Captured on Sail {escape(snapshot["verified"])}. '
            f"Latest capture: {escape(latest['captured_at'][:10])}, Python {escape(latest['python'])}.</p>"
            + stale
            + f'<pre tabindex="0" aria-label="Recorded command output"><code>{escape(snapshot["output"])}</code></pre></details>'
        )
    if variant != "default":
        return '<div class="variant-output">' + "".join(sections) + "</div>"
    return (
        '<section class="output-history" aria-labelledby="recorded-output"><h2 id="recorded-output">Output</h2>'
        + (
            "<p>The newest capture appears first. Older output stays here when a result changes.</p>"
            if snapshots
            else ""
        )
        + "".join(sections)
        + "</section>"
    )


def render_card(example):
    meta = example.meta
    category = CATEGORIES[meta["tier"]]
    search = " ".join([meta["title"], summary(example), meta["description"], *meta["tags"], category]).lower()
    return (
        f'<article class="example-card" data-example data-category="{escape(category)}" '
        f'data-level="{escape(meta["level"])}" data-search="{escape(search, quote=True)}">'
        f'<span class="tag">{escape(meta["level"])}</span><span class="arrow" aria-hidden="true">{EXTERNAL_ARROW}</span>'
        f'<h3><a href="{guide_url(example)}">{escape(meta["title"])}</a></h3>'
        f'<p>{escape(summary(example))}</p><span class="category">{topic_icon(meta["tier"])} {escape(category)}</span></article>'
    )


def render_featured(example, output):
    preview = media_url(example.path.parent / example.meta["preview"], output)
    diagram = " diagram" if preview.endswith(".svg") else ""
    return (
        f'<article class="featured-card"><img class="preview{diagram}" src="{preview}" '
        f'alt="Preview: {escape(summary(example))}" loading="lazy">'
        f'<div class="featured-body"><span class="tag">{escape(example.meta["level"])}</span>'
        f'<h3><a href="{guide_url(example)}">{escape(example.meta["title"])}</a></h3>'
        f'<p>{escape(summary(example))}</p><span class="card-link">Open the guide {EXTERNAL_ARROW}</span></div></article>'
    )


def render_facts(facts, examples_by_fact):
    markdown = MarkdownIt("commonmark", {"html": False})
    navigation = []
    sections = []
    for category, title in CATEGORY_TITLES.items():
        notes = ordered(f for f in facts if f.meta["category"] == category and not f.meta.get("private"))
        if not notes:
            continue
        icon = topic_icon(category)
        note_count = f"{len(notes)} {'note' if len(notes) == 1 else 'notes'}"
        navigation.append(
            f'<li><a href="#{category}">{icon}<span>{escape(title)}</span>'
            f'<span class="topic-count">{note_count}</span></a></li>'
        )
        cards = []
        for fact in notes:
            text = markdown.render(one_line(fact))
            source = REPOSITORY + quote(fact.path.relative_to(ROOT).as_posix())
            engines = " and ".join("Apache Spark" if engine == "spark" else "Sail" for engine in fact.engines)
            examples = examples_by_fact[fact.id]
            related = ""
            if examples:
                links = "".join(
                    f'<li><a href="../{guide_url(example)}">{escape(example.meta["title"])}</a></li>'
                    for example in examples
                )
                related = (
                    '<details class="fact-examples">'
                    f"<summary>Related examples ({len(examples)})</summary><ul>{links}</ul></details>"
                )
            cards.append(
                f'<article class="fact-note" id="{escape(fact.id)}" aria-labelledby="{escape(fact.id)}-title" data-fact>'
                f'<h3 id="{escape(fact.id)}-title"><a href="{quote(fact.id)}/index.html">{escape(fact.meta["title"])}</a></h3>'
                f'{text}{related}<footer class="fact-evidence"><span>Check runs on {escape(engines)}</span>'
                '<div class="fact-actions">'
                f'<a href="{quote(fact.id)}/index.html">Read note and Python check →</a>'
                f'<a href="{source}">View source {EXTERNAL_ARROW}</a></div></footer></article>'
            )
        sections.append(
            f'<section class="fact-category" aria-labelledby="{category}">'
            f'<h2 id="{category}">{icon} {escape(title)}</h2>{"".join(cards)}</section>'
        )
    return Template((SOURCE / "facts.html").read_text()).substitute(
        navigation="".join(navigation),
        sections="".join(sections),
        versions=escape(checked_against()),
        count=sum(not fact.meta.get("private") for fact in facts),
        check_command=render_shell_commands(
            "uv run pytest tests/test_facts.py -k cache-does-not-keep-results"
        ),
    )


def render_fact(fact, examples):
    markdown = MarkdownIt("commonmark", {"html": False})
    explanation = markdown.render(one_line(fact))
    category = fact.meta["category"]
    category_link = f"../index.html#{quote(category)}"
    source = REPOSITORY + quote(fact.path.relative_to(ROOT).as_posix())
    commands = []
    for engine in fact.engines:
        command = f"uv run pytest 'tests/test_facts.py::test_fact[{fact.id}[{engine}]]'"
        label = "Sail" if engine == "sail" else "Apache Spark (requires Java 17)"
        commands.append(f"<h3>{label}</h3>" + render_shell_commands(command))
    links = "".join(
        f'<li><a href="../../{guide_url(example)}">{escape(example.meta["title"])}</a></li>'
        for example in examples
    )
    related = (
        '<section class="related-facts"><h2 id="related-examples">Related examples</h2>'
        f"<ul>{links}</ul></section>"
        if examples
        else ""
    )
    related_nav = '<a class="toc-link" href="#related-examples">Related examples</a>' if examples else ""
    return (
        '<div class="guide-layout page-width"><aside class="guide-nav" aria-label="In this note"><div>'
        '<a class="back-link" href="../index.html">← All Sail behavior</a>'
        '<span class="tag">In this note</span>'
        '<a class="toc-link" href="#run-the-check">Run the check</a>'
        '<a class="toc-link" href="#python-source">Python code</a>'
        + related_nav
        + '</div></aside><article class="guide-body"><div class="guide-source">'
        f'<a href="{category_link}">{topic_icon(category)} {escape(CATEGORY_TITLES[category])}</a>'
        f'<a href="{source}">View Python source {EXTERNAL_ARROW}</a></div>'
        f"<h1>{escape(fact.meta['title'])}</h1>" + explanation + '<div class="reference-scope">'
        f"<strong>Check targets: <code>{escape(checked_against())}</code></strong>"
        "<p>The Python assertions below check this behavior. Details attributed to external documentation "
        "are outside those assertions. Newer releases may behave differently.</p></div>"
        '<h2 id="run-the-check">Run the check</h2>'
        f'<p>Follow the <a href="{REPOSITORY}CONTRIBUTING.md">test setup</a>, '
        "then run from the repository root (<code>sail-examples/</code>). The harness applies the configuration declared in this check.</p>"
        + "".join(commands)
        + render_source_files([fact.path])
        + related
        + "</article></div>"
    )


def write_page(root, url, title, description, content, *, showcase=False, section="Sail examples"):
    path = root / url.replace(".html", ".md")
    path.parent.mkdir(parents=True, exist_ok=True)
    if not showcase:
        article = re.search(r'<article class="guide-body">(.*?)</article>', content, re.DOTALL)
        if article:
            content = article.group(1)

        def heading_anchor(match):
            level, attributes, label = match.groups()
            identifier = re.search(r'id="([^"]+)"', attributes)
            identifier = (
                identifier.group(1)
                if identifier
                else re.sub(r"[^\w\s-]", "", re.sub(r"<[^>]*>", "", label).lower()).replace(" ", "-")
            )
            if 'id="' not in attributes:
                attributes += f' id="{escape(identifier)}"'
            return f'<h{level}{attributes}>{label}<a class="header-anchor" href="#{escape(identifier)}" aria-label="Permalink"></a></h{level}>'

        content = re.sub(r"<h([1-6])([^>]*)>(.*?)</h\1>", heading_anchor, content, flags=re.DOTALL)
        content = '<div class="guide-body">' + content + "</div>"
    metadata = {"title": title, "description": description, "showcase": showcase, "section": section}
    if url == "facts/index.html":
        metadata["outline"] = 2
    if showcase:
        metadata["layout"] = False
    # Import escaped HTML as data so Python and output text cannot become Vue templates.
    path.with_suffix(".content.html").write_text(content)
    path.write_text(
        "---\n" + json.dumps(metadata, ensure_ascii=False) + "\n---\n\n"
        f'<script setup>\nimport content from "./{path.with_suffix(".content.html").name}?raw"\n</script>\n\n'
        '<div v-html="content"></div>\n'
    )


def publish_llms(output):
    source = ROOT / "llms.txt"

    def absolute_link(match):
        prefix, url, closing = match.groups()
        return prefix + rewrite_link(url, source, {}, output, "") + closing

    # The context generator emits catalog links as Markdown list items; preserve all other text.
    content = re.sub(r"(?m)^(- \[.*?\]\()([^\n)]+)(\))", absolute_link, source.read_text())
    (output / "llms.txt").write_text(content)


def prepare_site():
    generated = SOURCE / ".generated"
    if generated.exists():
        shutil.rmtree(generated)
    public = generated / "public"
    public.mkdir(parents=True)
    shutil.copyfile(SOURCE / "sail.svg", public / "sail.svg")
    publish_llms(public)
    (public / ".nojekyll").touch()
    examples = discover_examples()
    facts = discover_facts()
    facts_by_example, examples_by_fact = example_fact_links(examples, facts)
    examples_by_readme = {example.path.with_name("README.md").resolve(): example for example in examples}
    shell = Template((SOURCE / "showcase.html").read_text()).safe_substitute(
        sail_logo=render_sail_logo(),
        github_icon=render_icon("github"),
    )
    featured = sorted((e for e in examples if e.meta.get("featured")), key=lambda e: e.meta["featured"])
    present = {CATEGORIES[e.meta["tier"]] for e in examples}
    categories = [title for title in GALLERY_GROUPS if title in present]
    filters = (
        f'<button type="button" data-topic="" aria-pressed="true">{topic_icon("all")} All examples</button>'
    )
    filters += "".join(
        f'<button type="button" data-topic="{escape(category)}" aria-pressed="false">'
        f"{topic_icon(GALLERY_GROUPS[category][0])} {escape(category)}</button>"
        for category in categories
    )
    home = Template((SOURCE / "home.html").read_text()).substitute(
        hero_photo=media_url(ROOT / "multimodal/search-images-by-text/images/dog-snow.jpg", public),
        featured="\n".join(render_featured(example, public) for example in featured),
        catalog="\n".join(render_card(example) for example in examples),
        count=len(examples),
        fact_count=sum(not fact.meta.get("private") for fact in facts),
        filters=filters,
    )
    write_page(
        generated,
        "index.html",
        "Good questions. Runnable answers.",
        "Explore real tasks with Spark SQL and Python on Sail.",
        Template(shell).substitute(base="", content=home),
        showcase=True,
    )
    write_page(
        generated,
        "facts/index.html",
        "How Sail behaves",
        "Practical Sail behavior backed by executable checks.",
        render_facts(facts, examples_by_fact),
        section="Sail behavior",
    )
    write_page(
        generated,
        "404.html",
        "Page not found",
        "Find a Sail example or behavior note.",
        "<h1>Page not found</h1><p>Use Explore, Behavior, or search to find a guide.</p>",
    )
    for fact in facts:
        if not fact.meta.get("private"):
            write_page(
                generated,
                fact_url(fact),
                fact.meta["title"],
                one_line(fact),
                render_fact(fact, examples_by_fact[fact.id]),
                section="Behavior / " + CATEGORY_TITLES[fact.meta["category"]],
            )
    for example in examples:
        write_page(
            generated,
            guide_url(example),
            example.meta["title"],
            summary(example),
            render_guide(example, examples_by_readme, public, facts_by_example[example.id]),
            section=CATEGORIES[example.meta["tier"]],
        )
    navigation = {
        "examples": [
            {
                "text": category,
                "collapsed": True,
                "items": [
                    {"text": e.meta["title"], "link": "/" + guide_url(e)}
                    for e in examples
                    if CATEGORIES[e.meta["tier"]] == category
                ],
            }
            for category in categories
        ],
        "facts": [{"text": "How Sail behaves", "link": "/facts/index.html"}]
        + [
            {
                "text": title,
                "collapsed": True,
                "items": [
                    {"text": f.meta["title"], "link": "/" + fact_url(f)}
                    for f in facts
                    if not f.meta.get("private") and f.meta["category"] == category
                ],
            }
            for category, title in CATEGORY_TITLES.items()
        ],
    }
    pages = []
    for path in sorted(generated.rglob("*.md")):
        metadata = json.loads(path.read_text().splitlines()[1])
        pages.append({"url": path.relative_to(generated).with_suffix(".html").as_posix(), **metadata})
    (generated / "pages.json").write_text(json.dumps(pages, ensure_ascii=False))
    (generated / "navigation.json").write_text(json.dumps(navigation, ensure_ascii=False))
    print(f"Prepared {len(examples)} example guides and Sail behavior pages for VitePress")


def build(output):
    if output.resolve() != ROOT / "_site":
        raise ValueError("the site output must be the repository's _site directory")
    prepare_site()
    executable = ROOT / "node_modules/vitepress/bin/vitepress.js"
    if not executable.is_file():
        raise RuntimeError("Install the website dependencies from the repository root with npm ci")
    subprocess.run(["node", str(executable), "build", "site"], cwd=ROOT, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prepare-only", action="store_true", help="Generate pages for the VitePress dev server"
    )
    args = parser.parse_args()
    if args.prepare_only:
        prepare_site()
    else:
        build(ROOT / "_site")
