---
name: prepare-sail-website
description: Prepare new or changed Sail examples for the sail-examples showcase, including editorial polish, versioned output captures, and visual checks. Use for maintainer website preparation, not ordinary example contributions.
---

# Prepare Sail examples for the website

Maintainers own website presentation. Contributors provide runnable examples,
README instructions, metadata, and tests under [CONTRIBUTING.md](../../../CONTRIBUTING.md).
Do not ask contributors to create pages, record website captures, design previews,
or perform visual QA. This skill is the single website checklist; keep it here.

Work from the repository root (`sail-examples/`). Scope preparation to the requested
examples. Do not commit, push, change hosting, or deploy unless the user authorizes it.

## Prepare the page

The Python generator discovers example READMEs and prepares VitePress pages automatically. There
is no page registry. A new example can render before it has an output capture or
preview. Missing presentation assets are maintainer follow-up work, not a reason
to reject the example. A missing capture omits the Output section; never invent one.

Edit the example's README and metadata for the guide and its Python files for code.
Record results through the capture command below. Never hand-edit generated
`sail-output` README sections or generated `_site/` files.
Keep the website and GitHub README aligned by editing these shared sources.

- Lead with a useful question or task. Keep prose short and explain unfamiliar
  terms, including model names such as CLIP. Put detailed contracts after the result.
- State prerequisites and download sizes before running. Follow CONTRIBUTING's
  repository-root command convention for first runs, reruns, and tests. Name exact
  paths when asking readers to edit, save, or open files.
- Show execution results as console output. Keep Markdown/HTML tables for schemas,
  explanations, and comparisons. Use captioned images for visual previews, with
  source credits and a reproducible connection to the example's result.
- When suggesting a change, use a short `diff main.py variant=name` fence with exact source
  context. The builder derives original line numbers, uses `+` for insertions,
  and rejects missing or ambiguous matches. Follow with the explicit rerun command
  and “Output after this change”, then a `<!-- sail-output:start variant=name -->`
  marker and a `<!-- sail-output:end -->` marker, each on its own line. The capture
  tool runs that displayed patch in a temporary copy. Its output has a separate
  version history under `variants` in `outputs.json`.
- Refine the task-focused `title` and `summary`; keep `description` for fuller
  selection guidance. Use accurate tags for discovery and related-example cards.
  Add relevant `facts` IDs for behavior links; no relevant note means an empty list.
  Use `featured` and a `preview` path only when promoting an example on the homepage.

## Record versioned output

Run `uv run pytest -ra tests/test_entry_points.py` from the repository root to
compare example stdout with reviewed captures. To check one example, add
`-k query-files-with-sql`. Inspect its `.reports/output-comparisons/` directory
before accepting a changed result. The comparison preserves history and records
source changes separately; it never treats a source change as permission to skip.
Use the capture command below only after reviewing the output change, then review
the resulting `outputs.json` diff. Preserve earlier releases.

Read the example's PEP 723 header to select its pinned Sail version. For example,
this command captures the SQL taxi example on Sail 0.7.3:

```bash
uv run python tests/harness.py --capture-output query-files-with-sql --sail-version 0.7.3
```

Replace the example ID and version for the requested capture. The harness runs the
entry point with pinned dependencies, changing only Sail for the selected release.
It saves stdout, Python/package versions, capture date, and script SHA-256 beside
that example in `outputs.json`. Logs stay in `.reports/output-captures/`.
It also regenerates that example's README output sections. The README contains
only the newest accepted result; the website shows the full history from JSON.
The website replaces generated README sections, so it never displays both copies.
Use public inputs without credentials and inspect stdout before including it.
The same command works in Docker when the repository and uv are available there.

To capture a named change, add its variant name:

```bash
uv run python tests/harness.py --capture-output query-files-with-sql --sail-version 0.7.3 --variant queens
```

The source remains unchanged. Entry-point tests run named variants too and compare
them with their own accepted captures. Review each variant's output before recording it.

To regenerate all README output sections from accepted JSON without running examples:

```bash
uv run python tests/harness.py --render-readmes
uv run python tests/harness.py --render-readmes --check
```

The check command writes nothing and fails when a section is stale. Both CI
workflows run this validation. A source change preserves the earlier output and
labels it **Recorded output**; generation never accepts a new result. Keep the
JSON and regenerated README changes together for review.

Keep older captures. The page labels the newest matching capture **Current output**,
and changed earlier output **Output through Sail N**, with the versions actually
captured. Identical output shares an entry across consecutive verified releases.
A changed script or later failed capture makes old output **Recorded output**.
Do not imply that untested releases match.

A different result or script for an already captured release stops the harness.
After reviewing the diff and the affected correctness checks, accept a new revision
with an explicit reason:

```bash
uv run python tests/harness.py --capture-output hello-sail --sail-version 0.7.3 --accept-revision "Reviewed rounding of displayed monetary totals"
```

The new revision becomes the comparison baseline for that release. Earlier captures
stay in JSON and on the website as **Earlier output**. The README shows only the
newest revision. The same option works with `--variant`. Repeating an unchanged
capture adds no revision; differences ignored by comparison, such as pytest's elapsed
time, also need no revision. Changing the source still requires review.
Failed runs record the release and display a failure notice, not partial output.
A successful retry clears that failure notice. Report unresolved failures without
changing expected results just to pass. Runtime/reference checks still determine
whether the example works; a successful stdout capture alone does not.

## Keep shared presentation consistent

`tools/build_site.py` prepares ignored `site/.generated/` content and invokes
VitePress. `site/.vitepress/` owns configuration and the shared theme. Its custom
layout preserves the homepage in `site/home.html` and `site/showcase.html`;
`site/site.css` is scoped to that homepage. Guides use VitePress's documentation
layout, sidebar, local search, and outline. Generated HTML is imported as data,
so source code and output never become executable Vue templates. Use these shared features instead of adding per-example implementations:

- Category names from `CATEGORY_TITLES` and `GALLERY_GROUPS` in
  `tools/build_context.py`; inline category icons from `site/icons.svg`.
- The complete official Sail mark in `site/sail.svg`, preserving all paths, and
  the GitHub icon with an accessible label. Keep icon/license sources intact.
- The light/dark palette and guide fonts from `https://docs.lakesail.com/platform/`,
  with VitePress’s native theme switch on the homepage and guides. Follow the system
  theme by default. Preserve the homepage geometry and serif hero, visible focus,
  and usable mobile layouts.
- Use VitePress's copy button beside runnable shell blocks, including behavior
  check commands. Keep buttons visible and copy commands without labels or line numbers.
- Expandable, highlighted Python viewers sourced directly from every sibling
  `.py` file, with `main.py` first. Keep line numbers outside copied code and
  retain GitHub links. Expansion works without JavaScript.
- Behavior in the main navigation and homepage, with detail pages generated from
  `facts/*.py`. Notes include full code and check commands; example links work in
  both directions. Use a responsive topic grid and distinct note cards on the index.
  Keep its outline at category level. Do not rename these notes “reference.”
- Two stable “Keep exploring” cards selected by shared tags, behavior notes, and
  category. Link directly to other examples; do not imply a course order.

## Metadata and social previews

The build derives discovery metadata from the same generated pages as the website.
New example and public behavior pages get canonical URLs, sitemap entries,
and Open Graph/Twitter tags automatically. The 404 page is marked
`noindex` and excluded from discovery files and social images. Metadata is in the
initial HTML and remains correct after VitePress navigation.

The website publishes the repository's generated `llms.txt`. Only relative catalog
links change, becoming absolute GitHub URLs. Its guidance, descriptions, ordering,
and external links stay intact. Update its sources and run `uv run tools/build_context.py`
before building the website; do not add a separate website summary or catalog.

Edit `site/social-preview.svg` to change the shared image design. Its layout follows
[LakeSail's social preview](https://lakesail.com/og-default-v2.png). The renderer in
`site/.vitepress/metadata.mjs` fills the template with each page's title, category,
description excerpt, domain, and the complete `site/sail.svg` mark. It measures text
with bundled Outfit fonts, wraps titles, and shortens description excerpts to fit.
Keep page titles complete. A title that cannot fit stops the build with its text.

Run `npm run site:build` from `sail-examples/` to regenerate the JPGs and metadata.
The files go to `_site/social/`; they are build artifacts, not files to commit.
Each JPG is 1200 × 630. Its filename includes a content hash so changed previews
get new URLs. Inspect the homepage, an example, a behavior note, and the longest
title after template or font changes. Social platforms may still cache page metadata
until the page is fetched again. Their final previews need a publicly reachable URL.

`site/fonts/outfit-400.ttf` and `outfit-700.ttf` are static instances of the Latin
WOFF2 in `@fontsource-variable/outfit@5.3.0`. They carry the adjacent SIL OFL license.
The renderer uses these fonts instead of system fonts. To recreate them after an
intentional font update, run from `sail-examples/`:

```bash
uv run --no-project --with fonttools --with brotli python - <<'PY'
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

source = "node_modules/@fontsource-variable/outfit/files/outfit-latin-wght-normal.woff2"
for weight, style in ((400, "Regular"), (700, "Bold")):
    font = TTFont(source)
    font = instantiateVariableFont(font, {"wght": weight}, updateFontNames=True)
    names = {1: "Outfit", 2: style, 4: f"Outfit {style}", 6: f"Outfit-{style}"}
    for record in font["name"].names:
        if record.nameID in names:
            record.string = names[record.nameID].encode(record.getEncoding())
    font.flavor = None
    font.save(f"site/fonts/outfit-{weight}.ttf")
PY
```

Keep the Outfit license with updated font files. The build copies font licenses and
the GitHub icon notice into `_site/third-party-notices.txt`. Preserve these notices
when changing assets. The JPG renderer and encoder run only during the build;
visitors and social crawlers receive static images.

## Validate and preview

Use Node 24 and npm. From the repository root (`sail-examples/`):

```bash
npm ci --ignore-scripts
uv run tools/build_context.py
uv run pytest -q tests/test_catalog.py tests/test_context.py tests/test_readme_links.py tests/test_output_presentation.py tests/test_site.py
npm run site:preview -- --host 127.0.0.1 --port 8000
```

The site tests rebuild `_site/`. Open `http://localhost:8000`, then the requested
example pages. Use `npm run site:dev` for the VitePress development server;
after editing README or Python sources, rerun `npm run site:prepare` to regenerate pages.
Serve builds over HTTP; VitePress modules do not support opening HTML through `file://`.
The site is published at `https://examples.lakesail.com/sail/`. Set
`SAIL_EXAMPLES_ORIGIN` to another HTTPS origin when the domain changes.
`SAIL_EXAMPLES_BASE` defaults to `/`; CI and the deploy workflow build with `/sail/`.
Keep both variables the same for building and previewing. The deploy copies
`robots.txt` and `404.html` to the host root.

The theme pins the same stable VitePress release as the platform docs. `package.json`
overrides its Vite dependency to a patched release; retain a compatible patched
version when updating the lockfile. Browser-test the development server after upgrades.

Both existing workflows run site checks over all discovered examples. They check
page/catalog inclusion, links, embedded source, output histories when present,
icons, and source positions in diffs. Missing captures are allowed so contributor
PRs can pass. Maintainers must identify missing or stale captures during preparation.

Check light/dark/system themes at narrow and wide widths, copied commands, source
expansion, output readability, behavior links, and related cards. Test search and
category filters after catalog changes. Prefer fixing shared rendering defects
once. Human review must judge teaching quality and whether an edit is useful.
Run affected runtime/reference checks when changing executable examples.

Finish by reporting prepared pages, checks run, and any missing captures, failed
runs, or remaining visual work. Do not claim website preparation complete while
requested pages still have unexplained gaps.

## Publishing, when authorized

Local builds do not publish. The `Checks` workflow uploads a `site-preview`
artifact built for `/sail/`; serve it from a `sail/` folder to preview it. The
`Deploy site` workflow builds `main` for `/sail/` in a job without secrets, then a
separate job deploys it to Vercel with the `VERCEL_TOKEN`, `VERCEL_ORG_ID` and
`VERCEL_PROJECT_ID` secrets of the `production` environment. `site/vercel.json` redirects paths outside `/sail/`
to `/sail/`. Require **Python, guides and site** and **Examples (pinned Sail)** in
branch rules. Changes to hosting, secrets, branch rules, or publication need user
authority; this skill does not grant it.
