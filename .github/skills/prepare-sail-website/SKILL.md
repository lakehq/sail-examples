---
name: prepare-sail-website
description: Record and review versioned example output and generate README output sections. Maintainers own captures; ordinary contributors provide runnable examples and tests.
---

# Prepare Sail example output

Maintainers own output captures and their presentation. Contributors provide runnable
examples, README instructions, metadata, and tests under [CONTRIBUTING.md](../../../CONTRIBUTING.md).
Work from the repository root (`sail-examples/`). Do not commit, push, or publish
unless the user authorizes it. This skill is the single output-maintenance checklist.

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
only the newest accepted result; JSON preserves the full history.
Use public inputs without credentials and inspect stdout before including it.
The same command works in Docker when the repository and uv are available there.

Named README diffs can declare runnable variants with `diff main.py variant=name`.
Capture a declared variant with the additional `--variant name` argument.
The harness runs the patch in a temporary copy and keeps its history separate.

To regenerate all README output sections from accepted JSON without running examples:

```bash
uv run python tests/harness.py --render-readmes
uv run python tests/harness.py --render-readmes --check
```

The check command writes nothing and fails when a section is stale. CI runs this validation. A source change preserves the earlier output and
labels it **Recorded output**; generation never accepts a new result. Keep the
JSON and regenerated README changes together for review.

Keep older captures. Presentation labels the newest matching capture **Current output**,
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
stay in JSON and are labeled **Earlier output**. The README shows only the
newest revision. The same option works with `--variant`. Repeating an unchanged
capture adds no revision; differences ignored by comparison, such as pytest's elapsed
time, also need no revision. Changing the source still requires review.
Failed runs record the release and display a failure notice, not partial output.
A successful retry clears that failure notice. Report unresolved failures without
changing expected results just to pass. Runtime/reference checks still determine
whether the example works; a successful stdout capture alone does not.
