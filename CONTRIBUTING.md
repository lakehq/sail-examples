# Contributing examples

Each example should teach a useful task with small, credential-free inputs and
make its results checkable. Readers run Sail. Apache Spark is a maintenance check
for portable Spark SQL and DataFrame code.

## Set up and check a change

Use [uv](https://docs.astral.sh/uv/) 0.13.4 or newer and Java 17 for the Apache Spark
checks. The repository's `.python-version` selects Python for local runs and tests.
Keep it on the newest stable Python minor supported by PySail, and check the
example dependencies when updating it. Install
[Git LFS](https://git-lfs.com) and run `git lfs install` once before cloning, since it stores the
examples' images and video.

```bash
uv sync --locked
uv run pytest -q -p no:cacheprovider
uv run ruff check .
uv run ruff format --check .
```

The tests download pinned public datasets and models and start engine processes.
Run only one test command that starts Apache Spark at a time. For a focused change,
select an example by its README `id`:

```bash
uv run pytest tests/test_examples.py tests/test_entry_points.py -k hello-sail
```

## Add an example

Use one directory containing `main.py` and `README.md`. Follow a nearby example's
layout and metadata. Keep the default small enough for a laptop, without an API
key or GPU. Clearly separate any optional longer run and its requirements.

The script has a PEP 723 header with Python requirements and exact dependency
versions. Its `run(spark)` returns a DataFrame or a dictionary of named DataFrames
without creating or stopping the session. Its runnable entry point starts Sail,
shows the results and closes the session and owned resources.

A portable example uses the same code and input files on both engines. A Sail-only
example declares `engines: sail-only` and supplies a `reference()` built independently
of Sail. Preserve comparison failures; investigate differences rather than changing
tolerances or dropping rows to make them pass.

Keep large downloads out of Git. Small committed inputs such as images, video,
audio and PDFs are stored with Git LFS through `.gitattributes`. Before committing
a new kind of binary file, add its extension with `git lfs track`;
`tests/test_binary_files.py` fails on a binary file stored directly in Git. Pin
dataset/model revisions and verify checksums where provided. Reject mismatches
before using cached or newly downloaded data.
Credit sources and record their license terms; do not assume public access grants
redistribution rights. Avoid personal or customer information in fixtures.

## Prove the result, then describe it

Engine agreement checks returned column names, normalized types and values on the
supplied inputs, within declared ordering and float tolerances. It does not prove
all behaviors, side effects or model predictions correct.

Use an independent reference or explicit expected answers for the task's key
results. For model workflows, score against appropriate references, account for
every input and distinguish errors, rejected items and uncertain predictions.
Do not call structural validation or a confidence threshold semantic approval.
A batching test with repeated inputs does not establish broader model quality.

Keep README claims scoped to what the example checks. Describe observed fixture
quality with its data and metric. Do not add performance comparisons, speed claims
or unsupported capacity recommendations. Make the commands, download requirements
and limitations clear. Retain rejected records and failures when the task needs
complete accounting.

## Update facts and generated context

A fact in `facts/` pairs a `FACT` dictionary with `check(spark, engine)`. Its guidance
must be demonstrated by that check; attribute any additional documented behavior.
Tests run the fact on its declared engines. Follow an existing fact's schema.

After changing README metadata or facts, regenerate the public context and gallery:

```bash
uv run tools/build_context.py
uv run pytest tests/test_catalog.py tests/test_context.py tests/test_readme_links.py
```

Review the generated diff as well as the source. Relative README file links are
checked; external URLs and heading anchors are not.

## Automated checks and Sail releases

`Checks` runs on pull requests and pushes to `main`. Its first job checks Python
syntax, Ruff lint and formatting, example metadata, generated context, README
links, LFS content. On PRs, the second job selects runtime
checks from the changed paths:

- Example code, inputs or captures: that example's entry point, comparison and
  related custom tests.
- Fact code: that fact's checks.
- README prose and website files: no runtime checks. A changed named runnable
  diff or runtime metadata (such as engines or tolerance) selects that example.
- Test files: the changed tests. Shared harness code, dependencies, Python,
  workflows and unknown shared inputs select the full suite.

Custom tests declare their example IDs with `pytest.mark.examples(...)`; use
parameter marks when different cases cover different examples. Unmarked tests
remain selected, so a missing mark cannot silently drop coverage. Cheap validation
runs on every PR. A PR with no runtime inputs skips the runtime job.

Pushes to `main`, weekly checks and manual checks run the full suite. To inspect
the selection or run it locally from the repository root:

```bash
uv run python tests/change_selection.py --base origin/main
uv run pytest -ra --changed-since origin/main
```

These commands compare tracked changes with the named commit. Add new files to
the Git index before using them locally; CI reads the committed PR changes.
The environment uses the Python version in `.python-version` and Java 17;
dependency versions come from the lock file, script headers and `tests/engines.toml`.

Weekly and manual runs test both the pinned Sail version and the newest stable
`pysail` release that resolves for the repository's Python version. The release check changes only Sail;
the Spark client, Arrow, pandas and example-specific dependencies retain their pins.
The workflow records Python and the resolved requirements in its summary and uploads them with
the JUnit results, including when tests fail. A failed compatibility run fails the
workflow; it does not silently update pins or open a pull request.

Entry-point tests also compare console output with the latest reviewed capture revision
in `outputs.json` at or before the Sail version under test. A difference fails
the test and saves `expected.txt`, `actual.txt`, `output.diff`, and version details
under `.reports/output-comparisons/<example-path>/`. The comparison ignores CRLF
line endings and elapsed time in pytest's success summary. Both raw outputs are
retained. Result values, row order, spacing, test counts, and final newlines must
match. A source change is reported but does not bypass the comparison.

An example without an applicable capture still runs its execution checks. Its
comparison is reported as `missing-baseline`, so contributors do not need to
prepare website captures. `execution-incomplete` means execution or comparison
did not complete; inspect the test failure. CI includes comparison counts in its
summary and saves the reports with the test artifacts.

Review output differences before accepting a new capture through the
[maintainer workflow](.github/skills/prepare-sail-website/SKILL.md#record-versioned-output).
Tests never update `outputs.json`, and a failed weekly run never becomes the next baseline.
README sections marked `sail-output` are generated from accepted captures. Do not
edit their contents by hand; the maintainer workflow explains how to refresh them.
To check all example commands and their recorded output locally:

```bash
uv run pytest -ra tests/test_entry_points.py
```

The override applies to the harness and all standalone CLI tests. To reproduce a
compatibility run, copy `SAIL_EXAMPLES_SAIL_DEPS` from its recorded Sail requirements:

```bash
SAIL_EXAMPLES_SAIL_DEPS='["pysail==0.7.3", "pyspark-client==4.1.3", "pyarrow==26.0.0", "pandas==3.0.6"]' \
  uv run pytest -ra
```

Use the actual recorded versions in place of the illustrative list above. Ordinary
`uv run quickstart/query-files-with-sql/main.py` still uses that script's pins. A release check executes that same
script in an isolated environment with the overridden engine dependencies.

A failure can be a Sail regression, an outdated fact, a dependency conflict or an
unavailable dataset/model host. Inspect the failing test and its recorded versions
before changing expectations. A passing run covers these fixtures and this dependency
combination; it does not cover unreleased Sail commits or every customer workload.
Scheduled failures appear in GitHub Actions; maintainers should enable the GitHub
notifications they need. The workflow does not send messages or create issues.
