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
