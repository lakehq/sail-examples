"""Pure input contracts and separate engine checks for spoken-intent analytics."""

import importlib.util
import io
import json
import os
import re
import wave
from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run

pytestmark = pytest.mark.examples("support-intent-analytics")


@pytest.fixture
def example():
    path = ROOT / "analytics/support-intent-analytics/main.py"
    spec = importlib.util.spec_from_file_location("support_intent_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def audio(seconds=1, sample=0):
    stream = io.BytesIO()
    with wave.open(stream, "wb") as writer:
        writer.setnchannels(1)
        writer.setsampwidth(2)
        writer.setframerate(8000)
        writer.writeframes(int(sample).to_bytes(2, "little", signed=True) * (8000 * seconds))
    return stream.getvalue()


def row(text, label=0, seconds=1, sample=0):
    return {"audio": {"bytes": audio(seconds, sample)}, "transcription": text, "intent_class": label}


@pytest.mark.parametrize(
    "text,label",
    [
        ("Can I use it abroad?", "abroad"),
        ("Change my address", "address"),
        ("The app crashes", "app_error"),
        ("What is the withdrawal limit?", "atm_limit"),
        ("What is my balance?", "balance"),
        ("I need a business loan", "business_loan"),
        ("My card was declined", "card_issues"),
        ("How do I deposit cash?", "cash_deposit"),
        ("Cancel my direct debit", "direct_debit"),
        ("Freeze my card", "freeze"),
        ("Make a large transfer", "high_value_payment"),
        ("Open a joint account", "joint_account"),
        ("Show recent transactions", "latest_transactions"),
        ("Pay my electricity bill", "pay_bill"),
        ("My address and balance", "unknown"),
        ("A completely new request", "unknown"),
        (None, "unknown"),
    ],
)
def test_rule_examples_are_explicit_and_abstain_on_ties(example, text, label):
    assert example.classify_text(text) == label


def test_selection_and_split_do_not_consume_labels(example, tmp_path):
    rows = [row(f"request {i}", i % 14, sample=i) for i in range(20)]
    first = example.prepare_requests(rows, tmp_path / "first", max_clips=8, max_seconds=7)
    changed = [{**item, "intent_class": (item["intent_class"] + 1) % 14} for item in rows]
    second = example.prepare_requests(changed, tmp_path / "second", max_clips=8, max_seconds=7)
    visible = lambda records: [(r["request_id"], r["duplicate_group"], r["split"]) for r in records]
    assert visible(first) == visible(second)
    assert len(first) == 7
    assert sum(r["seconds"] for r in first) == 7
    assert all(Path(r["path"]).stem == r["request_id"] for r in first)


def test_duplicate_groups_are_transitive_and_never_cross_splits(example, tmp_path):
    rows = [row("Same text!", sample=1), row("same text", sample=2), row("different", sample=2)]
    selected = example.prepare_requests(rows, tmp_path)
    assert len({r["duplicate_group"] for r in selected}) == 1
    assert len({r["split"] for r in selected}) == 1
    assert len({r["request_id"] for r in selected}) == 3


def test_decode_errors_keep_their_request_and_status(example, tmp_path):
    rows = [
        row("normal", sample=1),
        {"audio": {"bytes": b"not audio"}, "transcription": "balance", "intent_class": 4},
    ]
    selected = example.prepare_requests(rows, tmp_path)
    errors = [r for r in selected if r["decode_status"] == "decode_error"]
    assert len(selected) == 2
    assert len(errors) == 1 and errors[0]["seconds"] == 0


def test_selection_is_a_prefix_instead_of_skipping_over_longer_audio(example, tmp_path):
    rows = [row(f"request {i}", seconds=3 if i % 2 else 1, sample=i) for i in range(8)]
    complete = example.prepare_requests(rows, tmp_path / "all", max_clips=8, max_seconds=100)
    bounded = example.prepare_requests(rows, tmp_path / "bounded", max_clips=8, max_seconds=4)
    expected, seconds = [], 0
    for item in complete:
        if seconds + item["seconds"] > 4:
            break
        expected.append(item["request_id"])
        seconds += item["seconds"]
    assert [item["request_id"] for item in bounded] == expected


def test_truncated_audio_is_retained_as_an_error(example, tmp_path):
    item = row("balance")
    item["audio"]["bytes"] = item["audio"]["bytes"][:-100]
    selected = example.prepare_requests([item], tmp_path)
    assert selected[0]["decode_status"] == "decode_error"


@pytest.mark.parametrize(
    "clips,seconds",
    [
        (0, 1),
        (1, 0),
        (True, 1),
        (1, float("nan")),
        (564, 1),
        (1, 3601),
        (1, True),
    ],
)
def test_unbounded_or_empty_selection_is_rejected(example, tmp_path, clips, seconds):
    with pytest.raises(ValueError, match="bounds"):
        example.prepare_requests([row("balance")], tmp_path, clips, seconds)


def test_longer_cli_bounds_are_explicit(example):
    args = example.parse_args(["--max-clips", "96", "--max-seconds", "600", "--json"])
    assert (args.max_clips, args.max_seconds, args.json) == (96, 600, True)


@pytest.mark.parametrize("arguments", [["--max-clips", "564"], ["--max-seconds", "inf"]])
def test_invalid_cli_bounds_fail_before_engine_startup(example, arguments):
    with pytest.raises(SystemExit) as error:
        example.parse_args(arguments)
    assert error.value.code == 2


@pytest.mark.parametrize("json_output", [False, True], ids=["table", "json"])
def test_cli_stabilizes_display_without_rounding_json(tmp_path, json_output):
    script = tmp_path / "summary_cli.py"
    source = ROOT / "analytics/support-intent-analytics/main.py"
    values = [0.3333333333333333, 0.33333333333333337, 0.3333333433333333]
    arguments = ["--json"] if json_output else []
    # Supply results at the audio/model boundary, keeping the real CLI and Sail display.
    script.write_text(f"""import importlib.util
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("support_intent_cli", {str(source)!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def fixture_results(spark, *args):
    summary = spark.createDataFrame(
        list(enumerate({values!r})), "case int, macro_f1_all_14 double"
    ).orderBy("case")
    topics = spark.createDataFrame([("fixture",)], "topic string")
    return {{"summary": summary, "topics": topics}}

with patch.object(module, "run", fixture_results):
    module.main({arguments!r})
""")
    meta = run("sail", "entry-point", script, tmp_path / "run", [], 120)
    assert meta["status"] == "ok", meta
    output = meta["capture"]["output"]
    if json_output:
        assert [row["macro_f1_all_14"] for row in json.loads(output)["summary"]] == values
    else:
        displayed = re.findall(r"^\|[012]\s*\|([^|]+)\|$", output, re.MULTILINE)
        assert [value.strip() for value in displayed] == [
            "0.333333333333",
            "0.333333333333",
            "0.333333343333",
        ]


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_intent_analytics_preserves_failures_and_separate_denominators(engine, tmp_path):
    phase = os.environ.get("SUPPORT_INTENT_CHECK", "fixtures")
    assert phase in {"fixtures", "longer"}, "SUPPORT_INTENT_CHECK must be fixtures or longer"
    longer = phase == "longer"
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("support_intent_checks.py"),
        tmp_path,
        script_dependencies(ROOT / "analytics/support-intent-analytics/main.py")
        if longer
        else ["av==18.0.0"],
        900 if longer else 120,
        {
            "SAIL_EXECUTION__BATCH_SIZE": "3",
            "HF_HUB_OFFLINE": "0" if longer else "1",
            "SUPPORT_INTENT_CHECK": phase,
        },
    )
    assert meta["status"] == "ok", f"{engine}: {meta}"
