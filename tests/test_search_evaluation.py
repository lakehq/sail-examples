"""Search scores do not create labels; incomplete judgments do not become negatives."""

import ast
import json
import math
from pathlib import Path

import pytest
from catalog import ROOT, script_dependencies
from harness import run
from search_evaluation_checks import metric_reference


@pytest.mark.parametrize(
    ("table", "field"),
    [
        ("documents", "revision"),
        ("queries", "revision"),
        ("judgments", "query_revision"),
        ("judgments", "document_revision"),
        ("judgments", "judgment_revision"),
        ("judgments", "grade"),
    ],
)
@pytest.mark.parametrize("value", [1.9, 1.0, True, "1"])
def test_noninteger_numeric_input_is_rejected_before_schema_conversion(table, field, value):
    directory = ROOT / "pipelines/evaluate-search-dataset"
    tree = ast.parse((directory / "main.py").read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "curate")
    schemas = next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(getattr(target, "id", None) == "SCHEMAS" for target in node.targets)
    )
    namespace = {"SCHEMAS": schemas, "json": json}
    # Exercise the actual input boundary without importing engine or model dependencies.
    code = compile(ast.Module(body=[function], type_ignores=[]), str(directory / "main.py"), "exec")
    exec(code, namespace)  # noqa: S102 - only the tracked function with controlled globals
    fixture = json.loads((directory / "fixture.json").read_text())
    fixture[table][0][field] = value

    class NoConversion:
        def createDataFrame(self, *args, **kwargs):
            pytest.fail("invalid raw numeric input reached DataFrame conversion")

    with pytest.raises(ValueError, match=rf"{table}\.{field} must be an integer or null"):
        namespace["curate"](NoConversion(), fixture)


def test_metric_reference_matches_hand_calculated_graded_example():
    rows = [{"rank": i, "grade": grade} for i, grade in enumerate([0, 2, 1, 2], 1)]
    got = metric_reference(rows, 3)
    assert got["recall_at_k"] == pytest.approx(2 / 3)
    assert got["mrr_at_k"] == 0.5
    assert got["ndcg_at_k"] == pytest.approx((3 / math.log2(3) + 0.5) / (3 + 3 / math.log2(3) + 0.5))


def test_missing_judgment_keeps_its_rank_and_suppresses_metrics():
    got = metric_reference([{"rank": 1, "grade": None}, {"rank": 2, "grade": 2}])
    assert got["status"] == "incomplete_judgments"
    assert got["unjudged_in_top_k"] == 1
    assert [got[k] for k in ["recall_at_k", "mrr_at_k", "ndcg_at_k"]] == [None] * 3


def test_exhaustive_no_answer_query_has_undefined_rank_metrics():
    got = metric_reference([{"rank": 1, "grade": 0}, {"rank": 2, "grade": 0}])
    assert got["status"] == "no_relevant_documents"
    assert got["recall_at_k"] is None and got["mrr_at_k"] is None and got["ndcg_at_k"] is None


@pytest.mark.parametrize("engine", ["sail", "spark"])
def test_search_snapshot_rank_metrics_and_actual_loader(engine, tmp_path):
    meta = run(
        engine,
        "fact",
        Path(__file__).with_name("search_evaluation_checks.py"),
        tmp_path / engine,
        script_dependencies(ROOT / "pipelines/evaluate-search-dataset/main.py"),
        300,
    )
    assert meta["status"] == "ok", f"{engine} [{meta['status']}]: {meta.get('message')}"
