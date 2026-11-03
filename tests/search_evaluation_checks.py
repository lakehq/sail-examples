"""Independent metric arithmetic and data-integrity checks for search evaluation."""

import copy
import importlib.util
import json
import math
import tempfile
from collections import Counter, defaultdict
from pathlib import Path


def metric_reference(rows, k=3):
    """Python ranking arithmetic; never remove an unjudged candidate from its rank."""
    rows = sorted(rows, key=lambda r: r["rank"])
    assert [r["rank"] for r in rows] == list(range(1, len(rows) + 1))
    grades = [r["grade"] for r in rows]
    judged = sum(g is not None for g in grades)
    positives = sum(g is not None and g > 0 for g in grades)
    status = (
        "incomplete_judgments"
        if judged < len(rows)
        else "no_relevant_documents"
        if not positives
        else "scored"
    )
    result = {
        "documents": len(rows),
        "judged_documents": judged,
        "relevant_documents": positives,
        "unjudged_in_top_k": sum(r["grade"] is None for r in rows[:k]),
        "status": status,
        "recall_at_k": None,
        "mrr_at_k": None,
        "ndcg_at_k": None,
    }
    if status != "scored":
        return result
    hits = [r["rank"] for r in rows[:k] if r["grade"] > 0]
    dcg = sum((2 ** r["grade"] - 1) / math.log2(r["rank"] + 1) for r in rows[:k])
    ideal = sum(
        (2**grade - 1) / math.log2(rank + 1) for rank, grade in enumerate(sorted(grades, reverse=True)[:k], 1)
    )
    return {
        **result,
        "recall_at_k": len(hits) / positives,
        "mrr_at_k": 1 / min(hits) if hits else 0.0,
        "ndcg_at_k": dcg / ideal,
    }


def example():
    path = Path(__file__).resolve().parent.parent / "pipelines/evaluate-search-dataset/main.py"
    spec = importlib.util.spec_from_file_location("search_evaluation", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(spark, engine):
    import re

    module = example()
    fixture = json.loads((module.HERE / "fixture.json").read_text())
    with tempfile.TemporaryDirectory(prefix="search-evaluation-check-") as directory:
        root = Path(directory) / "export"
        outputs = {
            name: [r.asDict(recursive=True) for r in frame.collect()]
            for name, frame in module.run(spark, root).items()
        }
        for name in ["documents", "queries", "judgments"]:
            assert all(not key.startswith("_") for row in outputs[name] for key in row)
        documents = {r["document_id"]: r for r in outputs["documents"]}
        queries = {r["query_id"]: r for r in outputs["queries"]}
        assert set(documents) == {
            "address",
            "delays",
            "password",
            "receipts",
            "refunds",
            "returns",
            "security",
        }
        assert documents["refunds"]["revision"] == 2
        assert set(queries) == {
            "address",
            "missing",
            "password",
            "refund",
            "return",
            "security",
            "shipping",
            "unanswerable",
        }
        assert queries["refund"]["revision"] == 2
        assert len({r["content_group"] for r in documents.values()}) == 7
        assert len({r["content_group"] for r in queries.values()}) == 8
        assert {r["split"] for r in queries.values()} == {"train", "validation", "test"}
        groups = defaultdict(set)
        for row in queries.values():
            groups[row["query_group"]].add(row["split"])
        assert all(len(splits) == 1 for splits in groups.values())
        assert len(outputs["judgments"]) == 54
        assert len(outputs["quarantine"]) == 17
        assert len(outputs["rankings"]) == 112
        assert len(outputs["metrics"]) == 16
        assert sum(row["rows"] for row in outputs["consumer"]) == 112
        # Every original input row is either retained once or quarantined once.
        retained = {
            "document": outputs["documents"],
            "query": outputs["queries"],
            "judgment": outputs["judgments"],
        }
        for kind, name in [("document", "documents"), ("query", "queries"), ("judgment", "judgments")]:
            found = [r["row_id"] for r in retained[kind]] + [
                r["row_id"] for r in outputs["quarantine"] if r["record_type"] == kind
            ]
            assert Counter(found) == Counter(r["row_id"] for r in fixture[name])
        source = {r["row_id"]: r for name in ["documents", "queries", "judgments"] for r in fixture[name]}
        assert all(json.loads(row["payload"]) == source[row["row_id"]] for row in outputs["quarantine"])
        expected_reasons = {
            "d01-old": "superseded_revision",
            "d08-copy": "duplicate_content",
            "d09-left": "conflicting_latest_revision",
            "d09-right": "conflicting_latest_revision",
            "d10-invalid": "invalid_revision",
            "q01-old": "superseded_revision",
            "q09-copy": "duplicate_content",
            "q10-left": "conflicting_latest_revision",
            "q10-right": "conflicting_latest_revision",
            "j-conflict": "conflicting_latest_revision",
            "j-missing-security": "conflicting_latest_revision",
            "j-stale-query": "revision_mismatch",
            "j-stale-document": "revision_mismatch",
            "j-missing-document": "missing_document",
            "j-invalid-grade": "invalid_grade",
            "j-old-label": "superseded_revision",
            "j-refund-refunds": "exact_duplicate",
        }
        assert {r["row_id"]: r["reason"] for r in outputs["quarantine"]} == expected_reasons
        positive = {
            "refund": {"refunds": 2, "returns": 1, "receipts": 1},
            "return": {"returns": 2, "refunds": 1, "receipts": 1},
            "shipping": {"delays": 2},
            "address": {"address": 2},
            "password": {"password": 2, "security": 1},
            "security": {"security": 2},
            "missing": {"refunds": 2, "returns": 1},
            "unanswerable": {},
        }
        expected = {
            (qid, did): grade.get(did, 0)
            for qid, grade in positive.items()
            for did in documents
            if (qid, did) not in {("missing", "receipts"), ("missing", "security")}
        }
        assert {(r["query_id"], r["document_id"]): r["grade"] for r in outputs["judgments"]} == expected
        assert {r["label_origin"] for r in outputs["judgments"]} == {"authored_expected"}
        ranked = defaultdict(list)

        def tokenize(text):
            return set(re.findall("[a-z0-9]+", text.lower()))

        for row in outputs["rankings"]:
            assert math.isfinite(row["score"])
            assert row["grade"] == expected.get((row["query_id"], row["document_id"]))
            if row["method"] == "token_overlap":
                assert row["score"] == len(tokenize(row["query"]) & tokenize(row["document"]))
            ranked[row["method"], row["query_id"]].append(row)
        actual_metrics = {(r["method"], r["query_id"]): r for r in outputs["metrics"]}
        assert set(ranked) == set(actual_metrics)
        for key, rows in ranked.items():
            assert {r["document_id"] for r in rows} == set(documents)
            assert [r["document_id"] for r in sorted(rows, key=lambda r: r["rank"])] == [
                r["document_id"] for r in sorted(rows, key=lambda r: (-r["score"], r["document_id"]))
            ]
            expected_metric = metric_reference(rows, module.K)
            for field, value in expected_metric.items():
                actual = actual_metrics[key][field]
                matches = (
                    actual == value
                    if not isinstance(value, float)
                    else math.isclose(actual, value, rel_tol=1e-9, abs_tol=1e-12)
                )
                assert matches, (key, field, actual, value)
        assert Counter(r["status"] for r in outputs["metrics"]) == {
            "scored": 12,
            "incomplete_judgments": 2,
            "no_relevant_documents": 2,
        }
        try:
            module.consume_export(root, "another-snapshot")
        except ValueError as error:
            assert "revision mismatch" in str(error)
        else:
            raise AssertionError("mixed snapshot was accepted")
        manifest_path = root / "manifest.json"
        manifest_text = manifest_path.read_text()
        manifest = json.loads(manifest_text)
        assert manifest["processing_config"] == {
            "k": 3,
            "embedding_max_tokens": 128,
            "embedding_pooling": "attention_mask_mean",
            "embedding_normalization": "l2",
            "lexical_tokens": "distinct_lowercase_ascii_alphanumeric",
            "tie_break": "document_id_ascending",
        }
        manifest["processing_config"]["k"] = 4
        manifest_path.write_text(json.dumps(manifest))
        try:
            module.consume_export(root, fixture["snapshot_id"])
        except ValueError as error:
            assert "processing configuration mismatch" in str(error)
        else:
            raise AssertionError("unsupported processing configuration was accepted")
        finally:
            manifest_path.write_text(manifest_text)
        first_file = manifest["tables"]["rankings"]["files"][0]["path"]
        with (root / first_file).open("ab") as output:
            output.write(b"changed")
        try:
            module.consume_export(root, fixture["snapshot_id"])
        except ValueError as error:
            assert "checksum mismatch" in str(error)
        else:
            raise AssertionError("changed exported bytes were accepted")
        # The accounting key must exist before any DataFrame or nullable join.
        for name in ["documents", "queries", "judgments"]:
            for row_id in [None, 42, " \t", "missing"]:
                changed = copy.deepcopy(fixture)
                if row_id == "missing":
                    changed[name][0].pop("row_id")
                else:
                    changed[name][0]["row_id"] = row_id
                try:
                    module.curate(spark, changed)
                except ValueError as error:
                    assert "row_id must be a nonempty string" in str(error)
                else:
                    raise AssertionError(f"invalid row_id accepted in {name}: {row_id!r}")
        # Raw integer fields must be checked before either client's schema conversion.
        for name, field in [
            ("documents", "revision"),
            ("queries", "revision"),
            ("judgments", "query_revision"),
            ("judgments", "document_revision"),
            ("judgments", "judgment_revision"),
            ("judgments", "grade"),
        ]:
            for value in [1.9, 1.0, True, "1"]:
                changed = copy.deepcopy(fixture)
                changed[name][0][field] = value
                try:
                    module.curate(spark, changed)
                except ValueError as error:
                    assert f"{name}.{field} must be an integer or null" in str(error)
                else:
                    raise AssertionError(f"noninteger {name}.{field} accepted: {value!r}")
        # Equal query content assigned to another group must quarantine both IDs.
        changed = copy.deepcopy(fixture)
        changed["queries"].append(
            {
                "row_id": "cross-group",
                "query_id": "cross-group",
                "revision": 1,
                "text": queries["security"]["text"],
                "query_group": "delivery",
            }
        )
        curated = module.curate(spark, changed)
        assert "security" not in {r.query_id for r in curated["queries"].collect()}
        quarantined = {r.row_id: r.reason for r in curated["quarantine"].collect()}
        assert quarantined["cross-group"] == quarantined["q06"] == "duplicate_query_across_groups"
        incomplete = module.evaluate(
            spark.createDataFrame(
                [
                    ("ranker", "q", "test", "d1", 1, 2),
                    ("ranker", "q", "test", "d2", 2, None),
                ],
                "method string, query_id string, split string, document_id string, rank int, grade long",
            )
        ).first()
        assert incomplete.recall_at_k is None
        # Invalid newest payloads cannot resurrect an older text or judgment.
        for name, identity, payload in [
            ("documents", "document_id", {"text": "\t \n"}),
            ("documents", "document_id", {"text": None}),
            ("queries", "query_id", {"text": "\t \n"}),
            ("judgments", "query_id", {"grade": 3}),
        ]:
            changed = copy.deepcopy(fixture)
            original = next(
                r
                for r in changed[name]
                if r["row_id"]
                == {
                    "documents": "d07",
                    "queries": "q06",
                    "judgments": "j-security-security",
                }[name]
            )
            newest = {**original, **payload, "row_id": "invalid-newest"}
            version = "judgment_revision" if name == "judgments" else "revision"
            newest[version] += 1
            changed[name].append(newest)
            actual = module.curate(spark, changed)
            retained = [r.asDict() for r in actual[name].collect()]
            assert not any(
                r[identity] == original[identity]
                and (name != "judgments" or r["document_id"] == original["document_id"])
                for r in retained
            )
            quarantined = {r.row_id: r for r in actual["quarantine"].collect()}
            assert json.loads(quarantined["invalid-newest"].payload) == newest
        changed = copy.deepcopy(fixture)
        original = next(r for r in changed["judgments"] if r["row_id"] == "j-security-security")
        changed["judgments"].append({**original, "row_id": "invalid-peer", "grade": 3})
        actual = module.curate(spark, changed)
        assert not any(
            r.query_id == "security" and r.document_id == "security" for r in actual["judgments"].collect()
        )
        reasons = {r.row_id: r.reason for r in actual["quarantine"].collect()}
        assert reasons["j-security-security"] == "invalid_latest_revision"
        from pyspark.sql import functions as F

        punctuation = (
            spark.createDataFrame([("!!!", "???")], "query string, document string")
            .select(
                F.size(
                    F.array_intersect(
                        module.lexical_tokens(F.col("query")), module.lexical_tokens(F.col("document"))
                    )
                ).alias("overlap")
            )
            .first()
        )
        assert punctuation.overlap == 0
        # Real model control: more than one inference batch and an explicit Arrow tail.
        import numpy as np
        import pyarrow as pa

        texts = ["!!!", "???"] + [f"control text {i}" for i in range(31)]
        schema = pa.schema([("kind", pa.string()), ("id", pa.string()), ("text", pa.string())])
        records = [{"kind": "query", "id": str(i), "text": text} for i, text in enumerate(texts)]
        batches = [
            pa.RecordBatch.from_pylist(records[:19], schema=schema),
            pa.RecordBatch.from_pylist(records[19:], schema=schema),
        ]
        result_batches = list(module.embed(iter(batches)))
        assert [batch.num_rows for batch in result_batches] == [16, 3, 14]
        assert all(batch.schema.field("embedding").type == pa.list_(pa.float32()) for batch in result_batches)
        embedded = [row for batch in result_batches for row in batch.to_pylist()]
        assert [r["id"] for r in embedded] == [str(i) for i in range(33)]
        vectors = np.asarray([r["embedding"] for r in embedded])
        assert vectors.shape == (33, 384) and np.isfinite(vectors).all()
        assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0, atol=1e-5)
    return {
        "all_ranked_pairs": 112,
        "independent_metric_checks": 16,
        "complete_loader_rows": 112,
        "changed_bytes_rejected": True,
        "source_payloads_preserved": True,
        "bounded_embedding_texts": 33,
    }
