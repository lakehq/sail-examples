FACT = {
    "id": "vector-functions",
    "category": "ai-functions",
    "core": True,
    "text": """For embeddings, store vectors as `ARRAY<FLOAT>` and use `vector_cosine_similarity`,
        `vector_inner_product` and `vector_l2_distance` (Spark 4.2 has them too). Both engines reject
        `ARRAY<DOUBLE>`; Sail also rejects Arrow fixed-size lists (as written by NumPy or PyArrow), which
        Spark accepts. `CAST(col AS ARRAY<FLOAT>)` first.""",
    "engines": ["sail", "spark"],
    "order": 1,
}

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

V = "ARRAY(CAST({} AS FLOAT), CAST({} AS FLOAT))"
D = "ARRAY(CAST({} AS DOUBLE), CAST({} AS DOUBLE))"


def scalar(spark, sql):
    return spark.sql(sql).collect()[0][0]


def rejected(spark, sql) -> bool:
    try:
        spark.sql(sql).collect()
    except Exception as error:  # noqa: BLE001
        return "ARRAY<FLOAT>" in str(error) or "expects" in str(error)
    return False


def check(spark, engine):
    try:
        cosine = scalar(spark, f"SELECT vector_cosine_similarity({V.format(3, 4)}, {V.format(3, 4)})")
    except Exception as error:
        raise AssertionError(f"vector functions are unavailable on {engine}: {str(error)[:200]}") from error
    orthogonal = scalar(spark, f"SELECT vector_cosine_similarity({V.format(1, 0)}, {V.format(0, 1)})")
    inner = scalar(spark, f"SELECT vector_inner_product({V.format(1, 2)}, {V.format(2, 0)})")
    l2 = scalar(spark, f"SELECT vector_l2_distance({V.format(0, 0)}, {V.format(3, 4)})")
    assert abs(cosine - 1.0) < 1e-6 and abs(orthogonal) < 1e-6, (cosine, orthogonal)
    assert abs(inner - 2.0) < 1e-6 and abs(l2 - 5.0) < 1e-6, (inner, l2)

    path = Path(tempfile.mkdtemp()) / "fsl.parquet"
    pq.write_table(pa.table({"e": pa.array([[1.0, 0.0]], pa.list_(pa.float32(), 2))}), path)
    spark.read.parquet(str(path)).createOrReplaceTempView("fsl")
    double_rejected = rejected(spark, f"SELECT vector_cosine_similarity({D.format(1, 0)}, {D.format(1, 0)})")
    fixed_rejected = rejected(spark, "SELECT vector_cosine_similarity(e, e) FROM fsl")
    cast_ok = scalar(
        spark, "SELECT vector_cosine_similarity(CAST(e AS ARRAY<FLOAT>), CAST(e AS ARRAY<FLOAT>)) FROM fsl"
    )
    assert abs(cast_ok - 1.0) < 1e-6, cast_ok
    if engine == "sail":
        assert double_rejected and fixed_rejected, "Sail now accepts other array types; update this fact"
    else:
        assert double_rejected and not fixed_rejected, "Spark's array-type handling changed; update this fact"
    return {"double_rejected": double_rejected, "fixed_size_list_rejected": fixed_rejected}
