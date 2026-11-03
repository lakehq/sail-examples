"""Caption queries retain explicit target identity when their text is duplicated."""

from inference_checks import load_example


def check(spark, engine):
    module = load_example("multimodal/search-images-by-text")
    ranked = spark.createDataFrame(
        [
            (0, None, "a.jpg", 1),
            (5, "a.jpg", "a.jpg", 1),
            (5, "a.jpg", "b.jpg", 2),
            (6, "b.jpg", "a.jpg", 1),
            (6, "b.jpg", "b.jpg", 2),
        ],
        "query_id long, target_file string, file string, rank int",
    )
    result = module.caption_recall(ranked).first().asDict()
    assert result == {"captions": 2, "recall_at_1": 0.5, "recall_at_5": 1.0}, result
    return {"engine": engine, **result}
