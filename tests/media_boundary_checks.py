"""Exercise complete video model chunks and PDF text overlap boundaries."""

import os
import tempfile
from pathlib import Path

import pyarrow as pa
from inference_checks import compare_rows, load_example, observe_model_calls


def video_check(spark, engine):
    import av
    import numpy as np

    module = load_example("multimodal/search-video-frames")
    with tempfile.TemporaryDirectory(prefix="video-chunks-") as directory:
        paths = []
        for name, count in (("long.mp4", 65), ("tail.mp4", 33)):
            path = Path(directory) / name
            with av.open(str(path), mode="w") as container:
                stream = container.add_stream("mpeg4", rate=1)
                stream.width, stream.height, stream.pix_fmt = 96, 64, "yuv420p"
                for i in range(count):
                    pixels = np.zeros((64, 96, 3), dtype=np.uint8)
                    pixels[:, :, 0] = (np.arange(96)[None, :] + i * 3) % 256
                    pixels[:, :, 1] = (np.arange(64)[:, None] * 3 + i * 5) % 256
                    pixels[:, :, 2] = (i * 7) % 256
                    for packet in stream.encode(av.VideoFrame.from_ndarray(pixels, format="rgb24")):
                        container.mux(packet)
                for packet in stream.encode():
                    container.mux(packet)
            paths.append(str(path))

        batch = pa.RecordBatch.from_pydict({"path": paths})
        with observe_model_calls(32) as (sizes, setups):
            outputs = list(module.embed_frames(iter([batch])))
            assert sizes == [32, 32, 1, 32, 1] and len(setups) == 1, sizes
        assert [output.num_rows for output in outputs] == [32, 32, 1, 32, 1]
        rows = [row for output in outputs for row in output.to_pylist()]
        expected = {("long.mp4", i) for i in range(65)} | {("tail.mp4", i) for i in range(33)}
        assert {(row["video"], row["second"]) for row in rows} == expected and len(rows) == 98

        module.FRAMES_PER_CHUNK = 1
        try:
            reference = [row for output in module.embed_frames(iter([batch])) for row in output.to_pylist()]
        finally:
            module.FRAMES_PER_CHUNK = 32

        def identified(values):
            return [{**row, "frame_id": (row["video"], row["second"])} for row in values]

        compare_rows(identified(reference), identified(rows), "frame_id")
        function = module.embed_frames

        def bounded(batches):
            for output in function(batches):
                assert 0 < output.num_rows <= 32
                yield output

        frames = spark.createDataFrame([(path,) for path in paths], "path string").repartition(2)
        result = [r.asDict() for r in frames.mapInArrow(bounded, module.FRAME_COLUMNS).collect()]
        compare_rows(identified(reference), identified(result), "frame_id")
    return {"engine": engine, "videos": 2, "frames": 98, "model_batches": [32, 32, 1, 32, 1]}


def pdf_check(spark, engine):
    from fpdf import FPDF

    module = load_example("multimodal/pdf-search")
    with tempfile.TemporaryDirectory(prefix="pdf-chunks-") as directory:
        path = Path(directory) / "long.pdf"
        pdf = FPDF()
        pdf.set_auto_page_break(False)
        pdf.set_font("Helvetica", size=8)
        expected = []
        counts = [0, 1, 15, 16, 45, 59, 60, 61, 75, 105, 106] + [151] * 22
        for page, count in enumerate(counts, start=1):
            pdf.add_page()
            words = [f"p{page}word{i}" for i in range(count)]
            if words:
                pdf.multi_cell(0, 4, " ".join(words))
                start, chunk = 0, 0
                while True:
                    expected.append(
                        {
                            "doc": "long",
                            "page": page,
                            "chunk": chunk,
                            "text": " ".join(words[start : start + 60]),
                        }
                    )
                    if start + 60 >= count:
                        break
                    start, chunk = start + 45, chunk + 1
        # An image-only page has no extractable text; it must not create an empty chunk.
        pdf.add_page()
        from inference_checks import ROOT

        photo = next((ROOT / "multimodal/classify-images/images").glob("*.jpg"))
        pdf.image(str(photo), x=10, y=10, w=30)
        pdf.output(str(path))
        tail = Path(directory) / "tail.pdf"
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Helvetica", size=8)
        pdf.cell(text="final three words")
        pdf.output(str(tail))
        expected.append({"doc": "tail", "page": 1, "chunk": 0, "text": "final three words"})
        batch = pa.RecordBatch.from_pydict({"path": [str(path), str(tail)]})
        outputs = list(module.extract_chunks(iter([batch.slice(0, 0), batch])))
        assert all(0 < output.num_rows <= 32 for output in outputs)
        assert outputs[0].num_rows == 32 and outputs[-1].num_rows == 1
        key = lambda row: (row["doc"], row["page"], row["chunk"])
        actual = [row for output in outputs for row in output.to_pylist()]
        assert sorted(actual, key=key) == sorted(expected, key=key)
        assert len({key(row) for row in actual}) == len(expected)

        function = module.extract_chunks

        def bounded(batches):
            for output in function(batches):
                assert 0 < output.num_rows <= 32
                yield output

        paths = spark.createDataFrame([(str(path),), (str(tail),)], "path string").repartition(2)
        rows = [
            r.asDict()
            for r in paths.mapInArrow(bounded, "doc string, page int, chunk int, text string").collect()
        ]
        assert sorted(rows, key=key) == sorted(expected, key=key)
    return {"engine": engine, "documents": 2, "pages": 35, "chunks": len(expected), "max_batch_rows": 32}


def check(spark, engine):
    return {"video": video_check, "pdf": pdf_check}[os.environ["SAIL_EXAMPLES_BOUNDARY_CASE"]](spark, engine)
