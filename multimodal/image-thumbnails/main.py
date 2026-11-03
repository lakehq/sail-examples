# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
#   "pillow==13.0.0",
# ]
# ///
"""Make thumbnails for a folder of images: one row per file, small results out.

The pattern: list the files into a table of paths, open each file inside `mapInArrow`, write
the thumbnail, and return only metadata. The image bytes never travel between stages.
"""

import atexit
import random
import shutil
import tempfile
import zlib
from pathlib import Path

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

THUMBNAIL_SIZE = 64
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}
SIZES = [(320, 240), (640, 480), (800, 600), (1024, 768), (1280, 960), (500, 500)]

# The declared schema and the Arrow schema of every returned batch must match exactly:
# Apache Spark rejects an int64 column declared as `int`.
COLUMNS = [
    ("name", "string", pa.string()),
    ("format", "string", pa.string()),
    ("width", "int", pa.int32()),
    ("height", "int", pa.int32()),
    ("thumb_width", "int", pa.int32()),
    ("thumb_height", "int", pa.int32()),
    ("mean_r", "int", pa.int32()),
    ("mean_g", "int", pa.int32()),
    ("mean_b", "int", pa.int32()),
    ("thumb_crc32", "long", pa.int64()),
]
SCHEMA = ", ".join(f"{name} {sql}" for name, sql, _ in COLUMNS)
ARROW_SCHEMA = pa.schema([(name, arrow) for name, _, arrow in COLUMNS])


def temp_dir(prefix: str) -> str:
    """A temporary directory that is removed when the process exits."""
    path = tempfile.mkdtemp(prefix=prefix)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def make_images(directory: Path, count: int = 24) -> None:
    """Draw `count` deterministic test images, half PNG and half JPEG."""
    from PIL import Image, ImageDraw

    for i in range(count):
        rng = random.Random(i)
        width, height = SIZES[i % len(SIZES)]
        image = Image.new("RGB", (width, height), tuple(rng.randrange(256) for _ in range(3)))
        draw = ImageDraw.Draw(image)
        for _ in range(8):
            x0, y0 = rng.randrange(width), rng.randrange(height)
            x1, y1 = min(width, x0 + rng.randrange(20, width)), min(height, y0 + rng.randrange(20, height))
            color = tuple(rng.randrange(256) for _ in range(3))
            (draw.ellipse if rng.random() < 0.5 else draw.rectangle)([x0, y0, x1, y1], fill=color)
        if i % 2:
            image.save(directory / f"image-{i:02d}.jpg", quality=90)
        else:
            image.save(directory / f"image-{i:02d}.png")


def make_thumbnailer(output_dir: str):
    """Build the mapInArrow function; it writes thumbnails and returns one metadata row per file."""

    def thumbnails(batches):
        from PIL import Image, ImageOps, ImageStat

        for batch in batches:
            rows = {name: [] for name in ARROW_SCHEMA.names}
            for path in batch.column("path").to_pylist():
                with Image.open(path) as image:
                    rows["format"].append(image.format)
                    rows["width"].append(image.width)
                    rows["height"].append(image.height)
                    # Apply the camera's orientation tag, so photos are not stored sideways.
                    thumb = ImageOps.exif_transpose(image).convert("RGB")
                thumb.thumbnail((THUMBNAIL_SIZE, THUMBNAIL_SIZE))
                # Keep the extension in the name: a.jpg and a.png must not overwrite each other.
                thumb.save(Path(output_dir) / f"{Path(path).name}.png")
                mean = [round(c) for c in ImageStat.Stat(thumb).mean]
                rows["name"].append(Path(path).name)
                rows["thumb_width"].append(thumb.width)
                rows["thumb_height"].append(thumb.height)
                rows["mean_r"].append(mean[0])
                rows["mean_g"].append(mean[1])
                rows["mean_b"].append(mean[2])
                rows["thumb_crc32"].append(zlib.crc32(thumb.tobytes()))
            yield pa.RecordBatch.from_pydict(rows, schema=ARROW_SCHEMA)

    return thumbnails


def run(spark: SparkSession) -> dict[str, DataFrame]:
    images = Path(temp_dir("images-"))
    make_images(images)
    output = temp_dir("thumbnails-")

    # A table of paths, one row per file. Each task opens only the files in its own rows.
    files = sorted(str(p) for p in images.iterdir() if p.suffix.lower() in IMAGE_SUFFIXES)
    paths = spark.createDataFrame([(p,) for p in files], "path string").repartition(4)
    metadata = paths.mapInArrow(make_thumbnailer(output), SCHEMA)

    # Save the metadata once: rereading it below must not decode the images again.
    metadata_path = temp_dir("metadata-")
    metadata.write.mode("overwrite").parquet(metadata_path)
    saved = spark.read.parquet(metadata_path)

    summary = saved.groupBy("format").agg(
        F.count("*").alias("images"),
        F.sum(F.col("width") * F.col("height")).alias("source_pixels"),
        F.max("thumb_width").alias("max_thumb_width"),
        F.max("thumb_height").alias("max_thumb_height"),
    )
    return {"thumbnails": saved.orderBy("name"), "summary": summary.orderBy("format")}


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    results = run(spark)
    results["summary"].show()
    results["thumbnails"].select("name", "width", "height", "thumb_width", "thumb_height").show(5)
    spark.stop()
    server.stop()
