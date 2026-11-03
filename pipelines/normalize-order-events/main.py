# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Normalize a bounded JSONL snapshot into orders, lines and original rejected records."""

import argparse
import atexit
import base64
import hashlib
import json
import re
import shutil
import tempfile
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

MAX_FILE_BYTES = 1_048_576
MAX_RECORD_BYTES = 65_536
MAX_RECORDS_PER_FILE = 10_000
MAX_INTEGER_DIGITS = 4300
MAX_ITEMS = 100
MAX_QUANTITY = 1_000_000
MAX_PRICE = 1_000_000_000
MAX_FILES = 100
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
SOURCE = "source_id string, source_sha256 string, line_number long"
EVIDENCE = "raw_record_base64 string, raw_json string"
ITEM = "struct<line_id:string,sku:string,quantity:bigint,unit_price_minor:bigint>"
PARSED_SCHEMA = (
    SOURCE
    + ", "
    + EVIDENCE
    + ", order_id string, customer_id string, currency string, "
    + "items array<"
    + ITEM
    + ">, reasons array<string>"
)
ORDER_SCHEMA = (
    SOURCE
    + ", order_id string, customer_id string, currency string, item_count long, gross_minor long, "
    + EVIDENCE
)
LINE_SCHEMA = (
    SOURCE
    + ", order_id string, line_id string, sku string, quantity long, unit_price_minor long, gross_minor long"
)
REJECT_SCHEMA = SOURCE + ", " + EVIDENCE + ", reasons array<string>"


class DuplicateMember(ValueError):
    """JSON object members must be unique, including inside unknown additive fields."""


class NumericLimit(ValueError):
    """A valid JSON numeric token exceeds the supported representation."""


def bounded_integer(token):
    if len(token.lstrip("-")) > MAX_INTEGER_DIGITS:
        raise NumericLimit("numeric_representation_limit")
    try:
        return int(token)
    except ValueError:
        raise NumericLimit("numeric_representation_limit") from None


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateMember("duplicate_json_member")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("nonfinite_json_number")


def valid_id(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def parse_record(raw):
    """Validate a whole physical record before any child can enter a clean table."""
    result = {
        "raw_record_base64": base64.b64encode(raw).decode("ascii"),
        "raw_json": None,
        "order_id": None,
        "customer_id": None,
        "currency": None,
        "items": None,
        "reasons": [],
    }
    if len(raw) > MAX_RECORD_BYTES:
        result["reasons"] = ["record_byte_limit"]
        return result
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        result["reasons"] = ["invalid_utf8"]
        return result
    result["raw_json"] = text
    if not text.strip():
        result["reasons"] = ["empty_record"]
        return result
    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=Decimal,
            parse_int=bounded_integer,
        )
    except DuplicateMember:
        result["reasons"] = ["duplicate_json_member"]
        return result
    except (InvalidOperation, NumericLimit):
        result["reasons"] = ["numeric_representation_limit"]
        return result
    except (ValueError, RecursionError) as error:
        result["reasons"] = [
            "nonfinite_json_number" if str(error) == "nonfinite_json_number" else "malformed_json"
        ]
        return result
    if not isinstance(value, dict):
        result["reasons"] = ["top_level_not_object"]
        return result
    reasons = result["reasons"]
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        reasons.append("unsupported_schema_version")
    for name in ("order_id", "customer_id"):
        if valid_id(value.get(name)):
            result[name] = value[name]
        else:
            reasons.append("invalid_" + name)
    if value.get("currency") not in ("USD", "GBP"):
        reasons.append("unsupported_currency")
    else:
        result["currency"] = value["currency"]
    if "items" not in value:
        reasons.append("missing_items")
    elif value["items"] is None:
        reasons.append("null_items")
    elif not isinstance(value["items"], list):
        reasons.append("items_not_array")
    elif len(value["items"]) > MAX_ITEMS:
        reasons.append("item_count_limit")
    else:
        items, seen = [], set()
        for position, item in enumerate(value["items"]):
            prefix = f"items[{position}]."
            if not isinstance(item, dict):
                reasons.append(prefix + "not_object")
                continue
            for name in ("line_id", "sku"):
                if not valid_id(item.get(name)):
                    reasons.append(prefix + "invalid_" + name)
            line_id = item.get("line_id")
            if valid_id(line_id):
                if line_id in seen:
                    reasons.append(prefix + "duplicate_line_id")
                seen.add(line_id)
            quantity, price = item.get("quantity"), item.get("unit_price_minor")
            if type(quantity) is not int or not 1 <= quantity <= MAX_QUANTITY:
                reasons.append(prefix + "invalid_quantity")
            if type(price) is not int or not 0 <= price <= MAX_PRICE:
                reasons.append(prefix + "invalid_unit_price_minor")
            items.append(
                {name: item.get(name) for name in ("line_id", "sku", "quantity", "unit_price_minor")}
            )
        if not reasons:
            result["items"] = items
    return result


def arrow_schema():
    import pyarrow as pa

    item = pa.struct(
        [
            ("line_id", pa.string()),
            ("sku", pa.string()),
            ("quantity", pa.int64()),
            ("unit_price_minor", pa.int64()),
        ]
    )
    return pa.schema(
        [
            ("source_id", pa.string()),
            ("source_sha256", pa.string()),
            ("line_number", pa.int64()),
            ("raw_record_base64", pa.string()),
            ("raw_json", pa.string()),
            ("order_id", pa.string()),
            ("customer_id", pa.string()),
            ("currency", pa.string()),
            ("items", pa.list_(item)),
            ("reasons", pa.list_(pa.string())),
        ]
    )


def read_sources(batches):
    import pyarrow as pa

    schema = arrow_schema()
    for batch in batches:
        for source in batch.to_pylist():
            with Path(source["path"]).open("rb") as stream:
                content = stream.read(MAX_FILE_BYTES + 1)
            if len(content) > MAX_FILE_BYTES:
                raise ValueError("source_file_byte_limit")
            if hashlib.sha256(content).hexdigest() != source["source_sha256"]:
                raise ValueError("source_file_sha256_mismatch")
            # splitlines would treat embedded Unicode/control separators as extra records.
            records = content.split(b"\n")
            if records[-1] == b"":
                records.pop()  # the final LF terminates its record, not an extra empty record
            if len(records) > MAX_RECORDS_PER_FILE:
                raise ValueError("source_file_record_limit")
            rows = []
            for index, record in enumerate(records, 1):
                raw = record + (b"\n" if index < len(records) or content.endswith(b"\n") else b"")
                rows.append(
                    {
                        "source_id": source["source_id"],
                        "source_sha256": source["source_sha256"],
                        "line_number": index,
                        **parse_record(raw),
                    }
                )
            if rows:
                yield pa.RecordBatch.from_pylist(rows, schema=schema)


def manifest(paths):
    paths = [Path(path) for path in paths]
    if len(paths) > MAX_FILES or len({path.name for path in paths}) != len(paths):
        raise ValueError("at most 100 source files with unique basenames are supported")
    records = []
    for path in paths:
        with path.open("rb") as stream:
            content = stream.read(MAX_FILE_BYTES + 1)
        if len(content) > MAX_FILE_BYTES:
            raise ValueError("source_file_byte_limit")
        record_count = content.count(b"\n") + int(bool(content) and not content.endswith(b"\n"))
        if record_count > MAX_RECORDS_PER_FILE:
            raise ValueError("source_file_record_limit")
        records.append(
            {
                "source_id": path.name,
                "source_sha256": hashlib.sha256(content).hexdigest(),
                "path": str(path.resolve()),
            }
        )
    return records


def normalize(spark, parsed):
    from pyspark.sql import functions as F

    valid = parsed.where(F.size("reasons") == 0)
    counts = valid.groupBy("order_id").count().withColumnRenamed("count", "order_copies")
    checked = valid.join(counts, "order_id", "inner")
    accepted = checked.where("order_copies = 1")
    rejects = parsed.where(F.size("reasons") > 0).selectExpr(
        *[part.strip().split()[0] for part in REJECT_SCHEMA.split(",")]
    )
    repeated = checked.where("order_copies > 1").withColumn("reasons", F.array(F.lit("duplicate_order_id")))
    rejects = rejects.unionByName(repeated.select(*rejects.columns))
    source_columns = ["source_id", "source_sha256", "line_number", "order_id"]
    lines = (
        accepted.select(*source_columns, F.explode("items").alias("item"))
        .select(*source_columns, "item.*")
        .withColumn("gross_minor", F.col("quantity") * F.col("unit_price_minor"))
    )
    totals = lines.groupBy(*source_columns).agg(F.sum("gross_minor").alias("gross_minor"))
    orders = accepted.join(totals, source_columns, "left").withColumn(
        "item_count", F.size("items").cast("long")
    )
    orders = orders.withColumn("gross_minor", F.coalesce("gross_minor", F.lit(0).cast("long")))
    return {
        "orders": orders.selectExpr(*[part.strip().split()[0] for part in ORDER_SCHEMA.split(",")]),
        "lines": lines.selectExpr(*[part.strip().split()[0] for part in LINE_SCHEMA.split(",")]),
        "rejects": rejects,
    }


def temp_dir():
    path = Path(tempfile.mkdtemp(prefix="order-events-"))
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def readback(spark, frame, path, schema):
    import pyarrow.parquet as pq

    frame.write.mode("error").parquet(str(path))
    if not list(path.glob("*.parquet")):
        # Every output is a physically readable typed dataset, including all-reject/empty input.
        path.mkdir(parents=True, exist_ok=True)
        pq.write_table(frame.limit(0).toArrow(), path / "empty.parquet")
    return spark.read.schema(schema).parquet(str(path))


def demo_files(directory, copies=1):
    if type(copies) is not int or not 1 <= copies <= 1000:
        raise ValueError("copies must be an integer from 1 to 1000")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    records = (Path(__file__).parent / "inputs/orders.jsonl").read_bytes().splitlines(keepends=True)
    paths = []
    for group in range((copies + 49) // 50):
        path = directory / f"orders-{group:03d}.jsonl"
        with path.open("wb") as stream:
            for copy in range(group * 50, min(copies, (group + 1) * 50)):
                for raw in records:
                    # Replace only the explicit placeholder in this authored input, retaining malformed records.
                    stream.write(raw.replace(b"COPY", f"{copy:04d}".encode()))
        paths.append(path)
    return paths


def run(spark, paths=None, *, copies=1, output_dir=None):
    root = temp_dir()
    destination = root
    if output_dir is not None:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=False)
    if paths is None:
        paths = demo_files(root / "inputs", copies)
    sources = manifest(paths)
    inputs = spark.createDataFrame(sources, "source_id string, source_sha256 string, path string")
    parsed = inputs.repartition(2).mapInArrow(read_sources, PARSED_SCHEMA)
    parsed = readback(spark, parsed, root / "parsed", PARSED_SCHEMA)
    outputs = normalize(spark, parsed)
    schemas = {"orders": ORDER_SCHEMA, "lines": LINE_SCHEMA, "rejects": REJECT_SCHEMA}
    outputs = {
        name: readback(spark, frame, destination / name, schemas[name]) for name, frame in outputs.items()
    }
    input_keys = Counter(
        tuple(row) for row in parsed.select("source_id", "source_sha256", "line_number").collect()
    )
    output_keys = Counter()
    for name in ("orders", "rejects"):
        output_keys.update(
            tuple(row) for row in outputs[name].select("source_id", "source_sha256", "line_number").collect()
        )
    if input_keys != output_keys or any(count != 1 for count in input_keys.values()):
        raise AssertionError("each physical source record must appear in exactly one order or reject row")
    return {
        name: frame.orderBy("source_id", "line_number", *(["line_id"] if name == "lines" else []))
        for name, frame in outputs.items()
    }


def parse_cli(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--copies", type=int, help="independent copies of the authored snapshot, 1..1000 (default: 1)"
    )
    parser.add_argument(
        "--input", type=Path, action="append", help="read a supplied JSONL file; repeat for more files"
    )
    parser.add_argument("--output-dir", type=Path, help="retain final Parquet datasets in a new directory")
    parser.add_argument("--json", action="store_true", help="print output row counts")
    args = parser.parse_args(argv)
    if args.input is not None and args.copies is not None:
        parser.error("--input and --copies cannot be combined")
    args.copies = 1 if args.copies is None else args.copies
    if not 1 <= args.copies <= 1000:
        parser.error("--copies must be from 1 to 1000")
    return args


if __name__ == "__main__":
    args = parse_cli()
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start()
    spark = None
    try:
        _, port = server.listening_address
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        outputs = run(spark, paths=args.input, copies=args.copies, output_dir=args.output_dir)
        if args.json:
            print(json.dumps({name: frame.count() for name, frame in outputs.items()}, sort_keys=True))
        else:
            for name, frame in outputs.items():
                print(name)
                frame.show(truncate=80)
    finally:
        if spark is not None:
            spark.stop()
        server.stop()
