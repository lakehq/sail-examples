# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pysail==0.7.3",
#   "pyspark-client==4.1.3",
#   "pyarrow==26.0.0",
#   "pandas==3.0.6",
# ]
# ///
"""Save the input snapshot pair for a sales report and replay it after source changes."""

import argparse
import atexit
import contextlib
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import tempfile
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa

PARAMS = {"start": "2026-01-01", "end": "2026-02-01", "currency": "USD"}
CONFIGS = {"spark.sql.session.timeZone": "UTC"}
SCHEMAS = {
    "orders": "order_id bigint, product_id bigint, quantity bigint, unit_price_minor bigint, ordered_on string, currency string",
    "products": "product_id bigint, category string",
}
RESULT_SCHEMA = "category string, orders bigint, units bigint, sales_minor bigint"
RESULT_ARROW = pa.schema(
    [("category", pa.string()), ("orders", pa.int64()), ("units", pa.int64()), ("sales_minor", pa.int64())]
)
QUERY = """
SELECT p.category, COUNT(DISTINCT o.order_id) AS orders,
       SUM(o.quantity) AS units,
       SUM(o.quantity * o.unit_price_minor) AS sales_minor
FROM {orders_view} o JOIN {products_view} p ON o.product_id = p.product_id
WHERE o.ordered_on >= '{start}' AND o.ordered_on < '{end}' AND o.currency = '{currency}'
GROUP BY p.category ORDER BY p.category
"""


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def file_digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def temp_dir():
    path = Path(tempfile.mkdtemp(prefix="iceberg-report-"))
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    return path


def copies_count(value):
    if type(value) is not int or not 1 <= value <= 5000:
        raise ValueError("copies must be an integer from 1 to 5000")
    return value


def fixture_orders(copies=1):
    copies_count(copies)
    # Every copy has independent order IDs, including an order outside the report window.
    return [
        (copy * 10 + identity, product, quantity, price, day, "USD")
        for copy in range(copies)
        for identity, product, quantity, price, day in [
            (1, 1, 2, 1500, "2026-01-03"),
            (2, 2, 1, 2500, "2026-01-04"),
            (3, 3, 3, 800, "2026-01-08"),
            (4, 1, 1, 1500, "2026-02-01"),
        ]
    ]


def expected_rows(copies=1, changed=False):
    """Literal fixture expectations, separate from the SQL and its results."""
    copies_count(copies)
    values = (
        [("Components", 1, 3, 4500), ("Hardware", 2, 3, 7500)]
        if changed
        else [("Accessories", 1, 3, 2400), ("Components", 2, 3, 5500)]
    )
    return [
        {
            "category": category,
            "orders": orders * copies,
            "units": units * copies,
            "sales_minor": sales * copies,
        }
        for category, orders, units, sales in values
    ]


def validate_result_rows(rows):
    if not isinstance(rows, list):
        raise TypeError("report rows must be a list")
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"category", "orders", "units", "sales_minor"}:
            raise ValueError("report fields differ")
        if not isinstance(row["category"], str) or not row["category"]:
            raise ValueError("report category must be nonempty")
        if any(type(row[key]) is not int or row[key] < 0 for key in ["orders", "units", "sales_minor"]):
            raise ValueError("report totals must be nonnegative integers")
    if [row["category"] for row in rows] != sorted({row["category"] for row in rows}):
        raise ValueError("report categories must be unique and ordered")
    return rows


def canonical_location(value):
    if not isinstance(value, str):
        raise TypeError("table location must be a local path")
    parsed = urlparse(value)
    if parsed.scheme not in {"", "file"} or parsed.netloc not in {"", "localhost"}:
        raise ValueError("only local filesystem tables are supported")
    return str(Path(unquote(parsed.path) if parsed.scheme else value).resolve())


@contextlib.contextmanager
def state_lock(state):
    import fcntl

    state = Path(state).resolve()
    if not state.is_dir():
        raise ValueError("state directory does not exist")
    with (state / ".lock").open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("state is busy; use one process at a time") from error
        try:
            yield state
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x") as out:
            json.dump(value, out, indent=2, sort_keys=True)
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def metadata_at(table, name):
    """Read one exact local metadata file, never infer a catalog head from a directory listing."""
    table = Path(table).resolve()
    if not isinstance(name, str) or not re.fullmatch(r"v(?:0|[1-9][0-9]{0,8})\.metadata\.json", name):
        raise ValueError("unsupported local metadata filename")
    path = table / "metadata" / name
    if not path.is_file() or path.resolve().parent != (table / "metadata").resolve():
        raise ValueError("required metadata file is missing")
    raw = path.read_bytes()
    metadata = json.loads(raw)
    if (
        not isinstance(metadata, dict)
        or type(metadata.get("format-version")) is not int
        or metadata["format-version"] != 2
    ):
        raise ValueError("this example requires Iceberg format version 2")
    try:
        identifier = str(uuid.UUID(metadata["table-uuid"]))
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise ValueError("invalid Iceberg table UUID") from error
    if canonical_location(metadata.get("location")) != str(table):
        raise ValueError("metadata table location differs")
    if not isinstance(metadata.get("snapshots"), list) or not isinstance(metadata.get("schemas"), list):
        raise TypeError("metadata history or schemas are missing")
    snapshots = metadata["snapshots"]
    if any(not isinstance(item, dict) for item in snapshots + metadata["schemas"]):
        raise ValueError("metadata history and schemas must contain objects")
    snapshot_ids = [item.get("snapshot-id") for item in snapshots]
    if any(type(value) is not int or value <= 0 for value in snapshot_ids) or len(set(snapshot_ids)) != len(
        snapshot_ids
    ):
        raise ValueError("invalid or duplicate snapshot IDs")
    return {"metadata": metadata, "name": name, "sha256": hashlib.sha256(raw).hexdigest(), "uuid": identifier}


def current_metadata(table):
    """This demo's local writer uses a numeric version hint and exact vN metadata files."""
    hint = Path(table) / "metadata" / "version-hint.text"
    try:
        before = hint.read_bytes()
    except FileNotFoundError as error:
        raise ValueError("local version hint is missing; no current-table fallback") from error
    if len(before) > 32 or not re.fullmatch(rb"(?:0|[1-9][0-9]{0,8})\s*", before):
        raise ValueError("unsupported local version hint")
    current = metadata_at(table, f"v{int(before.strip())}.metadata.json")
    if hint.read_bytes() != before:
        raise ValueError("table metadata changed while resolving the local hint")
    return current


def selected_schema(metadata):
    schema_id = metadata.get("current-schema-id")
    if type(schema_id) is not int or schema_id < 0:
        raise ValueError("invalid current schema ID")
    matches = [
        schema
        for schema in metadata["schemas"]
        if type(schema.get("schema-id")) is int and schema["schema-id"] == schema_id
    ]
    if len(matches) != 1:
        raise ValueError("selected table schema is missing or ambiguous")
    fields = matches[0].get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("selected schema has no fields")
    for field in fields:
        if (
            not isinstance(field, dict)
            or type(field.get("id")) is not int
            or field["id"] <= 0
            or not isinstance(field.get("name"), str)
            or not field["name"]
            or not isinstance(field.get("type"), str)
            or field["type"] not in {"long", "string"}
            or type(field.get("required")) is not bool
        ):
            raise ValueError("unsupported selected schema field")
    if len({field["id"] for field in fields}) != len(fields) or len(
        {field["name"] for field in fields}
    ) != len(fields):
        raise ValueError("duplicate selected schema field")
    return matches[0]


def current_snapshot(metadata):
    selected = metadata.get("current-snapshot-id")
    if type(selected) is not int or selected <= 0:
        raise ValueError("a populated snapshot is required")
    matches = [snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] == selected]
    if len(matches) != 1:
        raise ValueError("current snapshot is missing from table history")
    return matches[0]


def describe_table(table):
    current = current_metadata(table)
    metadata = current["metadata"]
    return {
        "path": str(Path(table).resolve()),
        "uuid": current["uuid"],
        "snapshot_id": current_snapshot(metadata)["snapshot-id"],
        "metadata_file": current["name"],
        "metadata_sha256": current["sha256"],
        "schema": selected_schema(metadata),
    }


def runtime_versions(spark):
    return {
        "python": platform.python_version(),
        "pysail": importlib.metadata.version("pysail"),
        "pyspark-client": importlib.metadata.version("pyspark-client"),
        "pyarrow": pa.__version__,
        "server": spark.sql("SELECT version()").first()[0],
    }


def configure(spark):
    for name, value in CONFIGS.items():
        spark.conf.set(name, value)
    return {name: spark.conf.get(name) for name in CONFIGS}


def rows_at(spark, tables, params):
    from pyspark.sql import functions as F

    heads = {name: current_metadata(table["path"]) for name, table in tables.items()}
    if any(heads[name]["uuid"] != table["uuid"] for name, table in tables.items()):
        raise ValueError("table identity differs before report execution")
    frames = {
        name: spark.read.format("iceberg")
        .option("snapshot-id", str(table["snapshot_id"]))
        .load(table["path"])
        for name, table in tables.items()
    }
    orders, products = frames["orders"], frames["products"]
    for frame, identity in [(orders, "order_id"), (products, "product_id")]:
        if frame.where(F.col(identity).isNull()).take(1) or frame.groupBy(identity).count().where(
            "count <> 1"
        ).take(1):
            raise ValueError("report input keys must be nonnull and unique")
    if orders.where(
        "product_id IS NULL OR quantity IS NULL OR quantity <= 0 OR unit_price_minor IS NULL OR unit_price_minor < 0 OR ordered_on IS NULL OR currency IS NULL"
    ).take(1):
        raise ValueError("invalid order fields")
    if products.where("category IS NULL OR trim(category) = ''").take(1):
        raise ValueError("invalid product category")
    if orders.join(products, "product_id", "left_anti").take(1):
        raise ValueError("an order has no product lookup")
    suffix = uuid.uuid4().hex
    names = {name: f"report_{name}_{suffix}" for name in frames}
    try:
        for name, frame in frames.items():
            frame.createOrReplaceTempView(names[name])
        query = QUERY.format(orders_view=names["orders"], products_view=names["products"], **params)
        rows = [row.asDict() for row in spark.sql(query).collect()]
    finally:
        for name in names.values():
            spark.catalog.dropTempView(name)
    for name, table in tables.items():
        after = current_metadata(table["path"])
        if after["uuid"] != table["uuid"] or after["sha256"] != heads[name]["sha256"]:
            raise ValueError("table identity or metadata changed during report execution")
    return validate_result_rows(rows)


def validate_manifest(state, manifest, runtime, configs):
    state = Path(state).resolve()
    required = {
        "format_version",
        "state_path",
        "copies",
        "params",
        "configs",
        "runtime",
        "query_sha256",
        "code_sha256",
        "tables",
        "result_sha256",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != required
        or type(manifest["format_version"]) is not int
        or manifest["format_version"] != 1
    ):
        raise ValueError("unsupported report manifest")
    copies_count(manifest["copies"])
    if manifest["state_path"] != str(state):
        raise ValueError("state path differs; this local fixture cannot be relocated")
    if manifest["params"] != PARAMS or manifest["configs"] != configs or configs != CONFIGS:
        raise ValueError("report parameters or configuration differ")
    if manifest["runtime"] != runtime:
        raise ValueError("runtime versions differ")
    if manifest["query_sha256"] != digest(QUERY) or manifest["code_sha256"] != file_digest(__file__):
        raise ValueError("report query or code changed")
    if not isinstance(manifest["tables"], dict) or set(manifest["tables"]) != set(SCHEMAS):
        raise ValueError("manifest must select both input tables")
    for name, saved in manifest["tables"].items():
        expected_fields = {"path", "uuid", "snapshot_id", "metadata_file", "metadata_sha256", "schema"}
        if not isinstance(saved, dict) or set(saved) != expected_fields or saved["path"] != str(state / name):
            raise ValueError("input table path or fields differ")
        if type(saved["snapshot_id"]) is not int or saved["snapshot_id"] <= 0:
            raise ValueError("snapshot ID must be a positive integer")
        original = metadata_at(saved["path"], saved["metadata_file"])
        head = current_metadata(saved["path"])
        if (
            original["sha256"] != saved["metadata_sha256"]
            or original["uuid"] != saved["uuid"]
            or head["uuid"] != saved["uuid"]
        ):
            raise ValueError("input table UUID or saved metadata differs")
        if current_snapshot(original["metadata"])["snapshot-id"] != saved["snapshot_id"]:
            raise ValueError("saved metadata does not select the requested snapshot")
        if digest(selected_schema(original["metadata"])) != digest(saved["schema"]) or digest(
            selected_schema(head["metadata"])
        ) != digest(saved["schema"]):
            raise ValueError("selected table schema differs")
        previous = current_snapshot(original["metadata"])
        current_matches = [
            item for item in head["metadata"]["snapshots"] if item["snapshot-id"] == saved["snapshot_id"]
        ]
        if digest(current_matches) != digest([previous]):
            raise ValueError("required snapshot is missing or changed in current table history")
    result_path = state / "original-report.json"
    if (
        not isinstance(manifest["result_sha256"], str)
        or file_digest(result_path) != manifest["result_sha256"]
    ):
        raise ValueError("saved report checksum differs")
    original_rows = validate_result_rows(json.loads(result_path.read_text()))
    if original_rows != expected_rows(manifest["copies"]):
        raise ValueError("saved report differs from the fixture's independent expectations")
    return original_rows


def load_manifest(state):
    state = Path(state).resolve()
    path = state / "report-manifest.json"
    if not path.is_file() or not (state / "report-manifest.sha256").is_file():
        raise ValueError("prepared report manifest is missing")
    if file_digest(path) != (state / "report-manifest.sha256").read_text().strip():
        raise ValueError("report manifest checksum differs")
    return json.loads(path.read_text())


def create_tables(spark, state, copies):
    for name, rows in {
        "orders": fixture_orders(copies),
        "products": [(1, "Components"), (2, "Components"), (3, "Accessories")],
    }.items():
        spark.createDataFrame(rows, SCHEMAS[name]).write.format("iceberg").save(str(state / name))


def prepare(spark, state, copies=1):
    copies_count(copies)
    state = Path(state).resolve()
    state.mkdir(parents=True, exist_ok=False)
    with state_lock(state):
        configs = configure(spark)
        create_tables(spark, state, copies)
        tables = {name: describe_table(state / name) for name in SCHEMAS}
        rows = rows_at(spark, tables, PARAMS)
        if rows != expected_rows(copies):
            raise ValueError("initial report differs from independent fixture expectations")
        write_json(state / "original-report.json", rows)
        manifest = {
            "format_version": 1,
            "state_path": str(state),
            "copies": copies,
            "params": PARAMS,
            "configs": configs,
            "runtime": runtime_versions(spark),
            "query_sha256": digest(QUERY),
            "code_sha256": file_digest(__file__),
            "tables": tables,
            "result_sha256": file_digest(state / "original-report.json"),
        }
        write_json(state / "report-manifest.json", manifest)
        (state / "report-manifest.sha256").write_text(file_digest(state / "report-manifest.json") + "\n")
        return rows


def replay(spark, state):
    state = Path(state).resolve()
    with state_lock(state):
        manifest = load_manifest(state)
        original = validate_manifest(state, manifest, runtime_versions(spark), configure(spark))
        rows = rows_at(spark, manifest["tables"], manifest["params"])
        if rows != original:
            raise ValueError("replayed logical rows differ from the saved report")
        return rows


def change(spark, state):
    state = Path(state).resolve()
    with state_lock(state):
        manifest = load_manifest(state)
        validate_manifest(state, manifest, runtime_versions(spark), configure(spark))
        if (state / "change-started.json").exists():
            raise ValueError("fixture changes were already started; do not retry mutations")
        write_json(state / "change-started.json", {"before": manifest["tables"]})
        # Fresh processes register only the already existing fixture tables.
        names = {name: f"change_{name}_{uuid.uuid4().hex}" for name in SCHEMAS}
        for name in SCHEMAS:
            path = str(state / name).replace("'", "''")
            spark.sql(f"CREATE TABLE {names[name]} USING iceberg LOCATION '{path}'")
        before = {name: describe_table(state / name) for name in SCHEMAS}
        steps = []

        def record_step(table, operation):
            selected = describe_table(state / table)
            previous = next(
                (step["selected"] for step in reversed(steps) if step["table"] == table), before[table]
            )
            if (
                selected["uuid"] != before[table]["uuid"]
                or selected["snapshot_id"] == previous["snapshot_id"]
            ):
                raise ValueError("source identity changed or snapshot did not advance")
            steps.append({"table": table, "operation": operation, "selected": selected})

        added = [(copy * 10 + 5, 2, 2, 2500, "2026-01-10", "USD") for copy in range(manifest["copies"])]
        spark.createDataFrame(added, SCHEMAS["orders"]).write.format("iceberg").mode("append").save(
            str(state / "orders")
        )
        record_step("orders", "append")
        spark.sql(f"UPDATE {names['orders']} SET quantity = 3 WHERE order_id % 10 = 1")
        record_step("orders", "update")
        spark.sql(f"DELETE FROM {names['orders']} WHERE order_id % 10 = 3")
        record_step("orders", "delete")
        spark.sql(f"UPDATE {names['products']} SET category = 'Hardware' WHERE product_id = 2")
        record_step("products", "update")
        tables = {name: describe_table(state / name) for name in SCHEMAS}
        if any(tables[name]["snapshot_id"] == before[name]["snapshot_id"] for name in tables):
            raise ValueError("source snapshot did not advance")
        rows = rows_at(spark, tables, manifest["params"])
        if rows != expected_rows(manifest["copies"], changed=True):
            raise ValueError("changed report differs from independent fixture expectations")
        write_json(state / "changes.json", {"tables": tables, "steps": steps, "report": rows})
        return rows


def run(spark):
    state = temp_dir() / "sales-report"
    initial = prepare(spark, state)
    current = change(spark, state)
    restored = replay(spark, state)
    return {
        "original": spark.createDataFrame(initial, RESULT_SCHEMA).orderBy("category"),
        "current": spark.createDataFrame(current, RESULT_SCHEMA).orderBy("category"),
        "replayed": spark.createDataFrame(restored, RESULT_SCHEMA).orderBy("category"),
    }


def reference():
    return {
        "original": pa.Table.from_pylist(expected_rows(), schema=RESULT_ARROW),
        "current": pa.Table.from_pylist(expected_rows(changed=True), schema=RESULT_ARROW),
        "replayed": pa.Table.from_pylist(expected_rows(), schema=RESULT_ARROW),
    }


if __name__ == "__main__":
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, help="new or prepared local state directory")
    parser.add_argument("--phase", choices=["prepare", "change", "replay"], help="one retained-state step")
    parser.add_argument(
        "--copies", type=int, default=1, help="prepare with 1 to 5000 independent order copies"
    )
    args = parser.parse_args()
    if (args.state is None) != (args.phase is None):
        parser.error("--state and --phase must be supplied together")
    if args.copies != 1 and args.phase != "prepare":
        parser.error("--copies applies only to --phase prepare")
    server = SparkConnectServer()
    server.start()
    spark = SparkSession.builder.remote(f"sc://localhost:{server.listening_address[1]}").getOrCreate()
    try:
        if args.phase:
            operation = {"prepare": prepare, "change": change, "replay": replay}[args.phase]
            rows = (
                operation(spark, args.state, args.copies)
                if args.phase == "prepare"
                else operation(spark, args.state)
            )
            print(json.dumps({"phase": args.phase, "report": rows}, sort_keys=True))
        else:
            for name, frame in run(spark).items():
                print(name)
                frame.show(truncate=False)
    finally:
        spark.stop()
        server.stop()
