FACT = {
    "id": "udf-partitions-are-threads",
    "category": "python-udfs",
    "core": True,
    "text": """In local mode, Sail runs the partitions of a Python UDF at the same time as threads of
        the server process (Apache Spark uses separate Python worker processes). A library that must not be
        called from two threads at once (PDFium through pypdfium2, per its docs) therefore needs one lock for
        the whole process. Keep that lock in `sys.modules`, which every task and every later query in the
        process share. A global of your script will not do: its functions are pickled by value, so each task
        gets its own copy of the globals.""",
    "engines": ["sail", "spark"],
    "order": 2,
}

import pyarrow as pa

SCRIPT_GLOBAL = {"tasks": 0}
COLUMNS = "pid long, thread long, start double, finish double, tasks_seen long, shared_lock long"
SCHEMA = pa.schema(
    [
        ("pid", pa.int64()),
        ("thread", pa.int64()),
        ("start", pa.float64()),
        ("finish", pa.float64()),
        ("tasks_seen", pa.int64()),
        ("shared_lock", pa.int64()),
    ]
)


def report(batches):
    import os
    import sys
    import threading
    import time
    import types

    registry = sys.modules.setdefault("_process_locks", types.ModuleType("_process_locks"))
    lock = registry.__dict__.setdefault("fact", threading.Lock())
    # A task that shared the script global with other tasks would see their counts too.
    SCRIPT_GLOBAL["tasks"] += 1
    start = time.time()
    time.sleep(1.0)  # long enough for the partitions to overlap
    for _ in batches:
        pass
    row = {
        "pid": [os.getpid()],
        "thread": [threading.get_ident()],
        "start": [start],
        "finish": [time.time()],
        "tasks_seen": [SCRIPT_GLOBAL["tasks"]],
        "shared_lock": [id(lock)],
    }
    yield pa.RecordBatch.from_pydict(row, schema=SCHEMA)


def overlapping_pairs(rows) -> int:
    return sum(
        1 for i, a in enumerate(rows) for b in rows[i + 1 :] if a.start < b.finish and b.start < a.finish
    )


def check(spark, engine):
    runs = [spark.range(0, 400, 1, 4).mapInArrow(report, COLUMNS).collect() for _ in range(2)]
    rows = runs[0]
    pids = {r.pid for r in rows}
    assert len(rows) == 4, f"expected 4 partitions, saw {len(rows)}"
    assert overlapping_pairs(rows) >= 1, f"no two partitions ran at the same time on {engine}"
    if engine == "spark":
        assert len(pids) > 1, "Spark ran every partition in one Python process"
        return {"python_processes": len(pids)}

    assert len(pids) == 1, f"Sail ran partitions in {len(pids)} processes"
    assert len({r.thread for r in rows}) > 1, "Sail ran every partition on one thread"
    assert all(r.tasks_seen == 1 for r in rows), (
        f"tasks shared a script global: {[r.tasks_seen for r in rows]}"
    )
    locks = {r.shared_lock for run in runs for r in run}
    assert len(locks) == 1, f"a lock kept in sys.modules was not shared: {len(locks)} objects"
    return {
        "python_processes": 1,
        "threads": len({r.thread for r in rows}),
        "overlapping_pairs": overlapping_pairs(rows),
        "script_global_counts": [r.tasks_seen for r in rows],
        "shared_lock_objects": len(locks),
    }
