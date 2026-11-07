"""Show which engine versions a test run uses, so an override cannot pass unnoticed.

Every local Spark JVM the tests start binds to loopback. By default Spark binds to the address the
machine's hostname resolves to; on a host where that is a VPN address, the executor cannot fetch the
classes Spark Connect generates, and every Spark run fails with a RemoteClassLoaderError.
"""

import os

import pytest
from catalog import discover_examples
from harness import SAIL_DEPS_OVERRIDE, engine_deps

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def pytest_addoption(parser):
    parser.addoption("--changed-since", help="Run checks affected by changes since this Git commit")


def pytest_collection_modifyitems(config, items):
    from change_selection import select_item, selection_since

    known = {example.id for example in discover_examples()}
    for item in items:
        owners = item.get_closest_marker("examples")
        if owners and (not owners.args or any(owner not in known for owner in owners.args)):
            raise pytest.UsageError(f"{item.nodeid}: examples marker must name existing example IDs")
    base = config.getoption("--changed-since")
    if not base:
        return
    selection = selection_since(base)
    selected = []
    deselected = []
    for item in items:
        (selected if select_item(item, selection) else deselected).append(item)
    config.hook.pytest_deselected(items=deselected)
    items[:] = selected


def pytest_report_header(config):
    sail = engine_deps("sail")
    note = f" (from {SAIL_DEPS_OVERRIDE})" if os.environ.get(SAIL_DEPS_OVERRIDE) else " (pinned)"
    return [f"sail engine: {', '.join(sail)}{note}", f"spark engine: {', '.join(engine_deps('spark'))}"]
