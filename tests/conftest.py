"""Show which engine versions a test run uses, so an override cannot pass unnoticed.

Every local Spark JVM the tests start binds to loopback. By default Spark binds to the address the
machine's hostname resolves to; on a host where that is a VPN address, the executor cannot fetch the
classes Spark Connect generates, and every Spark run fails with a RemoteClassLoaderError.
"""

import os

from harness import SAIL_DEPS_OVERRIDE, engine_deps

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def pytest_report_header(config):
    sail = engine_deps("sail")
    note = f" (from {SAIL_DEPS_OVERRIDE})" if os.environ.get(SAIL_DEPS_OVERRIDE) else " (pinned)"
    return [f"sail engine: {', '.join(sail)}{note}", f"spark engine: {', '.join(engine_deps('spark'))}"]
