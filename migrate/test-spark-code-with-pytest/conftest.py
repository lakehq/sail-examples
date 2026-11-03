import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One Sail server and session for the whole test run, started in this process."""
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    yield session
    session.stop()
    server.stop()
