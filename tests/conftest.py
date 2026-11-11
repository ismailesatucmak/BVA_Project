import pytest


@pytest.fixture(scope="session")
def spark():
    """Testler için tek bir yerel SparkSession (açılışı ~10 sn, oturum boyunca paylaşılır)."""
    from src.spark_session import get_spark

    session = get_spark("tests", driver_memory="1g", cores="1")
    yield session
    session.stop()
