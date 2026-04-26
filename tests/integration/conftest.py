import pytest

BROKER_ADDR = "localhost:10000"


@pytest.fixture
def broker_addr():
    return BROKER_ADDR
