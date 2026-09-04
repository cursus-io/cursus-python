import os

import pytest

from cursus import AdminClient, AdminConfig, TopicDefinitionPatch

BROKER_ADDR = os.environ.get("CURSUS_TEST_BROKER_ADDR", "localhost:10000")


@pytest.fixture
def broker_addr():
    return BROKER_ADDR


def provision_topic(
    broker_addr: str,
    topic: str,
    *,
    partitions: int = 1,
    idempotent: bool = False,
    event_sourcing: bool = False,
) -> None:
    AdminClient(AdminConfig(brokers=[broker_addr])).create_topic(
        topic,
        TopicDefinitionPatch(
            partitions=partitions,
            idempotent=idempotent,
            event_sourcing=event_sourcing,
        ),
    )
