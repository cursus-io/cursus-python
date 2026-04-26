import uuid

import pytest

from cursus import Acks, Consumer, ConsumerConfig, ConsumerMode, Producer, ProducerConfig
from cursus.errors import ConnectionError


@pytest.fixture
def topic():
    return f"test-{uuid.uuid4().hex[:8]}"


def test_consumer_joins_group(broker_addr, topic):
    """Consumer can join a group on an existing topic."""
    pconfig = ProducerConfig(
        brokers=[broker_addr], topic=topic, partitions=1,
        acks=Acks.ONE, batch_size=1, linger_ms=0,
    )
    with Producer(pconfig) as p:
        p.send("setup")
        p.flush()

    config = ConsumerConfig(
        brokers=[broker_addr],
        topic=topic,
        group_id=f"grp-{uuid.uuid4().hex[:6]}",
        mode=ConsumerMode.POLLING,
    )
    consumer = Consumer(config)
    consumer._join_and_sync()
    assert consumer._generation > 0
    assert consumer._member_id != ""
    assert len(consumer._assignments) > 0
    consumer.close()


def test_consumer_fails_on_missing_topic(broker_addr):
    """Consumer raises on non-existent topic."""
    config = ConsumerConfig(
        brokers=[broker_addr],
        topic=f"nonexistent-{uuid.uuid4().hex[:8]}",
        group_id="grp",
        mode=ConsumerMode.POLLING,
    )
    consumer = Consumer(config)
    with pytest.raises(ConnectionError, match="join group failed"):
        consumer._join_and_sync()
