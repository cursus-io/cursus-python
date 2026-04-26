import uuid

import pytest

from cursus import Acks, Producer, ProducerConfig
from cursus.errors import ProducerClosedError


@pytest.fixture
def topic():
    return f"test-{uuid.uuid4().hex[:8]}"


def test_send_single_message(broker_addr, topic):
    config = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=1,
        acks=Acks.ONE,
        batch_size=1,
        linger_ms=0,
    )
    with Producer(config) as p:
        seq = p.send("hello")
        p.flush()
        assert seq == 1
        assert p.unique_ack_count == 1


def test_send_multiple_messages(broker_addr, topic):
    config = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=2,
        acks=Acks.ONE,
        batch_size=10,
        linger_ms=50,
    )
    with Producer(config) as p:
        for i in range(20):
            p.send(f"msg-{i}")
        p.flush()
        assert p.unique_ack_count == 20


def test_send_with_key_deterministic(broker_addr, topic):
    config = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=4,
        acks=Acks.ONE,
        batch_size=1,
        linger_ms=0,
    )
    with Producer(config) as p:
        p.send("a", key="same-key")
        p.send("b", key="same-key")
        p.flush()
        assert p.unique_ack_count == 2


def test_batch_producer_high_volume(broker_addr, topic):
    config = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=4,
        acks=Acks.ONE,
        batch_size=500,
        linger_ms=100,
    )
    n = 5000
    with Producer(config) as p:
        for i in range(n):
            p.send(f"batch-{i}")
        p.flush()
        assert p.unique_ack_count == n


def test_producer_close_rejects_send(broker_addr, topic):
    config = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=1,
        acks=Acks.ONE,
    )
    p = Producer(config)
    p.close()
    with pytest.raises(ProducerClosedError):
        p.send("should fail")
