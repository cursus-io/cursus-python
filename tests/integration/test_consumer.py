import threading
import uuid

import pytest
from conftest import provision_topic

from cursus import Acks, Consumer, ConsumerConfig, ConsumerMode, Producer, ProducerConfig
from cursus.errors import ConnectionError


@pytest.fixture
def topic(broker_addr):
    name = f"test-{uuid.uuid4().hex[:8]}"
    provision_topic(broker_addr, name, partitions=1)
    return name


def test_consumer_joins_group(broker_addr, topic):
    """Consumer can join a group on an existing topic."""
    pconfig = ProducerConfig(
        brokers=[broker_addr],
        topic=topic,
        partitions=1,
        acks=Acks.ONE,
        batch_size=1,
        linger_ms=0,
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


@pytest.mark.parametrize("_iteration", range(3))
@pytest.mark.parametrize("mode", [ConsumerMode.POLLING, ConsumerMode.STREAMING])
def test_publish_then_consume_end_to_end(broker_addr, topic, _iteration, mode):
    with Producer(
        ProducerConfig(
            brokers=[broker_addr],
            topic=topic,
            partitions=1,
            acks=Acks.ONE,
            batch_size=3,
            linger_ms=0,
        )
    ) as producer:
        for value in ("wire-v2-1", "wire-v2-2", "wire-v2-3"):
            producer.send(value)
        producer.flush()

    received = []
    complete = threading.Event()
    consumer = Consumer(
        ConsumerConfig(
            brokers=[broker_addr],
            topic=topic,
            group_id=f"grp-{uuid.uuid4().hex[:6]}",
            mode=mode,
            immediate_commit=True,
        )
    )

    def handle(message):
        received.append(message.payload)
        if len(received) >= 3:
            complete.set()

    worker = threading.Thread(target=consumer.start, args=(handle,), daemon=True)
    worker.start()
    try:
        assert complete.wait(timeout=15), f"timed out waiting for messages: {received}"
    finally:
        consumer.close()
        worker.join(timeout=5)

    assert received[:3] == ["wire-v2-1", "wire-v2-2", "wire-v2-3"]
