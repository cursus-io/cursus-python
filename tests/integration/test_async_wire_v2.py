import asyncio
import uuid

import pytest
from conftest import provision_topic

from cursus import (
    Acks,
    AsyncConsumer,
    AsyncEventStore,
    AsyncProducer,
    ConsumerConfig,
    ConsumerMode,
    Event,
    ProducerConfig,
)


@pytest.mark.parametrize("_iteration", range(3))
async def test_async_publish_then_consume_over_wire_v2(broker_addr, _iteration):
    topic = f"async-wire-v2-{uuid.uuid4().hex[:8]}"
    provision_topic(broker_addr, topic, partitions=1, idempotent=True)
    async with AsyncProducer(
        ProducerConfig(
            brokers=[broker_addr],
            topic=topic,
            partitions=1,
            acks=Acks.ALL,
            idempotent=True,
            compression_type="gzip",
            batch_size=3,
            linger_ms=10,
        )
    ) as producer:
        await producer.send("async-wire-v2-1")
        await producer.send("async-wire-v2-2")
        await producer.send("async-wire-v2-3")
        await producer.flush()
        assert producer.unique_ack_count == 3

    async with AsyncConsumer(
        ConsumerConfig(
            brokers=[broker_addr],
            topic=topic,
            group_id=f"async-e2e-{uuid.uuid4().hex[:8]}",
            mode=ConsumerMode.POLLING,
            immediate_commit=True,
        )
    ) as consumer:
        messages = [
            await asyncio.wait_for(anext(consumer), timeout=15),
            await asyncio.wait_for(anext(consumer), timeout=15),
            await asyncio.wait_for(anext(consumer), timeout=15),
        ]

    assert [message.payload for message in messages] == [
        "async-wire-v2-1",
        "async-wire-v2-2",
        "async-wire-v2-3",
    ]


async def test_async_event_store_reads_two_correlated_wire_v2_responses(broker_addr):
    topic = f"async-es-wire-v2-{uuid.uuid4().hex[:8]}"
    key = f"aggregate-{uuid.uuid4().hex[:8]}"
    provision_topic(broker_addr, topic, partitions=1, event_sourcing=True)
    async with AsyncEventStore(broker_addr, topic, "async-e2e") as store:
        await store.append(key, 1, Event(type="Created", payload='{"x":1}'))
        await store.append(key, 2, Event(type="Updated", payload='{"x":2}'))

        stream = await store.read_stream(key)

    assert [(event.version, event.type) for event in stream.events] == [
        (1, "Created"),
        (2, "Updated"),
    ]
