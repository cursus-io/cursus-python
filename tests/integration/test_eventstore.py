import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import provision_topic

from cursus import AsyncEventStore, Event, EventStore
from cursus.errors import ConnectionError


@pytest.fixture
def topic(broker_addr):
    name = f"es-{uuid.uuid4().hex[:8]}"
    provision_topic(broker_addr, name, partitions=2, event_sourcing=True)
    return name


@pytest.fixture
def key():
    return f"agg-{uuid.uuid4().hex[:8]}"


def test_uses_admin_provisioned_topic(broker_addr, topic):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.close()


def test_append_event(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    result = es.append(
        key=key,
        expected_version=1,
        event=Event(type="Created", payload='{"x":1}'),
    )
    assert result.version == 1
    assert result.offset >= 0
    es.close()


def test_append_multiple_versions(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    r1 = es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    assert r1.version == 1

    r2 = es.append(key=key, expected_version=2, event=Event(type="Updated", payload="{}"))
    assert r2.version == 2

    es.close()


@pytest.mark.parametrize("_iteration", range(3))
def test_read_stream_receives_correlated_envelope_and_batch(broker_addr, topic, key, _iteration):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.append(key=key, expected_version=1, event=Event(type="Created", payload='{"x":1}'))
    es.append(key=key, expected_version=2, event=Event(type="Updated", payload='{"x":2}'))

    stream = es.read_stream(key)

    assert [(event.version, event.type, event.payload) for event in stream.events] == [
        (1, "Created", '{"x":1}'),
        (2, "Updated", '{"x":2}'),
    ]
    es.close()


def test_append_version_conflict(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))

    with pytest.raises(ConnectionError, match="version_conflict"):
        es.append(key=key, expected_version=1, event=Event(type="Stale", payload="{}"))

    es.close()


def test_stream_version(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    es.append(key=key, expected_version=2, event=Event(type="Updated", payload="{}"))

    ver = es.stream_version(key)
    assert ver == 2

    es.close()


def test_save_and_read_snapshot(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    es.save_snapshot(key, version=1, payload='{"state":"saved"}')

    snap = es.read_snapshot(key)
    assert snap is not None
    assert snap.version == 1
    assert snap.payload == '{"state":"saved"}'

    es.close()


def test_read_snapshot_not_found(broker_addr, topic):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")

    snap = es.read_snapshot(f"nonexistent-{uuid.uuid4().hex[:8]}")
    assert snap is None

    es.close()


def test_shared_event_store_serializes_concurrent_reads(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)
    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(es.read_stream, key)
            if index % 2 == 0
            else executor.submit(es.stream_version, key)
            for index in range(20)
        ]
        results = [future.result(timeout=5) for future in futures]

    assert all(
        len(result.events) == 1 if index % 2 == 0 else result == 1
        for index, result in enumerate(results)
    )
    es.close()


async def test_shared_async_event_store_serializes_concurrent_reads(broker_addr, topic, key):
    es = AsyncEventStore(addr=broker_addr, topic=topic, producer_id="test")
    await es.create_topic(partitions=2)
    await es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))

    results = await asyncio.gather(
        *[es.read_stream(key) if index % 2 == 0 else es.stream_version(key) for index in range(20)]
    )

    assert all(
        len(result.events) == 1 if index % 2 == 0 else result == 1
        for index, result in enumerate(results)
    )
    await es.close()
