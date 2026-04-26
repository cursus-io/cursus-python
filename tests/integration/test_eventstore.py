import uuid

import pytest

from cursus import Event, EventStore
from cursus.errors import ConnectionError


@pytest.fixture
def topic():
    return f"es-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def key():
    return f"agg-{uuid.uuid4().hex[:8]}"


def test_create_topic(broker_addr, topic):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)
    es.close()


def test_append_event(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)

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
    es.create_topic(partitions=2)

    r1 = es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    assert r1.version == 1

    r2 = es.append(key=key, expected_version=2, event=Event(type="Updated", payload="{}"))
    assert r2.version == 2

    es.close()


def test_append_version_conflict(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))

    with pytest.raises(ConnectionError, match="version_conflict"):
        es.append(key=key, expected_version=1, event=Event(type="Stale", payload="{}"))

    es.close()


def test_stream_version(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    es.append(key=key, expected_version=2, event=Event(type="Updated", payload="{}"))

    ver = es.stream_version(key)
    assert ver == 2

    es.close()


def test_save_and_read_snapshot(broker_addr, topic, key):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)

    es.append(key=key, expected_version=1, event=Event(type="Created", payload="{}"))
    es.save_snapshot(key, version=1, payload='{"state":"saved"}')

    snap = es.read_snapshot(key)
    assert snap is not None
    assert snap.version == 1
    assert snap.payload == '{"state":"saved"}'

    es.close()


def test_read_snapshot_not_found(broker_addr, topic):
    es = EventStore(addr=broker_addr, topic=topic, producer_id="test")
    es.create_topic(partitions=2)

    snap = es.read_snapshot(f"nonexistent-{uuid.uuid4().hex[:8]}")
    assert snap is None

    es.close()
