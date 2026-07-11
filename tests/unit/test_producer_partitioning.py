import asyncio
import json
import threading

import pytest

from cursus.compression.registry import CompressionRegistry
from cursus.errors import ConnectionError
from cursus.producer import Producer, _PartitionBuffer
from cursus.types import Message


def test_fnv1a_32_known_values():
    assert Producer._fnv1a_32(b"") == 0x811C9DC5
    assert Producer._fnv1a_32(b"a") == 0xE40C292C


def test_partition_for_key_deterministic():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=4)
    p = Producer.__new__(Producer)
    p._config = cfg
    part1 = p._partition_for_key("order-123")
    part2 = p._partition_for_key("order-123")
    assert part1 == part2


def test_partition_for_key_within_range():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=8)
    p = Producer.__new__(Producer)
    p._config = cfg
    for key in ["a", "b", "order-1", "order-2", "user-xyz"]:
        part = p._partition_for_key(key)
        assert 0 <= part < 8


def test_partition_for_key_distributes():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=4)
    p = Producer.__new__(Producer)
    p._config = cfg
    partitions_seen = set()
    for i in range(100):
        partitions_seen.add(p._partition_for_key(f"key-{i}"))
    assert len(partitions_seen) > 1


def test_async_producer_uses_same_key_partitioning():
    from cursus.async_producer import AsyncProducer
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=4)
    sync = Producer.__new__(Producer)
    sync._config = cfg
    async_producer = AsyncProducer.__new__(AsyncProducer)
    async_producer._config = cfg

    assert async_producer._partition_for_key("order-123") == sync._partition_for_key("order-123")


def test_async_producer_key_partition_is_deterministic():
    from cursus.async_producer import AsyncProducer
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=8)
    producer = AsyncProducer.__new__(AsyncProducer)
    producer._config = cfg

    assert producer._partition_for_key("order-9") == producer._partition_for_key("order-9")


class _FakeConn:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.written: list[bytes] = []

    def write_frame(self, data: bytes) -> None:
        self.written.append(data)

    def read_frame(self) -> bytes:
        return self.response


def _new_sync_producer_for_send(epoch: int) -> Producer:
    from cursus.config import ProducerConfig

    producer = Producer.__new__(Producer)
    producer._config = ProducerConfig(topic="t", partitions=1, linger_ms=60_000)
    producer._closed = False
    producer._rr = 0
    producer._rr_lock = threading.Lock()
    producer._seq_counters = [0]
    producer._seq_locks = [threading.Lock()]
    producer._buffers = [_PartitionBuffer()]
    producer._producer_id = "py-test"
    producer._producer_epoch = epoch
    return producer


def test_sync_producer_uses_fixed_epoch_for_session_messages():
    producer = _new_sync_producer_for_send(epoch=1234)

    producer.send("a")
    producer.send("b")

    assert [msg.epoch for msg in producer._buffers[0].msgs] == [1234, 1234]
    assert [msg.seq_num for msg in producer._buffers[0].msgs] == [1, 2]


def test_sync_producer_non_terminal_ack_error_is_retryable_failure():
    from cursus.config import ProducerConfig

    producer = Producer.__new__(Producer)
    producer._config = ProducerConfig(topic="t", partitions=1)
    producer._compression = CompressionRegistry()
    producer._ack_lock = threading.Lock()
    producer._unique_ack_count = 0
    response = json.dumps({"status": "ERROR", "error": "temporary broker failure"}).encode()

    with pytest.raises(ConnectionError):
        producer._send_batch(
            _FakeConn(response),
            0,
            [Message(offset=0, seq_num=1, payload="a", producer_id="py-test", epoch=1)],
        )


def test_async_producer_uses_fixed_epoch_for_session_messages():
    from cursus.async_producer import AsyncProducer
    from cursus.config import ProducerConfig

    async def scenario() -> None:
        producer = AsyncProducer.__new__(AsyncProducer)
        producer._config = ProducerConfig(topic="t", partitions=1)
        producer._closed = False
        producer._seq_counters = [0]
        producer._rr = 0
        producer._buffers = [[]]
        producer._events = [asyncio.Event()]
        producer._producer_id = "py-async-test"
        producer._producer_epoch = 5678

        await producer.send("a")
        await producer.send("b")

        assert [msg.epoch for msg in producer._buffers[0]] == [5678, 5678]
        assert [msg.seq_num for msg in producer._buffers[0]] == [1, 2]

    asyncio.run(scenario())
