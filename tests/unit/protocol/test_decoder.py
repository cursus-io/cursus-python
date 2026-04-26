import json

import pytest

from cursus.errors import ProtocolError
from cursus.protocol.decoder import decode_ack, decode_batch
from cursus.protocol.encoder import encode_batch
from cursus.types import Message


def test_decode_batch_roundtrip():
    original = [
        Message(offset=0, seq_num=1, payload="hello", producer_id="p1", key="k1", epoch=100),
        Message(offset=1, seq_num=2, payload="world", producer_id="p1", key="k2", epoch=101),
    ]
    data = encode_batch("my-topic", 3, "1", True, original)
    messages, topic, partition = decode_batch(data)

    assert topic == "my-topic"
    assert partition == 3
    assert len(messages) == 2
    assert messages[0].payload == "hello"
    assert messages[0].key == "k1"
    assert messages[0].epoch == 100
    assert messages[1].payload == "world"
    assert messages[1].seq_num == 2


def test_decode_batch_event_sourcing_fields():
    original = [
        Message(
            offset=10,
            seq_num=5,
            payload="data",
            producer_id="p",
            event_type="Created",
            schema_version=2,
            aggregate_version=7,
            metadata='{"x":1}',
        ),
    ]
    data = encode_batch("es-topic", 0, "1", False, original)
    messages, _, _ = decode_batch(data)

    assert messages[0].event_type == "Created"
    assert messages[0].schema_version == 2
    assert messages[0].aggregate_version == 7
    assert messages[0].metadata == '{"x":1}'


def test_decode_batch_empty():
    data = encode_batch("t", 0, "1", False, [])
    messages, topic, partition = decode_batch(data)
    assert messages == []
    assert topic == "t"


def test_decode_batch_invalid_magic():
    bad_data = b"\x00\x00rest"
    with pytest.raises(ProtocolError, match="invalid magic"):
        decode_batch(bad_data)


def test_decode_ack_ok():
    raw = json.dumps(
        {
            "status": "OK",
            "last_offset": 100,
            "producer_epoch": 1,
            "producer_id": "p1",
            "seq_start": 1,
            "seq_end": 10,
            "leader": "localhost:9000",
            "error": "",
        }
    ).encode()
    ack = decode_ack(raw)
    assert ack.status == "OK"
    assert ack.last_offset == 100
    assert ack.leader == "localhost:9000"


def test_decode_ack_error():
    raw = json.dumps(
        {
            "status": "ERROR",
            "last_offset": 0,
            "producer_epoch": 0,
            "producer_id": "",
            "seq_start": 0,
            "seq_end": 0,
            "error": "topic not found",
        }
    ).encode()
    ack = decode_ack(raw)
    assert ack.status == "ERROR"
    assert ack.error == "topic not found"
