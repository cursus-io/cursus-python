import struct

from cursus.protocol.encoder import encode_batch, encode_message
from cursus.types import Message

BATCH_MAGIC = 0xBA7C


def test_encode_message():
    data = encode_message("my-topic", "hello world")
    topic_len = struct.unpack(">H", data[:2])[0]
    assert topic_len == len("my-topic")
    topic = data[2 : 2 + topic_len].decode()
    assert topic == "my-topic"
    payload = data[2 + topic_len :].decode()
    assert payload == "hello world"


def test_encode_message_empty_topic():
    data = encode_message("", "payload")
    topic_len = struct.unpack(">H", data[:2])[0]
    assert topic_len == 0
    assert data[2:].decode() == "payload"


def test_encode_batch_magic():
    msgs = [Message(offset=0, seq_num=1, payload="test", producer_id="p1")]
    data = encode_batch("topic", 0, "1", True, msgs)
    magic = struct.unpack(">H", data[:2])[0]
    assert magic == BATCH_MAGIC


def test_encode_batch_header():
    msgs = [
        Message(offset=0, seq_num=10, payload="a", producer_id="p1"),
        Message(offset=1, seq_num=11, payload="b", producer_id="p1"),
    ]
    data = encode_batch("t", 2, "1", False, msgs)
    pos = 0

    magic = struct.unpack_from(">H", data, pos)[0]
    pos += 2
    assert magic == BATCH_MAGIC

    topic_len = struct.unpack_from(">H", data, pos)[0]
    pos += 2
    topic = data[pos : pos + topic_len].decode()
    pos += topic_len
    assert topic == "t"

    partition = struct.unpack_from(">i", data, pos)[0]
    pos += 4
    assert partition == 2

    acks_len = struct.unpack_from(">B", data, pos)[0]
    pos += 1
    acks = data[pos : pos + acks_len].decode()
    pos += acks_len
    assert acks == "1"

    idempotent = struct.unpack_from(">?", data, pos)[0]
    pos += 1
    assert idempotent is False

    seq_start = struct.unpack_from(">Q", data, pos)[0]
    pos += 8
    assert seq_start == 10
    seq_end = struct.unpack_from(">Q", data, pos)[0]
    pos += 8
    assert seq_end == 11

    msg_count = struct.unpack_from(">i", data, pos)[0]
    pos += 4
    assert msg_count == 2


def test_encode_batch_empty():
    data = encode_batch("t", 0, "1", False, [])
    pos = 2  # skip magic
    topic_len = struct.unpack_from(">H", data, pos)[0]
    pos += 2
    pos += topic_len  # skip topic
    pos += 4  # partition
    acks_len = struct.unpack_from(">B", data, pos)[0]
    pos += 1
    pos += acks_len  # acks
    pos += 1  # idempotent
    pos += 16  # seq_start + seq_end
    msg_count = struct.unpack_from(">i", data, pos)[0]
    assert msg_count == 0
