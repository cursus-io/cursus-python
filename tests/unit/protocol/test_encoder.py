import struct

from cursus.protocol.encoder import encode_batch, encode_message
from cursus.protocol.wire import BATCH_MAGIC, BATCH_VERSION
from cursus.types import Message


def _read_string(data: bytes, position: int) -> tuple[str, int]:
    length = struct.unpack_from(">I", data, position)[0]
    position += 4
    return data[position : position + length].decode(), position + length


def test_encode_message_returns_command_text_for_wire_v2_connection():
    assert encode_message("ignored", "METADATA topic=orders") == b"METADATA topic=orders"


def test_encode_batch_magic_and_version():
    messages = [Message(offset=0, seq_num=1, payload="test", producer_id="p1")]
    data = encode_batch("topic", 0, "1", True, messages)

    assert struct.unpack_from(">I", data, 0)[0] == BATCH_MAGIC
    assert struct.unpack_from(">H", data, 4)[0] == BATCH_VERSION


def test_encode_batch_header():
    messages = [
        Message(offset=0, seq_num=10, payload="a", producer_id="p1"),
        Message(offset=1, seq_num=11, payload="b", producer_id="p1"),
    ]
    data = encode_batch("t", 2, "1", False, messages)
    position = 0

    magic, version, flags = struct.unpack_from(">IHH", data, position)
    position += 8
    assert (magic, version, flags) == (BATCH_MAGIC, BATCH_VERSION, 0)

    topic, position = _read_string(data, position)
    assert topic == "t"
    partition = struct.unpack_from(">i", data, position)[0]
    position += 4
    assert partition == 2
    acks, position = _read_string(data, position)
    assert acks == "1"
    seq_start, seq_end, message_count = struct.unpack_from(">QQI", data, position)
    assert (seq_start, seq_end, message_count) == (10, 11, 2)


def test_encode_batch_empty():
    data = encode_batch("t", 0, "1", False, [])
    position = 8
    _, position = _read_string(data, position)
    position += 4
    _, position = _read_string(data, position)
    seq_start, seq_end, message_count = struct.unpack_from(">QQI", data, position)
    assert (seq_start, seq_end, message_count) == (0, 0, 0)
