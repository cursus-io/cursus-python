import struct

import pytest

from cursus.connection.framing import MAX_MESSAGE_SIZE, decode_frame_from_buffer, encode_frame
from cursus.errors import ProtocolError


def test_encode_frame():
    data = b"hello"
    frame = encode_frame(data)
    length = struct.unpack(">I", frame[:4])[0]
    assert length == 5
    assert frame[4:] == b"hello"


def test_encode_frame_empty():
    frame = encode_frame(b"")
    length = struct.unpack(">I", frame[:4])[0]
    assert length == 0


def test_decode_frame_from_buffer():
    data = b"hello world"
    frame = struct.pack(">I", len(data)) + data
    result = decode_frame_from_buffer(frame)
    assert result == data


def test_decode_frame_roundtrip():
    original = b"roundtrip test data"
    frame = encode_frame(original)
    result = decode_frame_from_buffer(frame)
    assert result == original


def test_encode_frame_rejects_oversized():
    with pytest.raises(ProtocolError, match="exceeds maximum"):
        encode_frame(b"x" * (MAX_MESSAGE_SIZE + 1))
