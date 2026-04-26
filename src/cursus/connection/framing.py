import struct

from cursus.errors import ProtocolError

MAX_MESSAGE_SIZE = 64 * 1024 * 1024


def encode_frame(data: bytes) -> bytes:
    if len(data) > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"data size {len(data)} exceeds maximum {MAX_MESSAGE_SIZE}")
    return struct.pack(">I", len(data)) + data


def decode_frame_from_buffer(buffer: bytes) -> bytes:
    if len(buffer) < 4:
        raise ProtocolError("buffer too short to contain frame header")
    length = struct.unpack(">I", buffer[:4])[0]
    if length > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"frame size {length} exceeds maximum {MAX_MESSAGE_SIZE}")
    if len(buffer) < 4 + length:
        raise ProtocolError("buffer too short for declared frame length")
    return buffer[4 : 4 + length]
