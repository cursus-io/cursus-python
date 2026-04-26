import json
import struct

from cursus.errors import ProtocolError
from cursus.types import AckResponse, Message

BATCH_MAGIC = 0xBA7C


class _ByteReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def read(self, n: int) -> bytes:
        if self._pos + n > len(self._data):
            raise ProtocolError(f"unexpected end of data at pos {self._pos}, need {n} bytes")
        result = self._data[self._pos : self._pos + n]
        self._pos += n
        return result

    def read_uint8(self) -> int:
        return struct.unpack(">B", self.read(1))[0]

    def read_uint16(self) -> int:
        return struct.unpack(">H", self.read(2))[0]

    def read_int32(self) -> int:
        return struct.unpack(">i", self.read(4))[0]

    def read_uint32(self) -> int:
        return struct.unpack(">I", self.read(4))[0]

    def read_int64(self) -> int:
        return struct.unpack(">q", self.read(8))[0]

    def read_uint64(self) -> int:
        return struct.unpack(">Q", self.read(8))[0]

    def read_bool(self) -> bool:
        return self.read_uint8() != 0

    def read_str(self, length: int) -> str:
        return self.read(length).decode()


def decode_batch(data: bytes) -> tuple[list[Message], str, int]:
    r = _ByteReader(data)

    magic = r.read_uint16()
    if magic != BATCH_MAGIC:
        raise ProtocolError(f"invalid magic number: 0x{magic:04X}")

    topic = r.read_str(r.read_uint16())
    partition = r.read_int32()

    r.read_str(r.read_uint8())
    r.read_bool()

    r.read_uint64()
    r.read_uint64()

    msg_count = r.read_int32()
    messages: list[Message] = []

    for _ in range(msg_count):
        offset = r.read_uint64()
        seq_num = r.read_uint64()
        producer_id = r.read_str(r.read_uint16())
        key = r.read_str(r.read_uint16())
        epoch = r.read_int64()
        payload = r.read_str(r.read_uint32())
        event_type = r.read_str(r.read_uint16())
        schema_version = r.read_uint32()
        aggregate_version = r.read_uint64()
        metadata = r.read_str(r.read_uint16())

        messages.append(
            Message(
                offset=offset,
                seq_num=seq_num,
                payload=payload,
                producer_id=producer_id,
                key=key,
                epoch=epoch,
                event_type=event_type,
                schema_version=schema_version,
                aggregate_version=aggregate_version,
                metadata=metadata,
            )
        )

    return messages, topic, partition


def decode_ack(data: bytes) -> AckResponse:
    obj = json.loads(data)
    return AckResponse(
        status=obj.get("status", ""),
        last_offset=obj.get("last_offset", 0),
        producer_epoch=obj.get("producer_epoch", 0),
        producer_id=obj.get("producer_id", ""),
        seq_start=obj.get("seq_start", 0),
        seq_end=obj.get("seq_end", 0),
        leader=obj.get("leader", ""),
        error=obj.get("error", ""),
    )
