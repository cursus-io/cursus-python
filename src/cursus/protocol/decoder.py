import json
import struct

from cursus.errors import ProtocolError
from cursus.types import AckResponse, Message, OffsetRange, StreamControl

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
        return int(struct.unpack(">B", self.read(1))[0])

    def read_uint16(self) -> int:
        return int(struct.unpack(">H", self.read(2))[0])

    def read_int32(self) -> int:
        return int(struct.unpack(">i", self.read(4))[0])

    def read_uint32(self) -> int:
        return int(struct.unpack(">I", self.read(4))[0])

    def read_int64(self) -> int:
        return int(struct.unpack(">q", self.read(8))[0])

    def read_uint64(self) -> int:
        return int(struct.unpack(">Q", self.read(8))[0])

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
                partition=partition,
            )
        )

    return messages, topic, partition


def decode_ok_fields(response: str) -> dict[str, str]:
    resp = response.strip()
    if not resp.startswith("OK"):
        return {}
    return _decode_fields(resp.split()[1:])


def _decode_fields(parts: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in parts:
        key, sep, value = part.partition("=")
        if sep:
            fields[key] = value
    return fields


def decode_error_fields(response: str) -> dict[str, str]:
    resp = response.strip()
    if not resp.startswith("ERROR:"):
        return {}
    return _decode_fields(resp.split()[1:])


def decode_offset_response(response: str) -> int:
    resp = response.strip()
    if resp.startswith("ERROR:"):
        raise ValueError(f"broker error: {resp}")

    fields = decode_ok_fields(resp)
    if not fields:
        raise ValueError(f"unexpected offset response: {resp}")
    if "offset" not in fields:
        raise ValueError(f"missing offset in response: {resp}")
    return int(fields["offset"])


def is_offset_regression(response: str) -> bool:
    return response.strip().startswith("ERROR: offset_regression")


def is_coordinator_failure(response: str) -> bool:
    resp = response.strip()
    return any(
        token in resp
        for token in (
            "GEN_MISMATCH",
            "NOT_OWNER",
            "member_not_found",
            "group_not_found",
            "NOT_COORDINATOR",
        )
    )


def is_stale_producer_epoch(response: str) -> bool:
    return "stale_producer_epoch" in response


def is_offset_out_of_range(response: str) -> bool:
    return response.strip().startswith("ERROR: OFFSET_OUT_OF_RANGE")


def decode_offset_out_of_range(response: str) -> OffsetRange:
    fields = decode_error_fields(response)
    missing = {"requested", "earliest", "latest"} - fields.keys()
    if missing:
        raise ValueError(f"missing offset range fields {sorted(missing)} in response: {response}")
    return OffsetRange(
        requested=int(fields["requested"]),
        earliest=int(fields["earliest"]),
        latest=int(fields["latest"]),
    )


def is_stream_control_frame(data: bytes) -> bool:
    if len(data) == 0:
        return False
    try:
        return data.decode("utf-8").strip().startswith("STREAM_CONTROL")
    except UnicodeDecodeError:
        return False


def decode_stream_control(data: bytes | str) -> StreamControl:
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    resp = text.strip()
    if not resp.startswith("STREAM_CONTROL"):
        raise ValueError(f"unexpected stream control frame: {resp}")
    fields = _decode_fields(resp.split()[1:])
    return StreamControl(
        type=fields.get("type", ""),
        reason=fields.get("reason", ""),
        offset=int(fields["offset"]) if "offset" in fields else None,
        requested=int(fields["requested"]) if "requested" in fields else None,
        earliest=int(fields["earliest"]) if "earliest" in fields else None,
        latest=int(fields["latest"]) if "latest" in fields else None,
    )


def decode_version_response(response: str) -> int:
    resp = response.strip()
    if resp.startswith("ERROR:"):
        raise ValueError(f"broker error: {resp}")

    fields = decode_ok_fields(resp)
    if not fields:
        raise ValueError(f"unexpected version response: {resp}")
    if "version" not in fields:
        raise ValueError(f"missing version in response: {resp}")
    return int(fields["version"])


def decode_snapshot_response(response: str) -> str | None:
    resp = response.strip()
    if resp.startswith("ERROR:"):
        raise ValueError(f"broker error: {resp}")
    if resp == "OK snapshot=null":
        return None
    if resp.startswith("OK snapshot="):
        return resp.removeprefix("OK snapshot=")
    raise ValueError(f"unexpected snapshot response: {resp}")


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
