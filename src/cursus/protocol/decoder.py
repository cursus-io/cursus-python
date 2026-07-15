import json
import struct

from cursus.errors import (
    AuthenticationRequiredError,
    AuthorizationDeniedError,
    BrokerError,
    ProducerFencedError,
    ProtocolError,
    ValidationError,
)
from cursus.types import (
    AckResponse,
    Message,
    OffsetRange,
    PartitionOffsetRange,
    ProducerSession,
    StreamControl,
    TransactionStatus,
)

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

        if payload == "__cursus_txn_control_marker__":
            continue

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


def is_ok_response(response: str) -> bool:
    resp = response.strip()
    return resp == "OK" or resp.startswith("OK ")


def is_error_response(response: str) -> bool:
    return response.strip().startswith("ERROR:")


def decode_ok_fields(response: str) -> dict[str, str]:
    resp = response.strip()
    if not is_ok_response(resp):
        return {}
    return _decode_fields(resp.split()[1:])


def _decode_fields(parts: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in parts:
        key, sep, value = part.partition("=")
        if sep:
            fields[key] = value.strip('"')
    return fields


def decode_error_fields(response: str) -> dict[str, str]:
    resp = response.strip()
    if not is_error_response(resp):
        return {}
    return _decode_fields(resp.split()[2:])


def decode_error_code(response: str) -> str:
    resp = response.strip()
    if not is_error_response(resp):
        return ""
    parts = resp.split(maxsplit=2)
    return parts[1] if len(parts) > 1 else ""


def error_from_response(response: str) -> BrokerError:
    code = decode_error_code(response)
    fields = decode_error_fields(response)
    lower = response.lower()
    if code in {"AUTHENTICATION_REQUIRED", "authentication_required"}:
        return AuthenticationRequiredError(code, fields, response)
    if code in {"NOT_AUTHORIZED_FOR_TOPIC", "authorization_denied", "AUTHORIZATION_DENIED"}:
        return AuthorizationDeniedError(code, fields, response)
    if "producer_fenced" in lower or "stale_producer_epoch" in lower:
        return ProducerFencedError(code or "producer_fenced", fields, response)
    if code.startswith("invalid_") or code.startswith("missing_"):
        return ValidationError(code, fields, response)
    return BrokerError(code, fields, response)


def require_ok(response: str, *, operation: str = "command") -> dict[str, str]:
    resp = response.strip()
    if is_error_response(resp):
        raise error_from_response(resp)
    if not is_ok_response(resp):
        raise ProtocolError(f"unexpected {operation} response: {resp}")
    return decode_ok_fields(resp)


def decode_not_coordinator(response: str) -> str | None:
    if decode_error_code(response) != "NOT_COORDINATOR":
        return None
    fields = decode_error_fields(response)
    host = fields.get("host")
    port = fields.get("port")
    if not host or not port:
        return None
    return f"{host}:{port}"


def decode_offset_response(response: str) -> int:
    try:
        fields = require_ok(response, operation="offset")
    except ProtocolError as exc:
        raise ValueError(f"unexpected offset response: {response.strip()}") from exc
    if "offset" not in fields:
        raise ValueError(f"missing offset in response: {response.strip()}")
    return int(fields["offset"])


def decode_list_offsets_response(response: str) -> list[PartitionOffsetRange]:
    fields = require_ok(response, operation="list offsets")
    offsets = fields.get("offsets")
    if offsets is None:
        raise ValueError(f"missing offsets in response: {response.strip()}")

    result: list[PartitionOffsetRange] = []
    for entry in offsets.split(","):
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 5 or not parts[0].startswith("P"):
            raise ValueError(f"malformed partition offset entry: {entry}")
        partition = int(parts[0][1:])
        values: dict[str, int] = {}
        for item in parts[1:]:
            key, sep, value = item.partition("=")
            if not sep:
                raise ValueError(f"malformed partition offset field: {entry}")
            values[key] = int(value)
        missing = {"earliest", "latest", "leo", "hwm"} - values.keys()
        if missing:
            raise ValueError(f"missing offset fields {sorted(missing)} in response: {response}")
        result.append(
            PartitionOffsetRange(
                partition=partition,
                earliest=values["earliest"],
                latest=values["latest"],
                leo=values["leo"],
                hwm=values["hwm"],
            )
        )
    return result


def decode_producer_session(response: str) -> ProducerSession:
    fields = require_ok(response, operation="producer session")
    transactional_id = fields.get("transactional_id", "")
    producer_id = fields.get("producerId") or fields.get("producer_id") or ""
    epoch = fields.get("epoch", "")
    if not transactional_id or not producer_id or not epoch:
        raise ValueError(f"malformed producer session response: {response.strip()}")
    return ProducerSession(transactional_id, producer_id, int(epoch))


def decode_transaction_status(response: str) -> TransactionStatus:
    fields = require_ok(response, operation="transaction status")
    transactional_id = fields.get("transactional_id", "")
    state = fields.get("state", "")
    if not transactional_id or not state:
        raise ValueError(f"malformed transaction status response: {response.strip()}")
    return TransactionStatus(
        transactional_id=transactional_id,
        state=state,
        messages=int(fields.get("messages", 0)),
        offsets=int(fields.get("offsets", 0)),
    )


def is_offset_regression(response: str) -> bool:
    code = decode_error_code(response)
    return code == "offset_regression" or "offset regression" in response.lower()


def is_coordinator_failure(response: str) -> bool:
    return decode_error_code(response) in {
        "GEN_MISMATCH",
        "NOT_OWNER",
        "member_not_found",
        "group_not_found",
        "NOT_COORDINATOR",
    }


def is_terminal_producer_error(response: str) -> bool:
    resp = response.lower()
    return any(
        token in resp
        for token in (
            "producer_fenced",
            "stale_producer_epoch",
            "stale producer epoch",
            "idempotency_gap",
            "idempotency gap",
            "idempotency error",
            "first message",
            "seqnum=1",
            "seqnum 1",
            "seq_num=1",
        )
    )


def is_stale_producer_epoch(response: str) -> bool:
    return is_terminal_producer_error(response)


def is_offset_out_of_range(response: str) -> bool:
    return decode_error_code(response) == "OFFSET_OUT_OF_RANGE"


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
    try:
        fields = require_ok(response, operation="version")
    except ProtocolError as exc:
        raise ValueError(f"unexpected version response: {response.strip()}") from exc
    if "version" not in fields:
        raise ValueError(f"missing version in response: {response.strip()}")
    return int(fields["version"])


def decode_snapshot_response(response: str) -> str | None:
    resp = response.strip()
    try:
        require_ok(resp, operation="snapshot")
    except ProtocolError as exc:
        raise ValueError(f"unexpected snapshot response: {resp}") from exc
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
