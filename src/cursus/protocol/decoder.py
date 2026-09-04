import json
import shlex
import struct
from typing import Any

from cursus.errors import (
    AuthenticationRequiredError,
    AuthorizationDeniedError,
    BrokerError,
    ProducerFencedError,
    ProtocolError,
    ValidationError,
)
from cursus.protocol.wire import BATCH_MAGIC, BATCH_VERSION
from cursus.types import (
    AckResponse,
    Message,
    OffsetRange,
    PartitionOffsetRange,
    ProducerSession,
    StreamControl,
    TransactionStatus,
)

_BATCH_FLAG_IDEMPOTENT = 1
_RECORD_VERSION = 2
_RECORD_TIMESTAMP = 1 << 0
_RECORD_PRODUCER = 1 << 1
_RECORD_KEY = 1 << 2
_RECORD_EVENT_TYPE = 1 << 3
_RECORD_SCHEMA_VERSION = 1 << 4
_RECORD_AGGREGATE_VERSION = 1 << 5
_RECORD_METADATA = 1 << 6
_RECORD_TRANSACTIONAL_ID = 1 << 7
_RECORD_TRANSACTION_STATE = 1 << 8
_RECORD_TRANSACTION_MARKER = 1 << 9
_RECORD_CONTROL_BATCH_TYPE = 1 << 10
_RECORD_CONTROL_BATCH_VERSION = 1 << 11
_RECORD_CONTROL_COORDINATOR_EPOCH = 1 << 12
_RECORD_CONTROL_KEY = 1 << 13
_RECORD_CONTROL_VALUE = 1 << 14
_RECORD_KNOWN_MASK = (1 << 15) - 1


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

    def read_int16(self) -> int:
        return int(struct.unpack(">h", self.read(2))[0])

    def read_bytes(self) -> bytes:
        return self.read(self.read_uint32())

    def read_string(self) -> str:
        try:
            return self.read_bytes().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("invalid UTF-8 record field") from exc

    def finish(self) -> None:
        if self._pos != len(self._data):
            raise ProtocolError(f"batch has {len(self._data) - self._pos} trailing bytes")


def decode_batch(data: bytes) -> tuple[list[Message], str, int]:
    r = _ByteReader(data)

    magic = r.read_uint32()
    if magic != BATCH_MAGIC:
        raise ProtocolError(f"invalid magic number: 0x{magic:08X}")
    version = r.read_uint16()
    if version != BATCH_VERSION:
        raise ProtocolError(f"unsupported batch version: {version}")
    flags = r.read_uint16()
    if flags & ~_BATCH_FLAG_IDEMPOTENT:
        raise ProtocolError(f"batch contains unknown flags: 0x{flags:X}")

    topic = r.read_string()
    partition = r.read_int32()
    acks = r.read_string()
    if acks not in ("", "0", "1", "-1", "all"):
        raise ProtocolError(f"invalid acknowledgements: {acks}")
    r.read_uint64()
    r.read_uint64()

    msg_count = r.read_uint32()
    if msg_count > 100_000:
        raise ProtocolError("message count exceeds Wire v2 maximum")
    messages: list[Message] = []

    for _ in range(msg_count):
        record = _decode_record(r.read_bytes())
        if record.pop("topic") != topic or record["partition"] != partition:
            raise ProtocolError("record routing conflicts with batch")
        if record["payload"] == "__cursus_txn_control_marker__":
            continue
        messages.append(Message(**record))

    r.finish()
    return messages, topic, partition


def _decode_record(data: bytes) -> dict[str, Any]:
    r = _ByteReader(data)
    version = r.read_uint16()
    if version != _RECORD_VERSION:
        raise ProtocolError(f"unsupported record version: {version}")
    presence = r.read_uint64()
    if presence & ~_RECORD_KNOWN_MASK:
        raise ProtocolError(f"record contains unknown presence bits: 0x{presence:X}")
    result: dict[str, Any] = {
        "topic": r.read_string(),
        "partition": r.read_int32(),
        "offset": r.read_uint64(),
        "payload": r.read_string(),
        "seq_num": 0,
    }
    if presence & _RECORD_TIMESTAMP:
        result["timestamp"] = r.read_int64()
    if presence & _RECORD_PRODUCER:
        result["producer_id"] = r.read_string()
        result["seq_num"] = r.read_uint64()
        result["epoch"] = r.read_int64()
    if presence & _RECORD_KEY:
        result["key"] = r.read_string()
    if presence & _RECORD_EVENT_TYPE:
        result["event_type"] = r.read_string()
    if presence & _RECORD_SCHEMA_VERSION:
        result["schema_version"] = r.read_uint32()
    if presence & _RECORD_AGGREGATE_VERSION:
        result["aggregate_version"] = r.read_uint64()
    if presence & _RECORD_METADATA:
        result["metadata"] = r.read_string()
    if presence & _RECORD_TRANSACTIONAL_ID:
        result["transactional_id"] = r.read_string()
    if presence & _RECORD_TRANSACTION_STATE:
        result["transaction_state"] = r.read_string()
    if presence & _RECORD_TRANSACTION_MARKER:
        result["transaction_marker"] = r.read_string()
    if presence & _RECORD_CONTROL_BATCH_TYPE:
        result["control_batch_type"] = r.read_string()
    if presence & _RECORD_CONTROL_BATCH_VERSION:
        result["control_batch_version"] = r.read_int16()
    if presence & _RECORD_CONTROL_COORDINATOR_EPOCH:
        result["control_batch_coordinator_epoch"] = r.read_int64()
    if presence & _RECORD_CONTROL_KEY:
        result["control_batch_key"] = r.read_bytes()
    if presence & _RECORD_CONTROL_VALUE:
        result["control_batch_value"] = r.read_bytes()
    r.finish()
    return result


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
    return _decode_fields(shlex.split(resp)[2:])


def decode_error_code(response: str) -> str:
    resp = response.strip()
    if not is_error_response(resp):
        return ""
    parts = resp.split(maxsplit=2)
    return parts[1] if len(parts) > 1 else ""


def error_from_response(response: str) -> BrokerError:
    code = decode_error_code(response)
    fields = decode_error_fields(response)
    error_class = fields.pop("class", "")
    retryable = fields.pop("retryable", "false").lower() == "true"
    lower = response.lower()
    if code in {"AUTHENTICATION_REQUIRED", "authentication_required"}:
        return AuthenticationRequiredError(
            code, error_class, retryable, response, fields, response=response
        )
    if code in {"NOT_AUTHORIZED_FOR_TOPIC", "authorization_denied", "AUTHORIZATION_DENIED"}:
        return AuthorizationDeniedError(
            code, error_class, retryable, response, fields, response=response
        )
    if "producer_fenced" in lower or "stale_producer_epoch" in lower:
        return ProducerFencedError(
            code or "producer_fenced", error_class, retryable, response, fields, response=response
        )
    if code.startswith("invalid_") or code.startswith("missing_"):
        return ValidationError(code, error_class, retryable, response, fields, response=response)
    return BrokerError(code, error_class, retryable, response, fields, response=response)


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
    text = data.decode("utf-8").strip()
    if text.startswith("ERROR:"):
        tokens = shlex.split(text)
        fields = decode_error_fields(text)
        return AckResponse(
            status="ERROR",
            last_offset=int(fields.get("offset", "0")),
            producer_epoch=0,
            producer_id="",
            seq_start=0,
            seq_end=0,
            error=text,
            error_code=tokens[1] if len(tokens) > 1 else "",
            error_class=fields.get("class", ""),
            retryable=fields.get("retryable", "false").lower() == "true",
            error_fields={
                key: value for key, value in fields.items() if key not in {"class", "retryable"}
            },
        )
    obj = json.loads(text)
    return AckResponse(
        status=obj.get("status", ""),
        last_offset=obj.get("last_offset", 0),
        producer_epoch=obj.get("producer_epoch", 0),
        producer_id=obj.get("producer_id", ""),
        seq_start=obj.get("seq_start", 0),
        seq_end=obj.get("seq_end", 0),
        leader=obj.get("leader", ""),
        error=obj.get("error", ""),
        error_code=obj.get("error_code", ""),
        error_class=obj.get("error_class", ""),
        retryable=bool(obj.get("retryable", False)),
        error_fields=dict(obj.get("error_fields", {})),
    )
