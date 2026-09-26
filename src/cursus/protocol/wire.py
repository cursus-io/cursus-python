from __future__ import annotations

import gzip
import io
import json
import struct
from dataclasses import dataclass
from enum import IntEnum

from cursus.errors import ProtocolError

try:
    import snappy as _snappy
except ImportError:  # pragma: no cover - exercised by installations without the extra
    _snappy = None

try:
    import lz4.frame as _lz4_frame
except ImportError:  # pragma: no cover - exercised by installations without the extra
    _lz4_frame = None

PROTOCOL_VERSION = 2
HEADER_SIZE = 32
MAX_FRAME_PAYLOAD = 64 * 1024 * 1024
FRAME_MAGIC = 0x43525332  # CRS2
COMMAND_PAYLOAD_MAGIC = 0x43525132  # CRQ2
COMMAND_PAYLOAD_VERSION = 2
BATCH_MAGIC = 0x43425632  # CBV2
BATCH_VERSION = 2

_FLAG_COMPRESSION_EXPLICIT = 1
_FLAG_COMPRESSION_SHIFT = 1
_FLAG_COMPRESSION_MASK = 0x0E
_VALID_FRAME_FLAGS = _FLAG_COMPRESSION_EXPLICIT | _FLAG_COMPRESSION_MASK
_XERIAL_SNAPPY_HEADER = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01"
_XERIAL_SNAPPY_BLOCK_SIZE = 32 * 1024


class Kind(IntEnum):
    NEGOTIATION_REQUEST = 1
    NEGOTIATION_RESPONSE = 2
    REQUEST = 3
    RESPONSE = 4
    STREAM = 5


class Status(IntEnum):
    NONE = 0
    OK = 1
    ERROR = 2
    STREAM_END = 3


class Compression(IntEnum):
    NONE = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3


class ErrorClass(IntEnum):
    VALIDATION = 1
    AUTHORIZATION = 2
    ROUTING = 3
    AVAILABILITY = 4
    CONFLICT = 5
    FENCING = 6
    NOT_FOUND = 7
    INTERNAL = 8


class Command(IntEnum):
    UNKNOWN = 0
    AUTH = 1
    CREATE = 2
    ALTER_TOPIC_CONFIG = 3
    DELETE = 4
    TRUNCATE = 5
    LIST = 6
    LIST_CLUSTER = 7
    CLUSTER_STATUS = 8
    ELECT_LEADER = 9
    PUBLISH = 10
    CONSUME = 11
    STREAM = 12
    HELP = 13
    HEARTBEAT = 14
    JOIN_GROUP = 15
    SYNC_GROUP = 16
    LEAVE_GROUP = 17
    COMMIT_OFFSET = 18
    BATCH_COMMIT = 19
    REGISTER_GROUP = 20
    GROUP_STATUS = 21
    FETCH_OFFSET = 22
    LIST_GROUPS = 23
    LIST_OFFSETS = 24
    DESCRIBE = 25
    INIT_PRODUCER_ID = 26
    BEGIN_TXN = 27
    TXN_PUBLISH = 28
    TXN_APPEND_STREAM = 29
    SEND_OFFSETS_TO_TXN = 30
    END_TXN = 31
    TXN_STATUS = 32
    APPEND_STREAM = 33
    READ_STREAM = 34
    SAVE_SNAPSHOT = 35
    READ_SNAPSHOT = 36
    STREAM_VERSION = 37
    REPLICATE_MESSAGE = 38
    REPLICATE_SNAPSHOT = 39
    LIST_SNAPSHOTS = 40
    FETCH_SNAPSHOT = 41
    CATCHUP_SNAPSHOTS = 42
    FIND_COORDINATOR = 43
    RAFT_APPLY = 44
    METADATA = 45
    INTERNAL_BATCH = 46
    NEGOTIATE = 47
    EXIT = 48
    JOIN_CLUSTER = 49
    LEAVE_CLUSTER = 50
    HEARTBEAT_CLUSTER = 51
    REPLICA_CATCHUP = 52
    AGGREGATE_REPLAY_PROOF = 53
    AGGREGATE_EVENT_RANGE_READ = 54


_COMMANDS = {command.name: command for command in Command if command is not Command.UNKNOWN}
_ERROR_CLASS_NAMES = {
    ErrorClass.VALIDATION: "validation",
    ErrorClass.AUTHORIZATION: "authorization",
    ErrorClass.ROUTING: "routing",
    ErrorClass.AVAILABILITY: "availability",
    ErrorClass.CONFLICT: "conflict",
    ErrorClass.FENCING: "fencing",
    ErrorClass.NOT_FOUND: "not_found",
    ErrorClass.INTERNAL: "internal",
}


@dataclass(frozen=True)
class Frame:
    kind: Kind
    command: Command
    status: Status
    request_id: int
    payload: bytes
    version: int = PROTOCOL_VERSION


@dataclass(frozen=True)
class ErrorPayload:
    code: str
    error_class: ErrorClass
    retryable: bool
    message: str
    fields: dict[str, str]


class _Writer:
    def __init__(self) -> None:
        self.data = bytearray()

    def uint16(self, value: int) -> None:
        self.data += struct.pack(">H", value)

    def uint32(self, value: int) -> None:
        self.data += struct.pack(">I", value)

    def uint64(self, value: int) -> None:
        self.data += struct.pack(">Q", value)

    def bytes(self, value: bytes) -> None:
        if len(value) > 0xFFFFFFFF:
            raise ProtocolError("field exceeds uint32 length")
        self.uint32(len(value))
        self.data += value

    def string(self, value: str) -> None:
        self.bytes(value.encode("utf-8"))


class _Reader:
    def __init__(self, data: bytes) -> None:
        if len(data) > MAX_FRAME_PAYLOAD:
            raise ProtocolError("payload exceeds Wire v2 maximum")
        self.data = data
        self.position = 0

    def take(self, size: int) -> bytes:
        if size < 0 or size > len(self.data) - self.position:
            raise ProtocolError(f"truncated binary field at offset {self.position}")
        result = self.data[self.position : self.position + size]
        self.position += size
        return result

    def uint16(self) -> int:
        return int(struct.unpack(">H", self.take(2))[0])

    def uint32(self) -> int:
        return int(struct.unpack(">I", self.take(4))[0])

    def uint64(self) -> int:
        return int(struct.unpack(">Q", self.take(8))[0])

    def bytes(self) -> bytes:
        return self.take(self.uint32())

    def string(self) -> str:
        try:
            return self.bytes().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("invalid UTF-8 field") from exc

    def finish(self) -> None:
        if self.position != len(self.data):
            trailing = len(self.data) - self.position
            raise ProtocolError(f"binary payload has {trailing} trailing bytes")


def compression_from_name(value: str) -> Compression:
    normalized = value.strip().lower()
    names = {
        "": Compression.NONE,
        "none": Compression.NONE,
        "gzip": Compression.GZIP,
        "snappy": Compression.SNAPPY,
        "lz4": Compression.LZ4,
    }
    try:
        return names[normalized]
    except KeyError as exc:
        raise ProtocolError(f"unsupported compression type: {value}") from exc


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for value in data:
        crc ^= value
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def require_compression(compression: Compression) -> None:
    if compression is Compression.SNAPPY and _snappy is None:
        raise ProtocolError("compression snappy requires the 'snappy' package extra")
    if compression is Compression.LZ4 and _lz4_frame is None:
        raise ProtocolError("compression lz4 requires the 'lz4' package extra")


def _compress(payload: bytes, compression: Compression) -> bytes:
    if compression is Compression.NONE:
        return payload
    if compression is Compression.GZIP:
        return gzip.compress(payload)
    require_compression(compression)
    if compression is Compression.SNAPPY:
        assert _snappy is not None
        encoded = bytearray(_XERIAL_SNAPPY_HEADER)
        for position in range(0, len(payload), _XERIAL_SNAPPY_BLOCK_SIZE):
            block = _snappy.compress(payload[position : position + _XERIAL_SNAPPY_BLOCK_SIZE])
            encoded += struct.pack(">I", len(block))
            encoded += block
        return bytes(encoded)
    if compression is Compression.LZ4:
        assert _lz4_frame is not None
        return bytes(_lz4_frame.compress(payload))
    raise ProtocolError(f"unsupported compression {compression.name.lower()}")


def _read_bounded(reader: object, expected: int) -> bytes:
    read = getattr(reader, "read")
    decoded = read(expected + 1)
    if len(decoded) > expected:
        raise ProtocolError(f"decoded payload exceeds declared length {expected}")
    return bytes(decoded)


def _decompress_snappy(payload: bytes, decoded_size: int) -> bytes:
    require_compression(Compression.SNAPPY)
    assert _snappy is not None
    try:
        if not payload.startswith(_XERIAL_SNAPPY_HEADER[:8]):
            decoded = bytes(_snappy.decompress(payload))
        else:
            if len(payload) < len(_XERIAL_SNAPPY_HEADER) or not payload.startswith(
                _XERIAL_SNAPPY_HEADER
            ):
                raise ProtocolError("malformed xerial snappy header")
            position = len(_XERIAL_SNAPPY_HEADER)
            chunks: list[bytes] = []
            total = 0
            while position < len(payload):
                if len(payload) - position < 4:
                    raise ProtocolError("malformed xerial snappy block size")
                block_size = int(struct.unpack_from(">I", payload, position)[0])
                position += 4
                if block_size > len(payload) - position:
                    raise ProtocolError("malformed xerial snappy block")
                block = bytes(_snappy.decompress(payload[position : position + block_size]))
                position += block_size
                total += len(block)
                if total > decoded_size or total > MAX_FRAME_PAYLOAD:
                    raise ProtocolError("decoded snappy payload exceeds declared length")
                chunks.append(block)
            decoded = b"".join(chunks)
    except ProtocolError:
        raise
    except Exception as exc:
        raise ProtocolError("Wire v2 snappy decompression failed") from exc
    return decoded


def _decompress_lz4(payload: bytes, decoded_size: int) -> bytes:
    require_compression(Compression.LZ4)
    assert _lz4_frame is not None
    try:
        decompressor = _lz4_frame.LZ4FrameDecompressor()
        decoded = bytes(decompressor.decompress(payload, max_length=decoded_size + 1))
        if len(decoded) > decoded_size:
            raise ProtocolError("decoded lz4 payload exceeds declared length")
        if not decompressor.eof:
            extra = bytes(decompressor.decompress(b"", max_length=decoded_size + 1 - len(decoded)))
            decoded += extra
        if not decompressor.eof or decompressor.unused_data:
            raise ProtocolError("Wire v2 lz4 frame is incomplete or has trailing data")
        return decoded
    except ProtocolError:
        raise
    except Exception as exc:
        raise ProtocolError("Wire v2 lz4 decompression failed") from exc


def _decompress(payload: bytes, compression: Compression, decoded_size: int) -> bytes:
    if compression is Compression.NONE:
        decoded = payload
    elif compression is Compression.GZIP:
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(payload)) as reader:
                decoded = _read_bounded(reader, decoded_size)
        except (OSError, EOFError) as exc:
            raise ProtocolError("Wire v2 gzip decompression failed") from exc
    elif compression is Compression.SNAPPY:
        decoded = _decompress_snappy(payload, decoded_size)
    elif compression is Compression.LZ4:
        decoded = _decompress_lz4(payload, decoded_size)
    else:
        raise ProtocolError(f"unsupported compression {compression.name.lower()}")
    if len(decoded) != decoded_size:
        raise ProtocolError(
            f"decoded payload length mismatch: got={len(decoded)} expected={decoded_size}"
        )
    if len(decoded) > MAX_FRAME_PAYLOAD:
        raise ProtocolError("decoded payload exceeds Wire v2 maximum")
    return decoded


def _validate_semantics(frame: Frame) -> None:
    if frame.version != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported Wire v2 version {frame.version}")
    if frame.kind is Kind.NEGOTIATION_REQUEST:
        if frame.status is not Status.NONE:
            raise ProtocolError("negotiation request status must be zero")
    elif frame.kind in (Kind.NEGOTIATION_RESPONSE, Kind.RESPONSE):
        if frame.status not in (Status.OK, Status.ERROR):
            raise ProtocolError("response status must be OK or error")
        if frame.kind is Kind.RESPONSE and frame.request_id == 0:
            raise ProtocolError("response request id is required")
    elif frame.kind is Kind.REQUEST:
        if frame.status is not Status.NONE or frame.request_id == 0:
            raise ProtocolError("request status must be zero and request id is required")
    elif frame.kind is Kind.STREAM:
        if frame.status not in (Status.OK, Status.ERROR, Status.STREAM_END):
            raise ProtocolError("invalid stream status")
        if frame.request_id == 0:
            raise ProtocolError("stream request id is required")
    if frame.kind in (Kind.NEGOTIATION_REQUEST, Kind.NEGOTIATION_RESPONSE):
        if frame.command is not Command.NEGOTIATE:
            raise ProtocolError("negotiation frame requires NEGOTIATE command")
    if frame.command is Command.UNKNOWN:
        raise ProtocolError("unknown Wire v2 command")


def encode_frame(frame: Frame, compression: Compression) -> bytes:
    _validate_semantics(frame)
    if len(frame.payload) > MAX_FRAME_PAYLOAD:
        raise ProtocolError("payload exceeds Wire v2 maximum")
    selected = (
        Compression.NONE
        if frame.kind
        in (
            Kind.NEGOTIATION_REQUEST,
            Kind.NEGOTIATION_RESPONSE,
        )
        else compression
    )
    flags = (
        0
        if selected is Compression.NONE
        and frame.kind
        in (
            Kind.NEGOTIATION_REQUEST,
            Kind.NEGOTIATION_RESPONSE,
        )
        else _FLAG_COMPRESSION_EXPLICIT | (int(selected) << _FLAG_COMPRESSION_SHIFT)
    )
    encoded = _compress(frame.payload, selected)
    if len(encoded) > MAX_FRAME_PAYLOAD:
        raise ProtocolError("encoded payload exceeds Wire v2 maximum")
    header = struct.pack(
        ">IHBBHHQIII",
        FRAME_MAGIC,
        frame.version,
        int(frame.kind),
        flags,
        int(frame.command),
        int(frame.status),
        frame.request_id,
        len(encoded),
        len(frame.payload),
        crc32c(encoded),
    )
    return header + encoded


def decode_frame(data: bytes, compression: Compression) -> Frame:
    if len(data) < HEADER_SIZE:
        raise ProtocolError("Wire v2 frame header is truncated")
    (
        magic,
        version,
        kind_value,
        flags,
        command_value,
        status_value,
        request_id,
        encoded_size,
        decoded_size,
        checksum,
    ) = struct.unpack(">IHBBHHQIII", data[:HEADER_SIZE])
    if magic != FRAME_MAGIC:
        raise ProtocolError("invalid Wire v2 frame magic")
    if encoded_size > MAX_FRAME_PAYLOAD or decoded_size > MAX_FRAME_PAYLOAD:
        raise ProtocolError("Wire v2 frame exceeds size limit")
    if len(data) != HEADER_SIZE + encoded_size:
        raise ProtocolError("Wire v2 encoded length mismatch")
    try:
        kind = Kind(kind_value)
        command = Command(command_value)
        status = Status(status_value)
    except ValueError as exc:
        raise ProtocolError("invalid Wire v2 frame enum") from exc
    if flags & ~_VALID_FRAME_FLAGS:
        raise ProtocolError("Wire v2 frame contains unknown flags")
    if kind in (Kind.NEGOTIATION_REQUEST, Kind.NEGOTIATION_RESPONSE):
        if flags != 0:
            raise ProtocolError("negotiation frame must be uncompressed")
        selected = Compression.NONE
    else:
        if flags & _FLAG_COMPRESSION_EXPLICIT == 0:
            raise ProtocolError("Wire v2 compression flag is not explicit")
        try:
            selected = Compression((flags & _FLAG_COMPRESSION_MASK) >> _FLAG_COMPRESSION_SHIFT)
        except ValueError as exc:
            raise ProtocolError("unsupported Wire v2 compression") from exc
        if selected is not compression:
            raise ProtocolError(
                f"Wire v2 compression mismatch: negotiated={compression.name.lower()} "
                f"frame={selected.name.lower()}"
            )
    encoded = data[HEADER_SIZE:]
    if crc32c(encoded) != checksum:
        raise ProtocolError("Wire v2 checksum mismatch")
    frame = Frame(
        version=version,
        kind=kind,
        command=command,
        status=status,
        request_id=request_id,
        payload=_decompress(encoded, selected, decoded_size),
    )
    _validate_semantics(frame)
    return frame


def encoded_frame_size(header: bytes) -> int:
    if len(header) != HEADER_SIZE:
        raise ProtocolError("Wire v2 frame header is truncated")
    magic = int(struct.unpack_from(">I", header, 0)[0])
    encoded_size = int(struct.unpack_from(">I", header, 20)[0])
    decoded_size = int(struct.unpack_from(">I", header, 24)[0])
    if magic != FRAME_MAGIC:
        raise ProtocolError("invalid Wire v2 frame magic")
    if encoded_size > MAX_FRAME_PAYLOAD or decoded_size > MAX_FRAME_PAYLOAD:
        raise ProtocolError("Wire v2 frame exceeds size limit")
    return encoded_size


def encode_negotiation_request(compressions: list[Compression]) -> bytes:
    if not compressions or len(compressions) > 4 or len(set(compressions)) != len(compressions):
        raise ProtocolError("invalid compression preferences")
    return struct.pack(">HHH", PROTOCOL_VERSION, PROTOCOL_VERSION, len(compressions)) + bytes(
        int(value) for value in compressions
    )


def decode_negotiation_response(data: bytes) -> Compression:
    if len(data) != 3:
        raise ProtocolError("invalid negotiation response length")
    version, compression_value = struct.unpack(">HB", data)
    if version != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported negotiated version {version}")
    try:
        return Compression(compression_value)
    except ValueError as exc:
        raise ProtocolError("broker selected unsupported compression") from exc


def _find_trailing_field(rest: str, name: str) -> int:
    marker = name + "="
    position = rest.find(marker)
    while position >= 0:
        if position == 0 or rest[position - 1].isspace():
            return position
        position = rest.find(marker, position + 1)
    return -1


def parse_command_text(text: str) -> tuple[Command, list[str], list[tuple[str, str]]]:
    stripped = text.strip()
    if not stripped:
        raise ProtocolError("command is empty")
    name, _, rest = stripped.partition(" ")
    try:
        command = _COMMANDS[name.upper()]
    except KeyError as exc:
        raise ProtocolError(f"unknown Wire v2 command {name!r}") from exc

    trailing: list[tuple[str, str]] = []
    message_position = _find_trailing_field(rest, "message")
    payload_position = _find_trailing_field(rest, "payload")
    if message_position >= 0:
        message = rest[message_position + len("message=") :].strip()
        rest = rest[:message_position].strip()
        metadata_position = _find_trailing_field(rest, "metadata")
        if metadata_position >= 0:
            metadata = rest[metadata_position + len("metadata=") :].strip()
            rest = rest[:metadata_position].strip()
            trailing.append(("metadata", metadata))
        trailing.append(("message", message))
    elif payload_position >= 0:
        payload = rest[payload_position + len("payload=") :].strip()
        rest = rest[:payload_position].strip()
        trailing.append(("payload", payload))

    positionals: list[str] = []
    fields: list[tuple[str, str]] = []
    seen: set[str] = set()
    for part in rest.split():
        key, separator, value = part.partition("=")
        if not separator:
            positionals.append(part)
            continue
        if not _valid_field_name(key) or key in seen:
            raise ProtocolError(f"invalid or duplicate command field {key!r}")
        seen.add(key)
        fields.append((key, value))
    for key, value in trailing:
        if key in seen:
            raise ProtocolError(f"duplicate command field {key!r}")
        seen.add(key)
        fields.append((key, value))
    return command, positionals, fields


def _valid_field_name(value: str) -> bool:
    if not value or not ("a" <= value[0] <= "z"):
        return False
    return all(
        char == "_" or char.isdigit() or "a" <= char <= "z" or "A" <= char <= "Z"
        for char in value[1:]
    )


def encode_command_request(data: bytes) -> tuple[Command, bytes]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("command is not UTF-8") from exc
    command, positionals, fields = parse_command_text(text)
    if len(positionals) > 1024 or len(fields) > 1024:
        raise ProtocolError("command argument count exceeds maximum 1024")
    writer = _Writer()
    writer.uint32(COMMAND_PAYLOAD_MAGIC)
    writer.uint16(COMMAND_PAYLOAD_VERSION)
    writer.uint16(len(positionals))
    for positional in positionals:
        writer.string(positional)
    writer.uint16(len(fields))
    for key, value in fields:
        writer.string(key)
        writer.string(value)
    return command, bytes(writer.data)


def is_batch(data: bytes) -> bool:
    return len(data) >= 6 and struct.unpack_from(">IH", data, 0) == (BATCH_MAGIC, BATCH_VERSION)


def request_frame_payload(data: bytes) -> tuple[Command, bytes]:
    if is_batch(data):
        return Command.PUBLISH, data
    return encode_command_request(data)


def response_suppressed(data: bytes) -> bool:
    if is_batch(data):
        reader = _Reader(data)
        reader.uint32()
        reader.uint16()
        reader.uint16()
        reader.string()
        reader.take(4)
        return reader.string() == "0"
    command, _, fields = parse_command_text(data.decode("utf-8"))
    return command is Command.PUBLISH and dict(fields).get("acks") == "0"


def encode_error(
    *,
    code: str,
    error_class: ErrorClass,
    retryable: bool,
    message: str,
    fields: dict[str, str],
) -> bytes:
    if not code or len(fields) > 256:
        raise ProtocolError("invalid Wire v2 error payload")
    writer = _Writer()
    writer.string(code)
    writer.data.append(int(error_class))
    writer.data.append(1 if retryable else 0)
    writer.string(message)
    writer.uint16(len(fields))
    for key in sorted(fields):
        if not key:
            raise ProtocolError("error field name is empty")
        writer.string(key)
        writer.string(fields[key])
    return bytes(writer.data)


def decode_error(data: bytes) -> ErrorPayload:
    reader = _Reader(data)
    code = reader.string()
    try:
        error_class = ErrorClass(reader.take(1)[0])
    except ValueError as exc:
        raise ProtocolError("invalid Wire v2 error class") from exc
    retryable_value = reader.take(1)[0]
    if retryable_value > 1:
        raise ProtocolError("invalid Wire v2 retryable flag")
    message = reader.string()
    count = reader.uint16()
    fields: dict[str, str] = {}
    for _ in range(count):
        key, value = reader.string(), reader.string()
        if not key or key in fields:
            raise ProtocolError("invalid duplicate Wire v2 error field")
        fields[key] = value
    reader.finish()
    if not code:
        raise ProtocolError("Wire v2 error code is empty")
    return ErrorPayload(code, error_class, retryable_value == 1, message, fields)


def render_error(error: ErrorPayload) -> bytes:
    parts = [
        "ERROR:",
        error.code,
        f"class={_ERROR_CLASS_NAMES[error.error_class]}",
        f"retryable={'true' if error.retryable else 'false'}",
    ]
    for key in sorted(error.fields):
        value = error.fields[key]
        rendered = json.dumps(value) if any(char.isspace() for char in value) else value
        parts.append(f"{key}={rendered}")
    if error.message:
        parts.append(error.message)
    return " ".join(parts).encode("utf-8")
