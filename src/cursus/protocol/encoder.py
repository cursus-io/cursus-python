import struct

from cursus.protocol.wire import BATCH_MAGIC, BATCH_VERSION, MAX_FRAME_PAYLOAD
from cursus.types import Message

MAX_MESSAGE_SIZE = MAX_FRAME_PAYLOAD

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


def _bytes(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _string(value: str) -> bytes:
    return _bytes(value.encode("utf-8"))


def encode_message(_topic: str, payload: str) -> bytes:
    """Return command text for the Wire v2 connection to encode."""
    return payload.encode("utf-8")


def _record_presence(message: Message) -> int:
    result = 0
    if message.timestamp:
        result |= _RECORD_TIMESTAMP
    if message.producer_id or message.seq_num or message.epoch:
        result |= _RECORD_PRODUCER
    if message.key:
        result |= _RECORD_KEY
    if message.event_type:
        result |= _RECORD_EVENT_TYPE
    if message.schema_version:
        result |= _RECORD_SCHEMA_VERSION
    if message.aggregate_version:
        result |= _RECORD_AGGREGATE_VERSION
    if message.metadata:
        result |= _RECORD_METADATA
    if message.transactional_id:
        result |= _RECORD_TRANSACTIONAL_ID
    if message.transaction_state:
        result |= _RECORD_TRANSACTION_STATE
    if message.transaction_marker:
        result |= _RECORD_TRANSACTION_MARKER
    if message.control_batch_type:
        result |= _RECORD_CONTROL_BATCH_TYPE
    if message.control_batch_version:
        result |= _RECORD_CONTROL_BATCH_VERSION
    if message.control_batch_coordinator_epoch:
        result |= _RECORD_CONTROL_COORDINATOR_EPOCH
    if message.control_batch_key is not None:
        result |= _RECORD_CONTROL_KEY
    if message.control_batch_value is not None:
        result |= _RECORD_CONTROL_VALUE
    return result


def _encode_record(topic: str, partition: int, message: Message) -> bytes:
    presence = _record_presence(message)
    data = bytearray(struct.pack(">HQ", _RECORD_VERSION, presence))
    data += _string(topic)
    data += struct.pack(">iQ", partition, message.offset)
    data += _string(message.payload)
    if presence & _RECORD_TIMESTAMP:
        data += struct.pack(">q", message.timestamp)
    if presence & _RECORD_PRODUCER:
        data += _string(message.producer_id)
        data += struct.pack(">Qq", message.seq_num, message.epoch)
    if presence & _RECORD_KEY:
        data += _string(message.key)
    if presence & _RECORD_EVENT_TYPE:
        data += _string(message.event_type)
    if presence & _RECORD_SCHEMA_VERSION:
        data += struct.pack(">I", message.schema_version)
    if presence & _RECORD_AGGREGATE_VERSION:
        data += struct.pack(">Q", message.aggregate_version)
    if presence & _RECORD_METADATA:
        data += _string(message.metadata)
    if presence & _RECORD_TRANSACTIONAL_ID:
        data += _string(message.transactional_id)
    if presence & _RECORD_TRANSACTION_STATE:
        data += _string(message.transaction_state)
    if presence & _RECORD_TRANSACTION_MARKER:
        data += _string(message.transaction_marker)
    if presence & _RECORD_CONTROL_BATCH_TYPE:
        data += _string(message.control_batch_type)
    if presence & _RECORD_CONTROL_BATCH_VERSION:
        data += struct.pack(">h", message.control_batch_version)
    if presence & _RECORD_CONTROL_COORDINATOR_EPOCH:
        data += struct.pack(">q", message.control_batch_coordinator_epoch)
    if presence & _RECORD_CONTROL_KEY:
        data += _bytes(message.control_batch_key or b"")
    if presence & _RECORD_CONTROL_VALUE:
        data += _bytes(message.control_batch_value or b"")
    return bytes(data)


def encode_batch(
    topic: str,
    partition: int,
    acks: str,
    idempotent: bool,
    messages: list[Message],
) -> bytes:
    if acks not in ("", "0", "1", "-1", "all"):
        raise ValueError(f"invalid acknowledgements: {acks}")
    if len(messages) > 100_000:
        raise ValueError("message count exceeds Wire v2 maximum")

    flags = _BATCH_FLAG_IDEMPOTENT if idempotent else 0
    seq_start = messages[0].seq_num if messages else 0
    seq_end = messages[-1].seq_num if messages else 0
    data = bytearray(struct.pack(">IHH", BATCH_MAGIC, BATCH_VERSION, flags))
    data += _string(topic)
    data += struct.pack(">i", partition)
    data += _string(acks)
    data += struct.pack(">QQI", seq_start, seq_end, len(messages))
    for message in messages:
        data += _bytes(_encode_record(topic, partition, message))
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("batch exceeds Wire v2 maximum")
    return bytes(data)
