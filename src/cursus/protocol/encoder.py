import struct

from cursus.types import Message

BATCH_MAGIC = 0xBA7C
MAX_MESSAGE_SIZE = 64 * 1024 * 1024


def encode_message(topic: str, payload: str) -> bytes:
    topic_bytes = topic.encode()
    payload_bytes = payload.encode()
    data = bytearray(2 + len(topic_bytes) + len(payload_bytes))
    struct.pack_into(">H", data, 0, len(topic_bytes))
    data[2 : 2 + len(topic_bytes)] = topic_bytes
    data[2 + len(topic_bytes) :] = payload_bytes
    return bytes(data)


def encode_batch(
    topic: str,
    partition: int,
    acks: str,
    idempotent: bool,
    messages: list[Message],
) -> bytes:
    buf = bytearray()

    buf += struct.pack(">H", BATCH_MAGIC)

    topic_bytes = topic.encode()
    buf += struct.pack(">H", len(topic_bytes))
    buf += topic_bytes

    buf += struct.pack(">i", partition)

    acks_bytes = acks.encode()
    buf += struct.pack(">B", len(acks_bytes))
    buf += acks_bytes

    buf += struct.pack(">?", idempotent)

    seq_start = messages[0].seq_num if messages else 0
    seq_end = messages[-1].seq_num if messages else 0
    buf += struct.pack(">Q", seq_start)
    buf += struct.pack(">Q", seq_end)

    buf += struct.pack(">i", len(messages))

    for m in messages:
        buf += struct.pack(">Q", m.offset)
        buf += struct.pack(">Q", m.seq_num)

        pid = m.producer_id.encode()
        buf += struct.pack(">H", len(pid))
        buf += pid

        key = m.key.encode()
        buf += struct.pack(">H", len(key))
        buf += key

        buf += struct.pack(">q", m.epoch)

        pld = m.payload.encode()
        buf += struct.pack(">I", len(pld))
        buf += pld

        et = m.event_type.encode()
        buf += struct.pack(">H", len(et))
        buf += et

        buf += struct.pack(">I", m.schema_version)
        buf += struct.pack(">Q", m.aggregate_version)

        md = m.metadata.encode()
        buf += struct.pack(">H", len(md))
        buf += md

    return bytes(buf)
