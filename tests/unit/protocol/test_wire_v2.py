import struct

import pytest

from cursus.errors import ProtocolError
from cursus.protocol.encoder import encode_batch
from cursus.protocol.wire import (
    Command,
    Compression,
    ErrorClass,
    Frame,
    Kind,
    Status,
    crc32c,
    decode_error,
    decode_frame,
    decode_negotiation_response,
    encode_command_request,
    encode_error,
    encode_frame,
    encode_negotiation_request,
    response_suppressed,
)
from cursus.types import Message


def test_crc32c_matches_castagnoli_reference_vector():
    assert crc32c(b"123456789") == 0xE3069283


def test_negotiation_payload_matches_go_wire_v2_layout():
    payload = encode_negotiation_request([Compression.GZIP, Compression.NONE])
    assert payload == bytes.fromhex("0002000200020100")
    assert decode_negotiation_response(bytes.fromhex("000201")) == Compression.GZIP


def test_command_request_uses_command_header_and_crq2_payload():
    command, payload = encode_command_request(b"METADATA topic=orders")

    assert command is Command.METADATA
    assert payload.startswith(bytes.fromhex("43525132000200000001"))
    assert payload.endswith(struct.pack(">I", 5) + b"topic" + struct.pack(">I", 6) + b"orders")


def test_frame_roundtrip_validates_crc_and_correlation_fields():
    encoded = encode_frame(
        Frame(
            kind=Kind.REQUEST,
            command=Command.METADATA,
            status=Status.NONE,
            request_id=7,
            payload=b"payload",
        ),
        Compression.NONE,
    )

    decoded = decode_frame(encoded, Compression.NONE)
    assert decoded.request_id == 7
    assert decoded.command is Command.METADATA
    assert decoded.payload == b"payload"

    corrupted = encoded[:-1] + bytes([encoded[-1] ^ 0xFF])
    with pytest.raises(ProtocolError, match="checksum"):
        decode_frame(corrupted, Compression.NONE)


def test_structured_error_roundtrip_preserves_retry_contract():
    encoded = encode_error(
        code="replication_unavailable",
        error_class=ErrorClass.AVAILABILITY,
        retryable=True,
        message="",
        fields={"offset": "7", "reason": "replica timeout"},
    )

    decoded = decode_error(encoded)
    assert decoded.code == "replication_unavailable"
    assert decoded.error_class is ErrorClass.AVAILABILITY
    assert decoded.retryable is True
    assert decoded.fields == {"offset": "7", "reason": "replica timeout"}


def test_zero_ack_batch_suppresses_wire_response():
    batch = encode_batch(
        "orders",
        0,
        "0",
        False,
        [Message(offset=0, seq_num=1, payload="created")],
    )

    assert response_suppressed(batch)


@pytest.mark.parametrize("compression", list(Compression))
def test_all_wire_compressions_roundtrip(compression: Compression):
    payload = (b"cross-language-wire-v2\x00" * 4096) + bytes(range(256))
    frame = Frame(
        kind=Kind.REQUEST,
        command=Command.PUBLISH,
        status=Status.NONE,
        request_id=17,
        payload=payload,
    )

    assert decode_frame(encode_frame(frame, compression), compression) == frame


@pytest.mark.parametrize("compression", [Compression.GZIP, Compression.SNAPPY, Compression.LZ4])
def test_compression_rejects_wrong_declared_size(compression: Compression):
    encoded = bytearray(
        encode_frame(
            Frame(
                kind=Kind.REQUEST,
                command=Command.PUBLISH,
                status=Status.NONE,
                request_id=18,
                payload=b"bounded decompression",
            ),
            compression,
        )
    )
    struct.pack_into(">I", encoded, 24, 1)

    with pytest.raises(ProtocolError, match="decoded|declared"):
        decode_frame(bytes(encoded), compression)
