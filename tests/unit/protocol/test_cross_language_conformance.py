import json
from pathlib import Path

from cursus.protocol.decoder import decode_batch
from cursus.protocol.encoder import encode_batch
from cursus.protocol.wire import (
    Command,
    Compression,
    ErrorClass,
    Frame,
    Kind,
    Status,
    decode_error,
    decode_frame,
    decode_negotiation_response,
    encode_command_request,
    encode_error,
    encode_frame,
    encode_negotiation_request,
)
from cursus.types import Message

CAPABILITIES = [
    "wire_v2",
    "compression_none",
    "compression_gzip",
    "compression_snappy",
    "compression_lz4",
    "transport_tls",
    "authentication",
    "typed_errors",
    "request_correlation",
    "producer_acks",
    "producer_batching",
    "producer_idempotence",
    "safe_retry",
    "consumer_polling",
    "consumer_streaming",
    "consumer_groups",
    "offset_reset",
    "isolation_levels",
    "event_store",
    "snapshots",
    "transactions",
    "transactional_offsets",
    "transaction_status",
    "admin_client",
    "event_envelope",
    "aggregate_repository",
    "saga",
    "client_metrics",
    "error_classification",
]


def _fixture() -> dict[str, object]:
    path = Path(__file__).parents[2] / "fixtures" / "wire-v2.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _vectors() -> dict[str, bytes]:
    fixture = _fixture()
    return {
        vector["name"]: bytes.fromhex(vector["hex"])
        for vector in fixture["vectors"]  # type: ignore[index]
    }


def test_wire_constants_and_command_ids_match_go_fixture() -> None:
    fixture = _fixture()
    assert fixture["wire_version"] == 2
    assert fixture["header_size"] == 32
    assert fixture["max_payload"] == 64 * 1024 * 1024
    assert fixture["command_ids"] == {
        command.name: int(command) for command in Command if command is not Command.UNKNOWN
    }
    assert fixture["compression_ids"] == {
        compression.name.lower(): int(compression) for compression in Compression
    }
    assert fixture["capabilities"] == CAPABILITIES


def test_negotiation_and_frame_bytes_match_go_fixture() -> None:
    vectors = _vectors()
    preferences = [Compression.GZIP, Compression.SNAPPY, Compression.LZ4, Compression.NONE]
    assert (
        encode_negotiation_request(preferences) == vectors["negotiation_request_all_compressions"]
    )
    assert decode_negotiation_response(vectors["negotiation_response_lz4"]) is Compression.LZ4
    frame = Frame(
        kind=Kind.REQUEST,
        command=Command.PUBLISH,
        status=Status.NONE,
        request_id=42,
        payload=b"hello",
    )
    assert encode_frame(frame, Compression.NONE) == vectors["uncompressed_publish_frame"]


def test_decodes_go_generated_frames_for_every_compression() -> None:
    vectors = _vectors()
    expected = b"cross-language-compression-" * 2 + b"cross-language-compression"
    for compression in (Compression.GZIP, Compression.SNAPPY, Compression.LZ4):
        frame = decode_frame(vectors[f"{compression.name.lower()}_publish_frame"], compression)
        assert frame.request_id == 77
        assert frame.command is Command.PUBLISH
        assert frame.payload == expected


def test_command_and_structured_error_bytes_match_go_fixture() -> None:
    vectors = _vectors()
    command, payload = encode_command_request(
        b"APPEND_STREAM topic=events key=aggregate-7 expectedVersion=3 "
        b'eventType=Updated schemaVersion=2 metadata={"trace":"a b"} '
        b'message={"value":"x y"}'
    )
    assert command is Command.APPEND_STREAM
    assert payload == vectors["append_stream_command_payload"]

    encoded = encode_error(
        code="replication_unavailable",
        error_class=ErrorClass.AVAILABILITY,
        retryable=True,
        message="replication quorum unavailable",
        fields={"offset": "7", "reason": "replica timeout"},
    )
    assert encoded == vectors["structured_availability_error"]
    decoded = decode_error(encoded)
    assert decoded.retryable is True
    assert decoded.fields["reason"] == "replica timeout"


def test_full_record_batch_bytes_and_fields_match_go_fixture() -> None:
    vectors = _vectors()
    message = Message(
        offset=7,
        seq_num=9,
        payload="  opaque\x00한글\tpayload  ",
        key="aggregate-7",
        producer_id="producer-1",
        epoch=-2,
        event_type="Updated",
        schema_version=2,
        aggregate_version=3,
        metadata='{"trace":"a b"}',
        partition=2,
        timestamp=-123,
        transactional_id="txn-1",
        transaction_state="aborted",
        transaction_marker="abort",
        control_batch_type="transaction",
        control_batch_version=2,
        control_batch_coordinator_epoch=11,
        control_batch_key=b"\x00\x01\xff",
        control_batch_value=b"control-value",
    )
    encoded = encode_batch("events", 2, "all", True, [message])
    assert encoded == vectors["full_record_batch"]
    decoded, topic, partition = decode_batch(encoded)
    assert (topic, partition) == ("events", 2)
    assert decoded == [message]
