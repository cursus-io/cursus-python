import json

import pytest

from cursus.errors import ProtocolError
from cursus.protocol.decoder import (
    decode_ack,
    decode_batch,
    decode_offset_out_of_range,
    decode_offset_response,
    decode_snapshot_response,
    decode_stream_control,
    decode_version_response,
    is_coordinator_failure,
    is_offset_out_of_range,
    is_offset_regression,
    is_stream_control_frame,
)
from cursus.protocol.encoder import encode_batch
from cursus.types import Message


def test_decode_batch_roundtrip():
    original = [
        Message(offset=0, seq_num=1, payload="hello", producer_id="p1", key="k1", epoch=100),
        Message(offset=1, seq_num=2, payload="world", producer_id="p1", key="k2", epoch=101),
    ]
    data = encode_batch("my-topic", 3, "1", True, original)
    messages, topic, partition = decode_batch(data)

    assert topic == "my-topic"
    assert partition == 3
    assert len(messages) == 2
    assert messages[0].payload == "hello"
    assert messages[0].key == "k1"
    assert messages[0].epoch == 100
    assert messages[1].payload == "world"
    assert messages[1].seq_num == 2


def test_decode_batch_event_sourcing_fields():
    original = [
        Message(
            offset=10,
            seq_num=5,
            payload="data",
            producer_id="p",
            event_type="Created",
            schema_version=2,
            aggregate_version=7,
            metadata='{"x":1}',
        ),
    ]
    data = encode_batch("es-topic", 0, "1", False, original)
    messages, _, _ = decode_batch(data)

    assert messages[0].event_type == "Created"
    assert messages[0].schema_version == 2
    assert messages[0].aggregate_version == 7
    assert messages[0].metadata == '{"x":1}'


def test_decode_batch_empty():
    data = encode_batch("t", 0, "1", False, [])
    messages, topic, partition = decode_batch(data)
    assert messages == []
    assert topic == "t"


def test_decode_batch_invalid_magic():
    bad_data = b"\x00\x00rest"
    with pytest.raises(ProtocolError, match="invalid magic"):
        decode_batch(bad_data)


def test_decode_ack_ok():
    raw = json.dumps(
        {
            "status": "OK",
            "last_offset": 100,
            "producer_epoch": 1,
            "producer_id": "p1",
            "seq_start": 1,
            "seq_end": 10,
            "leader": "localhost:9000",
            "error": "",
        }
    ).encode()
    ack = decode_ack(raw)
    assert ack.status == "OK"
    assert ack.last_offset == 100
    assert ack.leader == "localhost:9000"


def test_decode_ack_error():
    raw = json.dumps(
        {
            "status": "ERROR",
            "last_offset": 0,
            "producer_epoch": 0,
            "producer_id": "",
            "seq_start": 0,
            "seq_end": 0,
            "error": "topic not found",
        }
    ).encode()
    ack = decode_ack(raw)
    assert ack.status == "ERROR"
    assert ack.error == "topic not found"


def test_decode_offset_response_ok():
    assert decode_offset_response("OK offset=42") == 42


def test_decode_version_response_ok():
    assert decode_version_response("OK version=7") == 7


def test_decode_snapshot_response_ok():
    assert decode_snapshot_response('OK snapshot={"version":1,"payload":"x"}') == (
        '{"version":1,"payload":"x"}'
    )
    assert decode_snapshot_response("OK snapshot=null") is None


def test_strict_response_decoders_reject_legacy_values():
    with pytest.raises(ValueError, match="unexpected offset response"):
        decode_offset_response("42")
    with pytest.raises(ValueError, match="unexpected version response"):
        decode_version_response("7")
    with pytest.raises(ValueError, match="unexpected snapshot response"):
        decode_snapshot_response("NULL")
    with pytest.raises(ValueError, match="unexpected snapshot response"):
        decode_snapshot_response('{"version":1,"payload":"x"}')



def test_decode_offset_out_of_range_response():
    resp = "ERROR: OFFSET_OUT_OF_RANGE requested=3 earliest=10 latest=20"
    offset_range = decode_offset_out_of_range(resp)

    assert is_offset_out_of_range(resp)
    assert offset_range.requested == 3
    assert offset_range.earliest == 10
    assert offset_range.latest == 20


def test_decode_stream_control_offset_out_of_range():
    frame = (
        b"STREAM_CONTROL type=CLOSE reason=offset_out_of_range "
        b"offset=3 requested=3 earliest=10 latest=20"
    )
    control = decode_stream_control(frame)

    assert is_stream_control_frame(frame)
    assert control.type == "CLOSE"
    assert control.reason == "offset_out_of_range"
    assert control.offset == 3
    assert control.requested == 3
    assert control.earliest == 10
    assert control.latest == 20


def test_zero_length_frame_is_not_stream_control():
    assert not is_stream_control_frame(b"")


def test_commit_failure_classifiers():
    assert is_offset_regression("ERROR: offset_regression current=10 attempted=9")
    assert is_coordinator_failure("ERROR: GEN_MISMATCH expected=2 actual=1")
    assert is_coordinator_failure("ERROR: NOT_OWNER partition=0")
    assert is_coordinator_failure("ERROR: member_not_found member=m1")
    assert is_coordinator_failure("ERROR: NOT_COORDINATOR host=127.0.0.1 port=9001")
