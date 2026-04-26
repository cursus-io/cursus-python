from cursus.types import (
    AckResponse,
    Acks,
    AppendResult,
    ConsumerMode,
    Event,
    Message,
    Snapshot,
    StreamData,
    StreamEvent,
)


def test_message_defaults():
    msg = Message(offset=0, seq_num=1, payload="hello")
    assert msg.key == ""
    assert msg.producer_id == ""
    assert msg.epoch == 0
    assert msg.event_type == ""
    assert msg.schema_version == 0
    assert msg.aggregate_version == 0
    assert msg.metadata == ""


def test_ack_response_defaults():
    ack = AckResponse(
        status="OK",
        last_offset=100,
        producer_epoch=1,
        producer_id="p1",
        seq_start=1,
        seq_end=10,
    )
    assert ack.leader == ""
    assert ack.error == ""


def test_acks_enum_values():
    assert Acks.NONE == "0"
    assert Acks.ONE == "1"
    assert Acks.ALL == "-1"


def test_consumer_mode_enum_values():
    assert ConsumerMode.POLLING == "polling"
    assert ConsumerMode.STREAMING == "streaming"


def test_event_defaults():
    event = Event(type="OrderCreated", payload='{"a":1}')
    assert event.schema_version == 1
    assert event.metadata == ""


def test_stream_data_structure():
    snap = Snapshot(version=5, payload='{"state":"x"}')
    se = StreamEvent(version=1, offset=0, payload="data")
    sd = StreamData(snapshot=snap, events=[se])
    assert sd.snapshot.version == 5
    assert len(sd.events) == 1


def test_append_result():
    ar = AppendResult(version=3, offset=42, partition=1)
    assert ar.version == 3
