import pytest

from cursus.config import ConsumerConfig
from cursus.consumer import Consumer
from cursus.errors import ConnectionError
from cursus.types import AutoOffsetReset, Message, OffsetRange, StreamControl


def make_consumer(
    auto_offset_reset: AutoOffsetReset = AutoOffsetReset.EARLIEST,
    *,
    immediate_commit: bool = False,
    commit_batch_size: int = 100,
) -> Consumer:
    return Consumer(
        ConsumerConfig(
            topic="orders",
            group_id="workers",
            auto_offset_reset=auto_offset_reset,
            immediate_commit=immediate_commit,
            commit_batch_size=commit_batch_size,
        )
    )


def test_auto_offset_reset_earliest_and_latest():
    offset_range = OffsetRange(requested=3, earliest=10, latest=20)

    assert make_consumer(AutoOffsetReset.EARLIEST)._resolve_offset_reset(offset_range) == 10
    assert make_consumer(AutoOffsetReset.LATEST)._resolve_offset_reset(offset_range) == 20


def test_auto_offset_reset_error_fails_closed():
    consumer = make_consumer(AutoOffsetReset.ERROR)

    with pytest.raises(ConnectionError, match="offset out of range"):
        consumer._resolve_offset_reset(OffsetRange(requested=3, earliest=10, latest=20))


def test_stream_control_offset_out_of_range_uses_reset_policy():
    consumer = make_consumer(AutoOffsetReset.LATEST)

    consumer._handle_stream_control(
        2,
        StreamControl(
            type="CLOSE",
            reason="offset_out_of_range",
            earliest=10,
            latest=20,
        ),
    )

    assert consumer._offsets[2] == 20


def test_stream_control_graceful_close_updates_next_fetch_offset():
    consumer = make_consumer()

    consumer._handle_stream_control(1, StreamControl(type="CLOSE", offset=42))

    assert consumer._offsets[1] == 42


def test_commit_success_advances_local_committed_offset():
    consumer = make_consumer(immediate_commit=True)
    sent: list[str] = []

    def fake_send(cmd: str) -> str:
        sent.append(cmd)
        return "OK"

    consumer._generation = 7
    consumer._member_id = "member-1"
    consumer._send_coordinator_command = fake_send  # type: ignore[method-assign]

    consumer._mark_processed(Message(offset=41, seq_num=1, payload="ok", partition=2))

    assert consumer._committed_offsets[2] == 42
    assert sent == [
        "COMMIT_OFFSET topic=orders partition=2 group=workers "
        "offset=42 generation=7 member=member-1"
    ]


def test_offset_regression_commit_does_not_rewind_local_state():
    consumer = make_consumer()
    consumer._generation = 7
    consumer._member_id = "member-1"
    consumer._committed_offsets[2] = 50
    consumer._send_coordinator_command = (  # type: ignore[method-assign]
        lambda _cmd: "ERROR: offset_regression current=50 attempted=42"
    )

    with pytest.raises(ConnectionError, match="offset commit rejected"):
        consumer._commit_offsets({2: 42})

    assert consumer._committed_offsets[2] == 50


def test_batch_commit_success_advances_multiple_partitions():
    consumer = make_consumer(commit_batch_size=2)
    sent: list[str] = []

    def fake_send(cmd: str) -> str:
        sent.append(cmd)
        return "OK"

    consumer._generation = 7
    consumer._member_id = "member-1"
    consumer._send_coordinator_command = fake_send  # type: ignore[method-assign]

    consumer._mark_processed(Message(offset=10, seq_num=1, payload="a", partition=0))
    consumer._mark_processed(Message(offset=20, seq_num=1, payload="b", partition=1))

    assert consumer._committed_offsets[0] == 11
    assert consumer._committed_offsets[1] == 21
    assert sent == [
        "BATCH_COMMIT topic=orders group=workers member=member-1 generation=7 P0:11,P1:21"
    ]
