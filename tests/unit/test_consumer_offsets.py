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


def test_coordinator_commit_failure_requests_rejoin_without_inline_join():
    consumer = make_consumer()
    consumer._generation = 7
    consumer._member_id = "member-1"
    consumer._send_coordinator_command = (  # type: ignore[method-assign]
        lambda _cmd: "ERROR: GEN_MISMATCH expected=8 actual=7"
    )

    def fail_join() -> None:
        raise AssertionError("commit path must not rejoin inline")

    consumer._join_and_sync = fail_join  # type: ignore[method-assign]

    with pytest.raises(ConnectionError, match="coordinator rejected offset commit"):
        consumer._commit_offsets({0: 11})

    assert consumer._rejoin_required.is_set()


def test_background_loops_start_once_across_rejoins():
    consumer = make_consumer()
    counts = {"heartbeat": 0, "commit": 0, "metadata": 0}

    def heartbeat() -> None:
        counts["heartbeat"] += 1

    def commit() -> None:
        counts["commit"] += 1

    def metadata() -> None:
        counts["metadata"] += 1

    consumer._start_heartbeat = heartbeat  # type: ignore[method-assign]
    consumer._start_commit_loop = commit  # type: ignore[method-assign]
    consumer._start_metadata_refresh = metadata  # type: ignore[method-assign]

    consumer._start_background_loops()
    consumer._start_background_loops()

    assert counts == {"heartbeat": 1, "commit": 1, "metadata": 1}


def test_close_marks_last_delivered_iterator_message():
    consumer = make_consumer()
    consumer._generation = 7
    consumer._member_id = "member-1"
    sent: list[str] = []

    def fake_send(cmd: str) -> str:
        sent.append(cmd)
        return "OK"

    consumer._send_coordinator_command = fake_send  # type: ignore[method-assign]
    consumer._last_delivered = Message(offset=12, seq_num=1, payload="ok", partition=0)

    consumer.close()

    assert consumer._committed_offsets[0] == 13
    assert sent[0] == (
        "COMMIT_OFFSET topic=orders partition=0 group=workers "
        "offset=13 generation=7 member=member-1"
    )


def test_async_commit_loop_is_singleton():
    import asyncio

    from cursus.async_consumer import AsyncConsumer

    async def scenario() -> None:
        consumer = AsyncConsumer(ConsumerConfig(topic="orders", group_id="workers"))
        consumer._ensure_commit_loop()
        first = consumer._commit_task
        consumer._ensure_commit_loop()
        assert consumer._commit_task is first
        await consumer.close()

    asyncio.run(scenario())


def test_restart_assignment_runs_outside_queue_lock():
    consumer = make_consumer()
    consumer._rejoin_required.set()
    consumer._done.set()
    consumer._start_background_loops = lambda: None  # type: ignore[method-assign]

    def restart() -> None:
        assert not consumer._queue_lock._is_owned()  # type: ignore[attr-defined]
        consumer._rejoin_required.clear()

    consumer._restart_assignment = restart  # type: ignore[method-assign]
    consumer.start(lambda _msg: None)


def test_partition_workers_are_not_added_to_background_workers():
    consumer = make_consumer()
    consumer._assignments = [0, 1]
    consumer._assignment_epoch = 1

    started = []

    class FakeThread:
        def __init__(self, target, args, daemon):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            started.append(self.args)

        def join(self, timeout=None) -> None:
            pass

    import threading

    original_thread = threading.Thread
    threading.Thread = FakeThread  # type: ignore[assignment]
    try:
        consumer._start_partition_workers()
    finally:
        threading.Thread = original_thread  # type: ignore[assignment]

    assert started == [(0, 1), (1, 1)]
    assert len(consumer._partition_workers) == 2
    assert consumer._workers == []


def test_heartbeat_coordinator_failure_requests_rejoin():
    consumer = make_consumer()
    consumer._generation = 7
    consumer._member_id = "member-1"
    sent: list[str] = []

    def fake_send(cmd: str) -> str:
        sent.append(cmd)
        return "ERROR: GEN_MISMATCH expected=8 actual=7"

    consumer._send_coordinator_command = fake_send  # type: ignore[method-assign]

    with pytest.raises(ConnectionError, match="coordinator rejected heartbeat"):
        consumer._send_heartbeat_once()

    assert consumer._rejoin_required.is_set()
    assert sent == ["HEARTBEAT topic=orders group=workers member=member-1 generation=7"]


def test_async_heartbeat_loop_is_singleton():
    import asyncio

    from cursus.async_consumer import AsyncConsumer

    async def scenario() -> None:
        consumer = AsyncConsumer(ConsumerConfig(topic="orders", group_id="workers"))

        async def fake_send(_cmd: str) -> str:
            return "OK"

        consumer._send_command = fake_send  # type: ignore[method-assign]
        consumer._ensure_heartbeat_loop()
        first = consumer._heartbeat_task
        consumer._ensure_heartbeat_loop()
        assert consumer._heartbeat_task is first
        await consumer.close()

    asyncio.run(scenario())


def test_async_heartbeat_coordinator_failure_requests_rejoin():
    import asyncio

    from cursus.async_consumer import AsyncConsumer

    async def scenario() -> None:
        consumer = AsyncConsumer(ConsumerConfig(topic="orders", group_id="workers"))
        consumer._generation = 7
        consumer._member_id = "member-1"
        sent: list[str] = []

        async def fake_send(cmd: str) -> str:
            sent.append(cmd)
            return "ERROR: member_not_found member=member-1"

        consumer._send_command = fake_send  # type: ignore[method-assign]

        with pytest.raises(ConnectionError, match="coordinator rejected heartbeat"):
            await consumer._send_heartbeat_once()

        assert consumer._rejoin_event.is_set()
        assert sent == ["HEARTBEAT topic=orders group=workers member=member-1 generation=7"]

    asyncio.run(scenario())
