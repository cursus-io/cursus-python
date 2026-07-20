import threading

import pytest

from cursus.eventstore import EventStore


def test_parse_append_response_strict_contract():
    es = EventStore("localhost:9000", "orders", "producer-1")

    result = es._parse_append_response("OK version=3 offset=42 partition=1")

    assert result.version == 3
    assert result.offset == 42
    assert result.partition == 1


@pytest.mark.parametrize("resp", ["OK version=3", "3", "", "ERROR: version_conflict"])
def test_parse_append_response_rejects_non_contract_responses(resp):
    es = EventStore("localhost:9000", "orders", "producer-1")

    with pytest.raises(ValueError):
        es._parse_append_response(resp)


class _TrackedThreadLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._attempts = 0
        self.second_attempted = threading.Event()

    def __enter__(self):
        self._attempts += 1
        if self._attempts == 2:
            self.second_attempted.set()
        self._lock.acquire()
        return self

    def __exit__(self, *args):
        self._lock.release()


class _BlockingSyncConnection:
    def __init__(self):
        self.writes = []
        self.batch_read_started = threading.Event()
        self.release_batch = threading.Event()
        self._reads = 0

    def write_frame(self, data):
        self.writes.append(data)

    def read_frame(self):
        self._reads += 1
        if self._reads == 1:
            return b'{"status":"OK","snapshot":null}'
        if self._reads == 2:
            self.batch_read_started.set()
            if not self.release_batch.wait(timeout=2):
                raise TimeoutError("batch release was not signalled")
            return b""
        return b"OK"

    def close(self):
        pass


def test_read_stream_serializes_both_response_frames():
    es = EventStore("localhost:9000", "orders", "producer-1")
    conn = _BlockingSyncConnection()
    lock = _TrackedThreadLock()
    es._conn = conn
    es._request_lock = lock
    failures = []

    def read_stream():
        try:
            es.read_stream("order-1")
        except Exception as exc:
            failures.append(exc)

    def create_topic():
        try:
            es.create_topic(1)
        except Exception as exc:
            failures.append(exc)

    reader = threading.Thread(target=read_stream)
    creator = threading.Thread(target=create_topic)
    reader.start()
    assert conn.batch_read_started.wait(timeout=1)
    creator.start()
    assert lock.second_attempted.wait(timeout=1)

    assert len(conn.writes) == 1

    conn.release_batch.set()
    reader.join(timeout=2)
    creator.join(timeout=2)
    assert not reader.is_alive()
    assert not creator.is_alive()
    assert not failures
    assert len(conn.writes) == 2


def test_retry_sequence_holds_request_lock(monkeypatch):
    es = EventStore("localhost:9000", "orders", "producer-1")
    lock = _TrackedThreadLock()
    es._request_lock = lock
    retry_started = threading.Event()
    release_retry = threading.Event()
    calls = []
    failures = []

    def send_once(cmd):
        calls.append(cmd)
        if cmd.startswith("CREATE") and sum(call.startswith("CREATE") for call in calls) == 1:
            return "ERROR: topic_not_found"
        return "OK"

    def retry_sleep(_delay):
        retry_started.set()
        if not release_retry.wait(timeout=2):
            raise TimeoutError("retry release was not signalled")

    monkeypatch.setattr(es, "_send_command_once", send_once)
    monkeypatch.setattr("cursus.eventstore.time.sleep", retry_sleep)

    def create_topic():
        try:
            es.create_topic(1)
        except Exception as exc:
            failures.append(exc)

    def ping():
        try:
            es._send_command("PING", retries=0)
        except Exception as exc:
            failures.append(exc)

    creator = threading.Thread(target=create_topic)
    other = threading.Thread(target=ping)
    creator.start()
    assert retry_started.wait(timeout=1)
    other.start()
    assert lock.second_attempted.wait(timeout=1)

    assert len(calls) == 1
    assert calls[0].startswith("CREATE")

    release_retry.set()
    creator.join(timeout=2)
    other.join(timeout=2)
    assert not creator.is_alive()
    assert not other.is_alive()
    assert not failures
    assert [cmd.split()[0] for cmd in calls] == ["CREATE", "CREATE", "PING"]
