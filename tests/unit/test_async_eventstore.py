import asyncio

from cursus.async_eventstore import AsyncEventStore


class _TrackedAsyncLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._attempts = 0
        self.second_attempted = asyncio.Event()

    async def __aenter__(self):
        self._attempts += 1
        if self._attempts == 2:
            self.second_attempted.set()
        await self._lock.acquire()
        return self

    async def __aexit__(self, *args):
        self._lock.release()


class _BlockingAsyncConnection:
    def __init__(self):
        self.writes = []
        self.batch_read_started = asyncio.Event()
        self.release_batch = asyncio.Event()
        self._reads = 0

    async def write_frame(self, data):
        self.writes.append(data)

    async def read_frame(self):
        self._reads += 1
        if self._reads == 1:
            return b'{"status":"OK","snapshot":null}'
        if self._reads == 2:
            self.batch_read_started.set()
            await asyncio.wait_for(self.release_batch.wait(), timeout=2)
            return b""
        return b"OK"

    async def close(self):
        pass


async def test_read_stream_serializes_both_response_frames():
    es = AsyncEventStore("localhost:9000", "orders", "producer-1")
    conn = _BlockingAsyncConnection()
    lock = _TrackedAsyncLock()
    es._conn = conn
    es._request_lock = lock

    reader = asyncio.create_task(es.read_stream("order-1"))
    await asyncio.wait_for(conn.batch_read_started.wait(), timeout=1)
    creator = asyncio.create_task(es.create_topic(1))
    await asyncio.wait_for(lock.second_attempted.wait(), timeout=1)

    assert len(conn.writes) == 1

    conn.release_batch.set()
    await asyncio.gather(reader, creator)
    assert len(conn.writes) == 2
