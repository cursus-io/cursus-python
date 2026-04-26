import asyncio
import time
from types import TracebackType

from typing_extensions import Self

from cursus.compression.registry import CompressionRegistry
from cursus.config import ProducerConfig
from cursus.connection.async_conn import AsyncConnection
from cursus.errors import ProducerClosedError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_ack
from cursus.protocol.encoder import encode_batch, encode_message
from cursus.types import Message


class AsyncProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._compression = CompressionRegistry()
        self._closed = False
        self._seq_counters = [0] * config.partitions
        self._rr = 0
        self._unique_ack_count = 0
        self._buffers: list[list[Message]] = [[] for _ in range(config.partitions)]
        self._tasks: list[asyncio.Task[None]] = []
        self._events: list[asyncio.Event] = [asyncio.Event() for _ in range(config.partitions)]
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        await self._create_topic()
        for part in range(self._config.partitions):
            task = asyncio.create_task(self._partition_sender(part))
            self._tasks.append(task)

    async def _create_topic(self) -> None:
        try:
            async with AsyncConnection(self._config.brokers[0]) as conn:
                cmd = CommandBuilder.create(self._config.topic, self._config.partitions)
                await conn.write_frame(encode_message("admin", cmd))
                await conn.read_frame()
        except Exception:
            pass

    def _next_partition(self) -> int:
        part = self._rr % self._config.partitions
        self._rr += 1
        return part

    async def send(self, payload: str, *, key: str = "") -> int:
        if self._closed:
            raise ProducerClosedError("producer is closed")

        part = self._next_partition()
        self._seq_counters[part] += 1
        seq = self._seq_counters[part]

        msg = Message(
            offset=0,
            seq_num=seq,
            payload=payload,
            key=key,
            producer_id=f"py-async-{id(self):x}",
            epoch=int(time.time()),
        )
        self._buffers[part].append(msg)
        self._events[part].set()
        return seq

    async def _partition_sender(self, part: int) -> None:
        linger_s = self._config.linger_ms / 1000.0
        conn: AsyncConnection | None = None

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._events[part].wait(), timeout=linger_s)
            except asyncio.TimeoutError:
                pass
            self._events[part].clear()

            if not self._buffers[part]:
                continue

            n = min(len(self._buffers[part]), self._config.batch_size)
            batch = self._buffers[part][:n]
            self._buffers[part] = self._buffers[part][n:]

            if conn is None:
                try:
                    conn = AsyncConnection(self._config.brokers[0])
                    await conn.connect()
                except Exception:
                    conn = None
                    continue

            try:
                data = encode_batch(
                    self._config.topic,
                    part,
                    self._config.acks.value,
                    self._config.idempotent,
                    batch,
                )
                data = self._compression.compress(data, self._config.compression_type)
                await conn.write_frame(data)
                resp_data = await conn.read_frame()
                ack = decode_ack(resp_data)
                if ack.status == "OK":
                    self._unique_ack_count += len(batch)
            except Exception:
                if conn is not None:
                    await conn.close()
                    conn = None

        if conn is not None:
            await conn.close()

    @property
    def unique_ack_count(self) -> int:
        return self._unique_ack_count

    async def flush(self) -> None:
        for event in self._events:
            event.set()
        timeout_s = self._config.flush_timeout_ms / 1000.0
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if all(len(buf) == 0 for buf in self._buffers):
                return
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        for event in self._events:
            event.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
