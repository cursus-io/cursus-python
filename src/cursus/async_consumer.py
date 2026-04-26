import asyncio
from collections.abc import AsyncIterator
from types import TracebackType

from typing_extensions import Self

from cursus.compression.registry import CompressionRegistry
from cursus.config import ConsumerConfig
from cursus.connection.async_conn import AsyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_batch
from cursus.protocol.encoder import encode_message
from cursus.types import ConsumerMode, Message


class AsyncConsumer:
    def __init__(self, config: ConsumerConfig) -> None:
        self._config = config
        self._compression = CompressionRegistry()
        self._closed = False
        self._stop_event = asyncio.Event()
        self._queue: asyncio.Queue[Message] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self._generation = 0
        self._member_id = config.consumer_id or ""
        self._assignments: list[int] = []
        self._offsets: dict[int, int] = {}
        self._leader_addr: str | None = None

    async def _connect(self) -> AsyncConnection:
        addrs = list(self._config.brokers)
        if self._leader_addr:
            addrs = [self._leader_addr] + [a for a in addrs if a != self._leader_addr]

        for addr in addrs:
            try:
                conn = AsyncConnection(addr)
                await conn.connect()
                self._leader_addr = addr
                return conn
            except ConnectionError:
                continue
        raise ConnectionError(f"failed to connect to any broker: {self._config.brokers}")

    async def _send_command(self, cmd: str) -> str:
        conn = await self._connect()
        try:
            await conn.write_frame(encode_message("", cmd))
            resp = await conn.read_frame()
            return resp.decode()
        finally:
            await conn.close()

    async def _join_and_sync(self) -> None:
        group = self._config.group_id or "default-group"
        cmd = CommandBuilder.join_group(self._config.topic, group, self._member_id)
        resp = await self._send_command(cmd)

        if not resp.startswith("OK"):
            raise ConnectionError(f"join group failed: {resp}")

        for part in resp.split():
            if part.startswith("generation="):
                self._generation = int(part.split("=", 1)[1])
            elif part.startswith("member="):
                self._member_id = part.split("=", 1)[1]

        if "assignments=" in resp and "[" in resp and "]" in resp:
            start = resp.index("[") + 1
            end = resp.index("]")
            parts = resp[start:end].replace(",", " ").split()
            self._assignments = [int(p.strip()) for p in parts if p.strip().isdigit()]

        if not self._assignments:
            sync_cmd = CommandBuilder.sync_group(
                self._config.topic, group, self._member_id, self._generation
            )
            sync_resp = await self._send_command(sync_cmd)
            if "[" in sync_resp and "]" in sync_resp:
                start = sync_resp.index("[") + 1
                end = sync_resp.index("]")
                parts = sync_resp[start:end].replace(",", " ").split()
                self._assignments = [int(p.strip()) for p in parts if p.strip().isdigit()]

        for pid in self._assignments:
            try:
                fetch_cmd = CommandBuilder.fetch_offset(self._config.topic, pid, group)
                resp = await self._send_command(fetch_cmd)
                self._offsets[pid] = int(resp.strip())
            except (ValueError, ConnectionError):
                self._offsets[pid] = 0

    async def start(self) -> None:
        await self._join_and_sync()
        for pid in self._assignments:
            task = asyncio.create_task(self._poll_loop(pid))
            self._tasks.append(task)

    async def _poll_loop(self, partition: int) -> None:
        while not self._stop_event.is_set():
            offset = self._offsets.get(partition, 0)
            try:
                group = self._config.group_id or "default-group"
                if self._config.mode == ConsumerMode.STREAMING:
                    cmd = CommandBuilder.stream(
                        self._config.topic, partition, group,
                        self._member_id, self._generation, offset=offset,
                    )
                else:
                    cmd = CommandBuilder.consume(
                        self._config.topic, partition, offset,
                        self._member_id, group=group, generation=self._generation,
                    )

                conn = await self._connect()
                try:
                    await conn.write_frame(encode_message("", cmd))
                    resp_data = await conn.read_frame()
                    resp_data = self._compression.decompress(
                        resp_data, self._config.compression_type
                    )
                    if len(resp_data) > 2:
                        messages, _, _ = decode_batch(resp_data)
                        for msg in messages:
                            await self._queue.put(msg)
                        if messages:
                            self._offsets[partition] = messages[-1].offset + 1
                finally:
                    await conn.close()
            except Exception:
                pass

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

    def __aiter__(self) -> AsyncIterator[Message]:
        return self

    async def __anext__(self) -> Message:
        while not self._closed:
            try:
                return await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self._closed:
                    raise StopAsyncIteration
                continue
        raise StopAsyncIteration

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            group = self._config.group_id or "default-group"
            cmd = CommandBuilder.leave_group(self._config.topic, group, self._member_id)
            await self._send_command(cmd)
        except Exception:
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
