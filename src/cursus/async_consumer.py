import asyncio
from collections.abc import AsyncIterator
from types import TracebackType

from typing_extensions import Self

from cursus.compression.registry import CompressionRegistry
from cursus.config import ConsumerConfig
from cursus.connection.async_conn import AsyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import (
    decode_batch,
    decode_offset_out_of_range,
    decode_offset_response,
    decode_stream_control,
    is_coordinator_failure,
    is_offset_out_of_range,
    is_offset_regression,
    is_stream_control_frame,
)
from cursus.protocol.encoder import encode_message
from cursus.types import AutoOffsetReset, ConsumerMode, Message, OffsetRange, StreamControl


class AsyncConsumer:
    def __init__(self, config: ConsumerConfig) -> None:
        self._config = config
        self._compression = CompressionRegistry()
        self._closed = False
        self._stop_event = asyncio.Event()
        self._queue: asyncio.Queue[Message] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self._partition_tasks: list[asyncio.Task[None]] = []
        self._commit_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._rejoin_task: asyncio.Task[None] | None = None
        self._rejoin_event = asyncio.Event()
        self._generation = 0
        self._member_id = config.consumer_id or ""
        self._assignments: list[int] = []
        self._offsets: dict[int, int] = {}
        self._committed_offsets: dict[int, int] = {}
        self._dirty_offsets: dict[int, int] = {}
        self._dirty_count = 0
        self._last_delivered: Message | None = None
        self._leader_addr: str | None = None
        self._coordinator_addr: str | None = None
        self._partition_leaders: dict[int, str] = {}

    async def _connect(self, preferred: str | None = None) -> AsyncConnection:
        addrs = list(self._config.brokers)
        if preferred:
            addrs = [preferred] + [a for a in addrs if a != preferred]
        elif self._leader_addr:
            addrs = [self._leader_addr] + [a for a in addrs if a != self._leader_addr]

        for addr in addrs:
            try:
                conn = AsyncConnection(addr)
                await conn.connect()
                if preferred is None:
                    self._leader_addr = addr
                return conn
            except ConnectionError:
                continue
        raise ConnectionError(f"failed to connect to any broker: {self._config.brokers}")

    async def _connect_to_partition_leader(self, partition: int) -> AsyncConnection:
        return await self._connect(self._partition_leaders.get(partition))

    async def _send_command(self, cmd: str) -> str:
        for _attempt in range(3):
            conn = await self._connect(self._coordinator_addr)
            try:
                await conn.write_frame(encode_message("", cmd))
                resp = (await conn.read_frame()).decode()
            finally:
                await conn.close()
            if "NOT_COORDINATOR" in resp:
                host, port = None, None
                for part in resp.split():
                    if part.startswith("host="):
                        host = part.split("=", 1)[1]
                    elif part.startswith("port="):
                        port = part.split("=", 1)[1]
                if host and port:
                    self._coordinator_addr = f"{host}:{port}"
                    continue
            return resp
        return resp

    async def _fetch_metadata(self) -> None:
        resp = await self._send_command(f"METADATA topic={self._config.topic}")
        if not resp.startswith("OK"):
            return
        for part in resp.split():
            if part.startswith("leaders="):
                addrs = part.split("=", 1)[1].split(",")
                for i, addr in enumerate(addrs):
                    addr = addr.strip()
                    if addr:
                        self._partition_leaders[i] = addr

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
                offset = decode_offset_response(resp)
                self._offsets[pid] = offset
                self._committed_offsets[pid] = offset
            except ValueError as exc:
                raise ConnectionError(f"fetch offset failed: {resp}") from exc

        try:
            await self._fetch_metadata()
        except Exception:
            pass

    async def start(self) -> None:
        await self._join_and_sync()
        self._ensure_heartbeat_loop()
        self._ensure_commit_loop()
        self._start_partition_tasks()
        if self._rejoin_task is None or self._rejoin_task.done():
            self._rejoin_task = asyncio.create_task(self._rejoin_loop())
            self._tasks.append(self._rejoin_task)

    def _ensure_commit_loop(self) -> None:
        if self._commit_task is None or self._commit_task.done():
            self._commit_task = asyncio.create_task(self._commit_loop())
            self._tasks.append(self._commit_task)

    def _ensure_heartbeat_loop(self) -> None:
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._tasks.append(self._heartbeat_task)

    def _start_partition_tasks(self) -> None:
        for pid in self._assignments:
            task = asyncio.create_task(self._poll_loop(pid))
            self._partition_tasks.append(task)
            self._tasks.append(task)

    async def _stop_partition_tasks(self) -> None:
        tasks = list(self._partition_tasks)
        self._partition_tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _request_rejoin(self) -> None:
        self._rejoin_event.set()

    async def _rejoin_loop(self) -> None:
        backoff_s = 1.0
        while not self._stop_event.is_set():
            await self._rejoin_event.wait()
            if self._stop_event.is_set():
                return
            self._rejoin_event.clear()
            await self._stop_partition_tasks()
            while not self._queue.empty():
                self._queue.get_nowait()
            try:
                await self._join_and_sync()
            except Exception:
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, self._config.max_backoff_ms / 1000.0)
                self._rejoin_event.set()
                continue
            backoff_s = 1.0
            self._start_partition_tasks()

    async def _poll_loop(self, partition: int) -> None:
        while not self._stop_event.is_set():
            offset = self._offsets.get(partition, 0)
            try:
                group = self._config.group_id or "default-group"
                if self._config.mode == ConsumerMode.STREAMING:
                    cmd = CommandBuilder.stream(
                        self._config.topic,
                        partition,
                        group,
                        self._member_id,
                        self._generation,
                        offset=offset,
                    )
                else:
                    cmd = CommandBuilder.consume(
                        self._config.topic,
                        partition,
                        offset,
                        self._member_id,
                        group=group,
                        generation=self._generation,
                    )

                conn = await self._connect_to_partition_leader(partition)
                try:
                    await conn.write_frame(encode_message("", cmd))
                    resp_data = await conn.read_frame()
                    resp_text = resp_data.decode("utf-8", errors="replace")
                    if "NOT_LEADER LEADER_IS" in resp_text:
                        parts = resp_text.split()
                        for i, token in enumerate(parts):
                            if token == "LEADER_IS" and i + 1 < len(parts):
                                self._partition_leaders[partition] = parts[i + 1]
                                break
                        continue
                    resp_data = self._compression.decompress(
                        resp_data, self._config.compression_type
                    )
                    if len(resp_data) == 0:
                        continue
                    try:
                        resp_str = resp_data.decode()
                        if is_coordinator_failure(resp_str):
                            self._request_rejoin()
                            return
                        if is_offset_out_of_range(resp_str):
                            self._offsets[partition] = self._resolve_offset_reset(
                                decode_offset_out_of_range(resp_str)
                            )
                            continue
                    except UnicodeDecodeError:
                        pass
                    if is_stream_control_frame(resp_data):
                        control = decode_stream_control(resp_data)
                        self._handle_stream_control(partition, control)
                        continue
                    if len(resp_data) > 2:
                        messages, _, _ = decode_batch(resp_data)
                        if self._rejoin_event.is_set() or self._stop_event.is_set():
                            return
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
        if self._last_delivered is not None:
            await self._mark_processed(self._last_delivered)
            self._last_delivered = None
        while not self._closed:
            try:
                msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                self._last_delivered = msg
                return msg
            except asyncio.TimeoutError:
                if self._closed:
                    raise StopAsyncIteration
                continue
        raise StopAsyncIteration

    def _resolve_offset_reset(self, offset_range: OffsetRange) -> int:
        policy = self._config.auto_offset_reset
        if policy == AutoOffsetReset.EARLIEST:
            return offset_range.earliest
        if policy == AutoOffsetReset.LATEST:
            return offset_range.latest
        self._stop_event.set()
        raise ConnectionError(
            "offset out of range: "
            f"requested={offset_range.requested} earliest={offset_range.earliest} "
            f"latest={offset_range.latest}"
        )

    def _handle_stream_control(self, partition: int, control: StreamControl) -> None:
        if control.reason == "offset_out_of_range":
            if control.earliest is None or control.latest is None:
                raise ConnectionError(f"stream control missing offset range: {control}")
            self._offsets[partition] = self._resolve_offset_reset(
                OffsetRange(
                    control.requested or self._offsets.get(partition, 0),
                    control.earliest,
                    control.latest,
                )
            )
            return
        if control.type == "CLOSE" and control.offset is not None:
            self._offsets[partition] = control.offset

    async def _mark_processed(self, msg: Message) -> None:
        partition = msg.partition
        next_offset = msg.offset + 1
        if next_offset <= self._committed_offsets.get(partition, 0):
            return
        if self._config.immediate_commit:
            await self._commit_offsets({partition: next_offset})
            return
        self._dirty_offsets[partition] = max(next_offset, self._dirty_offsets.get(partition, 0))
        self._dirty_count += 1
        if self._dirty_count >= self._config.commit_batch_size:
            await self._commit_dirty_offsets()

    async def _heartbeat_loop(self) -> None:
        interval_s = self._config.heartbeat_interval_ms / 1000.0
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_s)
                return
            except asyncio.TimeoutError:
                try:
                    await self._send_heartbeat_once()
                except Exception:
                    pass

    async def _send_heartbeat_once(self) -> None:
        group = self._config.group_id or "default-group"
        cmd = CommandBuilder.heartbeat(
            self._config.topic,
            group,
            self._member_id,
            self._generation,
        )
        resp = await self._send_command(cmd)
        if resp.startswith("OK"):
            return
        if is_coordinator_failure(resp):
            self._request_rejoin()
            raise ConnectionError(f"coordinator rejected heartbeat: {resp}")
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"heartbeat failed: {resp}")

    async def _commit_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._config.auto_commit_interval_s
                )
            except asyncio.TimeoutError:
                try:
                    await self._commit_dirty_offsets()
                except Exception:
                    pass

    async def _commit_dirty_offsets(self) -> None:
        if not self._dirty_offsets:
            return
        offsets = dict(self._dirty_offsets)
        await self._commit_offsets(offsets)
        for partition in offsets:
            self._dirty_offsets.pop(partition, None)
        self._dirty_count = 0

    async def _commit_offsets(self, offsets: dict[int, int]) -> None:
        if not offsets:
            return
        group = self._config.group_id or "default-group"
        if len(offsets) == 1:
            partition, offset = next(iter(offsets.items()))
            cmd = CommandBuilder.commit_offset(
                self._config.topic, group, partition, offset, self._generation, self._member_id
            )
        else:
            cmd = CommandBuilder.batch_commit(
                self._config.topic, group, self._member_id, self._generation, offsets
            )
        resp = await self._send_command(cmd)
        if resp.startswith("OK"):
            for partition, offset in offsets.items():
                if offset > self._committed_offsets.get(partition, 0):
                    self._committed_offsets[partition] = offset
            return
        if is_offset_regression(resp):
            raise ConnectionError(f"offset commit rejected: {resp}")
        if is_coordinator_failure(resp):
            self._request_rejoin()
            raise ConnectionError(f"coordinator rejected offset commit: {resp}")
        raise ConnectionError(f"offset commit failed: {resp}")

    async def close(self) -> None:
        if self._closed:
            return
        if self._last_delivered is not None:
            await self._mark_processed(self._last_delivered)
            self._last_delivered = None
        try:
            await self._commit_dirty_offsets()
        except Exception:
            pass
        self._closed = True
        self._stop_event.set()
        self._rejoin_event.set()
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
