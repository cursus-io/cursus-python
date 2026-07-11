import threading
from collections.abc import Callable, Iterator

from typing_extensions import Self

from cursus.compression.registry import CompressionRegistry
from cursus.config import ConsumerConfig
from cursus.connection.sync_conn import SyncConnection
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


class Consumer:
    def __init__(self, config: ConsumerConfig) -> None:
        self._config = config
        self._compression = CompressionRegistry()
        self._closed = False
        self._close_lock = threading.Lock()
        self._done = threading.Event()
        self._message_queue: list[Message] = []
        self._queue_lock = threading.Lock()
        self._queue_cond = threading.Condition(self._queue_lock)
        self._workers: list[threading.Thread] = []

        self._generation: int = 0
        self._member_id: str = config.consumer_id or ""
        self._assignments: list[int] = []
        self._offsets: dict[int, int] = {}
        self._committed_offsets: dict[int, int] = {}
        self._dirty_offsets: dict[int, int] = {}
        self._dirty_count = 0
        self._partition_leaders: dict[int, str] = {}
        self._leader_addr: str | None = None
        self._coordinator_addr: str | None = None

    def start(self, handler: Callable[[Message], None]) -> None:
        self._join_and_sync()
        self._start_heartbeat()
        self._start_commit_loop()
        self._start_partition_workers()

        while not self._done.is_set():
            with self._queue_cond:
                while len(self._message_queue) == 0 and not self._done.is_set():
                    self._queue_cond.wait(timeout=1.0)
                if self._done.is_set() and len(self._message_queue) == 0:
                    break
                msgs = list(self._message_queue)
                self._message_queue.clear()

            for msg in msgs:
                if self._done.is_set():
                    break
                handler(msg)
                self._mark_processed(msg)

    def __iter__(self) -> Iterator[Message]:
        self._join_and_sync()
        self._start_heartbeat()
        self._start_commit_loop()
        self._start_partition_workers()

        while not self._done.is_set():
            with self._queue_cond:
                while len(self._message_queue) == 0 and not self._done.is_set():
                    self._queue_cond.wait(timeout=1.0)
                if self._done.is_set() and len(self._message_queue) == 0:
                    return
                msgs = list(self._message_queue)
                self._message_queue.clear()

            for msg in msgs:
                if self._done.is_set():
                    return
                yield msg
                self._mark_processed(msg)

    def _connect_to_leader(self) -> SyncConnection:
        addrs = list(self._config.brokers)
        if self._leader_addr:
            addrs = [self._leader_addr] + [a for a in addrs if a != self._leader_addr]

        for addr in addrs:
            try:
                conn = SyncConnection(addr)
                conn.connect()
                self._leader_addr = addr
                return conn
            except ConnectionError:
                continue
        raise ConnectionError(f"failed to connect to any broker: {self._config.brokers}")

    def _send_command(self, cmd: str) -> str:
        for _attempt in range(3):
            conn = self._connect_to_leader()
            try:
                conn.write_frame(encode_message("", cmd))
                resp = conn.read_frame().decode()
            finally:
                conn.close()

            if "NOT_LEADER LEADER_IS" in resp:
                parts = resp.split()
                for i, p in enumerate(parts):
                    if p == "LEADER_IS" and i + 1 < len(parts):
                        self._leader_addr = parts[i + 1]
                        break
                continue
            return resp
        return resp

    def _find_coordinator(self) -> str:
        group = self._config.group_id or "default-group"
        resp = self._send_command(f"FIND_COORDINATOR group={group}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"find coordinator failed: {resp}")
        host, port = None, None
        for part in resp.split():
            if part.startswith("host="):
                host = part.split("=", 1)[1]
            elif part.startswith("port="):
                port = part.split("=", 1)[1]
        if not host or not port:
            raise ConnectionError(f"find coordinator: missing host/port: {resp}")
        return f"{host}:{port}"

    def _send_coordinator_command(self, cmd: str) -> str:
        for _attempt in range(3):
            addr = self._coordinator_addr or self._leader_addr or self._config.brokers[0]
            conn = SyncConnection(addr)
            try:
                conn.connect()
                conn.write_frame(encode_message("", cmd))
                resp = conn.read_frame().decode()
            except Exception as e:
                raise ConnectionError(f"coordinator command failed: {e}") from e
            finally:
                conn.close()

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

    def _fetch_metadata(self) -> None:
        resp = self._send_command(f"METADATA topic={self._config.topic}")
        if not resp.startswith("OK"):
            return
        for part in resp.split():
            if part.startswith("leaders="):
                addrs = part.split("=", 1)[1].split(",")
                for i, addr in enumerate(addrs):
                    self._partition_leaders[i] = addr.strip()

    def _connect_to_partition_leader(self, partition: int) -> SyncConnection:
        addr = self._partition_leaders.get(partition)
        if not addr:
            return self._connect_to_leader()
        try:
            conn = SyncConnection(addr)
            conn.connect()
            return conn
        except ConnectionError:
            return self._connect_to_leader()

    def _join_and_sync(self) -> None:
        try:
            self._coordinator_addr = self._find_coordinator()
        except Exception:
            pass

        cmd = CommandBuilder.join_group(
            self._config.topic,
            self._config.group_id or "default-group",
            self._member_id,
        )
        resp = self._send_coordinator_command(cmd)

        if not resp.startswith("OK"):
            raise ConnectionError(f"join group failed: {resp}")

        self._parse_join_response(resp)

        if not self._assignments:
            sync_cmd = CommandBuilder.sync_group(
                self._config.topic,
                self._config.group_id or "default-group",
                self._member_id,
                self._generation,
            )
            sync_resp = self._send_coordinator_command(sync_cmd)
            self._parse_sync_response(sync_resp)

        for pid in self._assignments:
            try:
                fetch_cmd = CommandBuilder.fetch_offset(
                    self._config.topic, pid, self._config.group_id or "default-group"
                )
                resp = self._send_coordinator_command(fetch_cmd)
                offset = decode_offset_response(resp)
                self._offsets[pid] = offset
                self._committed_offsets[pid] = offset
            except ValueError as exc:
                raise ConnectionError(f"fetch offset failed: {resp}") from exc

        try:
            self._fetch_metadata()
        except Exception:
            pass

    def _parse_join_response(self, resp: str) -> None:
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

    def _parse_sync_response(self, resp: str) -> None:
        if "[" in resp and "]" in resp:
            start = resp.index("[") + 1
            end = resp.index("]")
            parts = resp[start:end].replace(",", " ").split()
            self._assignments = [int(p.strip()) for p in parts if p.strip().isdigit()]

    def _start_heartbeat(self) -> None:
        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t.start()
        self._workers.append(t)

    def _heartbeat_loop(self) -> None:
        interval_s = self._config.heartbeat_interval_ms / 1000.0
        while not self._done.is_set():
            self._done.wait(timeout=interval_s)
            if self._done.is_set():
                break
            try:
                cmd = CommandBuilder.heartbeat(
                    self._config.topic,
                    self._config.group_id or "default-group",
                    self._member_id,
                    self._generation,
                )
                self._send_coordinator_command(cmd)
            except Exception:
                pass

    def _start_metadata_refresh(self) -> None:
        t = threading.Thread(target=self._metadata_refresh_loop, daemon=True)
        t.start()
        self._workers.append(t)

    def _metadata_refresh_loop(self) -> None:
        interval_s = self._config.metadata_refresh_interval_ms / 1000.0
        while not self._done.is_set():
            self._done.wait(timeout=interval_s)
            if self._done.is_set():
                break
            try:
                self._fetch_metadata()
            except Exception:
                pass

    def _start_partition_workers(self) -> None:
        self._start_metadata_refresh()
        for pid in self._assignments:
            t = threading.Thread(target=self._partition_poll_loop, args=(pid,), daemon=True)
            t.start()
            self._workers.append(t)

    def _partition_poll_loop(self, partition: int) -> None:
        while not self._done.is_set():
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

                conn = self._connect_to_partition_leader(partition)
                try:
                    conn.write_frame(encode_message("", cmd))
                    resp_data = conn.read_frame()

                    resp_str = resp_data.decode("utf-8", errors="replace")
                    if "NOT_LEADER LEADER_IS" in resp_str:
                        parts = resp_str.split()
                        for i, p in enumerate(parts):
                            if p == "LEADER_IS" and i + 1 < len(parts):
                                self._partition_leaders[partition] = parts[i + 1]
                                break
                        continue

                    resp_data = self._compression.decompress(
                        resp_data, self._config.compression_type
                    )
                    if len(resp_data) == 0:
                        continue

                    if len(resp_data) > 2:
                        try:
                            resp_str = resp_data.decode()
                            if "NOT_LEADER LEADER_IS" in resp_str:
                                parts = resp_str.split()
                                for i, p in enumerate(parts):
                                    if p == "LEADER_IS" and i + 1 < len(parts):
                                        self._partition_leaders[partition] = parts[i + 1]
                                        break
                                continue
                            if is_coordinator_failure(resp_str):
                                self._join_and_sync()
                                continue
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

                        messages, _, _ = decode_batch(resp_data)
                        if messages:
                            with self._queue_cond:
                                self._message_queue.extend(messages)
                                self._queue_cond.notify()
                            last = messages[-1]
                            self._offsets[partition] = last.offset + 1
                finally:
                    conn.close()
            except Exception:
                pass

            self._done.wait(timeout=0.5)

    def _resolve_offset_reset(self, offset_range: OffsetRange) -> int:
        policy = self._config.auto_offset_reset
        if policy == AutoOffsetReset.EARLIEST:
            return offset_range.earliest
        if policy == AutoOffsetReset.LATEST:
            return offset_range.latest
        self._done.set()
        raise ConnectionError(
            "offset out of range: "
            f"requested={offset_range.requested} earliest={offset_range.earliest} "
            f"latest={offset_range.latest}"
        )

    def _handle_stream_control(self, partition: int, control: StreamControl) -> None:
        if control.reason == "offset_out_of_range":
            if control.requested is None or control.earliest is None or control.latest is None:
                raise ConnectionError(f"stream control missing offset range: {control}")
            self._offsets[partition] = self._resolve_offset_reset(
                OffsetRange(control.requested, control.earliest, control.latest)
            )
            return
        if control.type == "CLOSE" and control.offset is not None:
            self._offsets[partition] = control.offset

    def _mark_processed(self, msg: Message) -> None:
        partition = msg.partition
        next_offset = msg.offset + 1
        if next_offset <= self._committed_offsets.get(partition, 0):
            return
        if self._config.immediate_commit:
            self._commit_offsets({partition: next_offset})
            return
        self._dirty_offsets[partition] = max(next_offset, self._dirty_offsets.get(partition, 0))
        self._dirty_count += 1
        if self._dirty_count >= self._config.commit_batch_size:
            self._commit_dirty_offsets()

    def _start_commit_loop(self) -> None:
        t = threading.Thread(target=self._commit_loop, daemon=True)
        t.start()
        self._workers.append(t)

    def _commit_loop(self) -> None:
        interval_s = self._config.auto_commit_interval_s
        while not self._done.is_set():
            self._done.wait(timeout=interval_s)
            if self._done.is_set():
                break
            try:
                self._commit_dirty_offsets()
            except Exception:
                pass

    def _commit_dirty_offsets(self) -> None:
        if not self._dirty_offsets:
            return
        offsets = dict(self._dirty_offsets)
        self._commit_offsets(offsets)
        for partition in offsets:
            self._dirty_offsets.pop(partition, None)
        self._dirty_count = 0

    def _commit_offsets(self, offsets: dict[int, int]) -> None:
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
        resp = self._send_coordinator_command(cmd)
        if resp.startswith("OK"):
            for partition, offset in offsets.items():
                if offset > self._committed_offsets.get(partition, 0):
                    self._committed_offsets[partition] = offset
            return
        if is_offset_regression(resp):
            raise ConnectionError(f"offset commit rejected: {resp}")
        if is_coordinator_failure(resp):
            self._join_and_sync()
            raise ConnectionError(f"coordinator rejected offset commit: {resp}")
        raise ConnectionError(f"offset commit failed: {resp}")

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        self._done.set()
        with self._queue_cond:
            self._queue_cond.notify_all()

        try:
            self._commit_dirty_offsets()
        except Exception:
            pass

        if self._member_id:
            try:
                cmd = CommandBuilder.leave_group(
                    self._config.topic,
                    self._config.group_id or "default-group",
                    self._member_id,
                )
                self._send_coordinator_command(cmd)
            except Exception:
                pass

        for t in self._workers:
            t.join(timeout=5.0)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
