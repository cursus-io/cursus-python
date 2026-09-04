import asyncio
import time
from types import TracebackType

from typing_extensions import Self

from cursus.config import ProducerConfig
from cursus.connection.async_conn import AsyncConnection
from cursus.errors import BrokerError, ConnectionError, ProducerClosedError, ProducerFencedError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_ack, is_terminal_producer_error
from cursus.protocol.encoder import encode_batch, encode_message
from cursus.types import Message


class AsyncProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._closed = False
        self._seq_counters = [0] * config.partitions
        self._rr = 0
        self._unique_ack_count = 0
        self._background_error: Exception | None = None
        self._in_flight = [0] * config.partitions
        self._buffers: list[list[Message]] = [[] for _ in range(config.partitions)]
        self._tasks: list[asyncio.Task[None]] = []
        self._partition_leaders: dict[int, str] = {}
        self._events: list[asyncio.Event] = [asyncio.Event() for _ in range(config.partitions)]
        self._producer_id = f"py-async-{id(self):x}"
        self._producer_epoch = int(time.time())
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._config.auto_create_topics:
            await self._create_topic()
        await self._fetch_metadata()
        for part in range(self._config.partitions):
            task = asyncio.create_task(self._partition_sender(part))
            self._tasks.append(task)

    async def _create_topic(self) -> None:
        async with AsyncConnection(
            self._config.brokers[0],
            tls_cert_path=self._config.tls_cert_path,
            tls_key_path=self._config.tls_key_path,
            compression_type=self._config.compression_type,
            principal=self._config.principal,
            auth_token=self._config.auth_token,
        ) as conn:
            cmd = CommandBuilder.create(self._config.topic, self._config.partitions)
            await conn.write_frame(encode_message("admin", cmd))
            response = (await conn.read_frame()).decode(errors="replace")
            if response != "OK" and not response.startswith("OK "):
                raise ConnectionError(f"topic auto-create failed: {response}")

    async def _fetch_metadata(self) -> None:
        for addr in self._config.brokers:
            try:
                async with AsyncConnection(
                    addr,
                    tls_cert_path=self._config.tls_cert_path,
                    tls_key_path=self._config.tls_key_path,
                    compression_type=self._config.compression_type,
                    principal=self._config.principal,
                    auth_token=self._config.auth_token,
                ) as conn:
                    await conn.write_frame(
                        encode_message("", f"METADATA topic={self._config.topic}")
                    )
                    resp = (await conn.read_frame()).decode()
                if not resp.startswith("OK"):
                    continue
                for part in resp.split():
                    if part.startswith("leaders="):
                        addrs = part.split("=", 1)[1].split(",")
                        for i, leader in enumerate(addrs):
                            leader = leader.strip()
                            if leader:
                                self._partition_leaders[i] = leader
                        return
            except Exception:
                continue

    @staticmethod
    def _fnv1a_32(data: bytes) -> int:
        h = 0x811C9DC5
        for b in data:
            h ^= b
            h = (h * 0x01000193) & 0xFFFFFFFF
        return h

    def _partition_for_key(self, key: str) -> int:
        return self._fnv1a_32(key.encode()) % self._config.partitions

    def _next_partition(self) -> int:
        part = self._rr % self._config.partitions
        self._rr += 1
        return part

    async def _connect_partition(self, partition: int) -> AsyncConnection:
        addrs = []
        leader = self._partition_leaders.get(partition)
        if leader:
            addrs.append(leader)
        addrs.extend(addr for addr in self._config.brokers if addr not in addrs)
        last_error: Exception | None = None
        for addr in addrs:
            try:
                conn = AsyncConnection(
                    addr,
                    tls_cert_path=self._config.tls_cert_path,
                    tls_key_path=self._config.tls_key_path,
                    compression_type=self._config.compression_type,
                    principal=self._config.principal,
                    auth_token=self._config.auth_token,
                )
                await conn.connect()
                return conn
            except Exception as exc:
                last_error = exc
                continue
        raise ConnectionError(f"failed to connect partition {partition}") from last_error

    async def send(self, payload: str, *, key: str = "") -> int:
        if self._closed:
            raise ProducerClosedError("producer is closed")

        part = self._partition_for_key(key) if key else self._next_partition()
        self._seq_counters[part] += 1
        seq = self._seq_counters[part]

        msg = Message(
            offset=0,
            seq_num=seq,
            payload=payload,
            key=key,
            producer_id=self._producer_id,
            epoch=self._producer_epoch,
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
            self._in_flight[part] += 1

            if conn is None:
                try:
                    conn = await self._connect_partition(part)
                except Exception:
                    conn = None
                    self._buffers[part] = batch + self._buffers[part]
                    self._in_flight[part] -= 1
                    self._events[part].set()
                    await asyncio.sleep(min(self._config.max_backoff_ms / 1000.0, 0.1))
                    continue

            try:
                data = encode_batch(
                    self._config.topic,
                    part,
                    self._config.acks.value,
                    self._config.idempotent,
                    batch,
                )
                await conn.write_frame(data)
                if self._config.acks.value == "0":
                    self._in_flight[part] -= 1
                    continue
                resp_data = await conn.read_frame()
                resp_text = resp_data.decode("utf-8", errors="replace")
                if "NOT_LEADER LEADER_IS" in resp_text:
                    parts = resp_text.split()
                    for i, token in enumerate(parts):
                        if token == "LEADER_IS" and i + 1 < len(parts):
                            self._partition_leaders[part] = parts[i + 1]
                            break
                    await conn.close()
                    conn = None
                    if self._config.idempotent:
                        self._buffers[part] = batch + self._buffers[part]
                    self._in_flight[part] -= 1
                    continue
                ack = decode_ack(resp_data)
                if ack.status == "OK":
                    self._unique_ack_count += len(batch)
                    self._in_flight[part] -= 1
                elif ack.error and is_terminal_producer_error(ack.error):
                    self._stop_event.set()
                    raise ProducerFencedError(ack.error)
                elif ack.error:
                    raise BrokerError(
                        ack.error_code,
                        ack.error_class,
                        ack.retryable,
                        ack.error,
                        ack.error_fields,
                    )
                else:
                    error = ack.error or f"broker returned status={ack.status}"
                    raise ConnectionError(f"broker rejected batch for partition {part}: {error}")
            except ProducerFencedError as exc:
                if conn is not None:
                    await conn.close()
                    conn = None
                self._record_background_error(exc)
                self._in_flight[part] -= 1
                continue
            except BrokerError as exc:
                if conn is not None:
                    await conn.close()
                    conn = None
                if exc.code.lower() == "not_leader" and exc.fields.get("leader"):
                    self._partition_leaders[part] = exc.fields["leader"]
                if exc.can_retry(idempotent=self._config.idempotent) and batch:
                    self._buffers[part] = batch + self._buffers[part]
                    self._events[part].set()
                    await asyncio.sleep(min(self._config.max_backoff_ms / 1000.0, 0.1))
                else:
                    self._record_background_error(exc)
                self._in_flight[part] -= 1
            except Exception as exc:
                if conn is not None:
                    await conn.close()
                    conn = None
                if batch and self._config.idempotent:
                    self._buffers[part] = batch + self._buffers[part]
                else:
                    self._record_background_error(exc)
                self._in_flight[part] -= 1
                if batch and self._config.idempotent:
                    self._events[part].set()
                    await asyncio.sleep(min(self._config.max_backoff_ms / 1000.0, 0.1))

        if conn is not None:
            await conn.close()

    @property
    def unique_ack_count(self) -> int:
        return self._unique_ack_count

    def _record_background_error(self, error: Exception) -> None:
        if getattr(self, "_background_error", None) is None:
            self._background_error = error

    def _raise_background_error(self) -> None:
        error = getattr(self, "_background_error", None)
        if error is not None:
            raise error

    async def flush(self) -> None:
        for event in self._events:
            event.set()
        timeout_s = self._config.flush_timeout_ms / 1000.0
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if all(len(buf) == 0 for buf in self._buffers) and all(
                count == 0 for count in self._in_flight
            ):
                self._raise_background_error()
                return
            await asyncio.sleep(0.01)
        self._raise_background_error()
        raise TimeoutError("producer flush timed out with buffered or in-flight messages")

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
