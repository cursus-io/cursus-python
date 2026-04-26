import threading
import time

from typing_extensions import Self

from cursus.compression.registry import CompressionRegistry
from cursus.config import ProducerConfig
from cursus.connection.sync_conn import SyncConnection
from cursus.errors import ProducerClosedError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_ack
from cursus.protocol.encoder import encode_batch, encode_message
from cursus.types import Message


class _PartitionBuffer:
    def __init__(self) -> None:
        self.msgs: list[Message] = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.closed = False


class Producer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._compression = CompressionRegistry()
        self._closed = False
        self._close_lock = threading.Lock()
        self._done = threading.Event()

        self._seq_counters: list[int] = [0] * config.partitions
        self._seq_locks: list[threading.Lock] = [threading.Lock() for _ in range(config.partitions)]
        self._rr = 0
        self._rr_lock = threading.Lock()

        self._unique_ack_count = 0
        self._ack_lock = threading.Lock()

        self._in_flight = [0] * config.partitions
        self._in_flight_lock = threading.Lock()

        self._buffers = [_PartitionBuffer() for _ in range(config.partitions)]
        self._senders: list[threading.Thread] = []
        self._partition_leaders: dict[int, str] = {}

        self._create_topic()
        try:
            self._fetch_metadata()
        except Exception:
            pass
        self._start_senders()

    def _create_topic(self) -> None:
        try:
            conn = SyncConnection(self._config.brokers[0], timeout_ms=self._config.write_timeout_ms)
            conn.connect()
            try:
                cmd = CommandBuilder.create(self._config.topic, self._config.partitions)
                conn.write_frame(encode_message("admin", cmd))
                conn.read_frame()
            finally:
                conn.close()
        except Exception:
            pass

    def _fetch_metadata(self) -> None:
        for addr in self._config.brokers:
            try:
                conn = SyncConnection(addr)
                conn.connect()
                conn.write_frame(encode_message("", f"METADATA topic={self._config.topic}"))
                resp = conn.read_frame().decode()
                conn.close()
                if not resp.startswith("OK"):
                    continue
                for part in resp.split():
                    if part.startswith("leaders="):
                        addrs = part.split("=", 1)[1].split(",")
                        for i, a in enumerate(addrs):
                            a = a.strip()
                            if a:
                                self._partition_leaders[i] = a
                        return
            except Exception:
                continue

    def _start_senders(self) -> None:
        for part in range(self._config.partitions):
            t = threading.Thread(target=self._partition_sender, args=(part,), daemon=True)
            t.start()
            self._senders.append(t)

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
        with self._rr_lock:
            part = self._rr % self._config.partitions
            self._rr += 1
            return part

    def _next_seq_num(self, partition: int) -> int:
        with self._seq_locks[partition]:
            self._seq_counters[partition] += 1
            return self._seq_counters[partition]

    def send(self, payload: str, *, key: str = "") -> int:
        if self._closed:
            raise ProducerClosedError("producer is closed")

        part = self._partition_for_key(key) if key else self._next_partition()
        buf = self._buffers[part]
        seq = self._next_seq_num(part)

        msg = Message(
            offset=0,
            seq_num=seq,
            payload=payload,
            key=key,
            producer_id=f"py-{id(self):x}",
            epoch=int(time.time()),
        )

        with buf.cond:
            if buf.closed:
                raise ProducerClosedError("partition buffer closed")
            if len(buf.msgs) >= self._config.buffer_size:
                raise BufferError(f"partition {part} buffer full")
            buf.msgs.append(msg)
            buf.cond.notify()

        return seq

    def _partition_sender(self, part: int) -> None:
        buf = self._buffers[part]
        linger_s = self._config.linger_ms / 1000.0
        conn: SyncConnection | None = None

        while not self._done.is_set():
            with buf.cond:
                while len(buf.msgs) == 0 and not buf.closed and not self._done.is_set():
                    buf.cond.wait(timeout=linger_s)

                if (buf.closed or self._done.is_set()) and len(buf.msgs) == 0:
                    break

                n = min(len(buf.msgs), self._config.batch_size)
                batch = buf.msgs[:n]
                buf.msgs = buf.msgs[n:]

            if not batch:
                continue

            with self._in_flight_lock:
                self._in_flight[part] += 1

            sent = False
            backoff_ms = 100
            for attempt in range(self._config.max_retries + 1):
                if self._done.is_set():
                    break

                if conn is None:
                    try:
                        broker = self._partition_leaders.get(part, self._config.brokers[0])
                        conn = SyncConnection(broker, timeout_ms=self._config.write_timeout_ms)
                        conn.connect()
                    except Exception:
                        conn = None
                        if attempt < self._config.max_retries:
                            time.sleep(backoff_ms / 1000.0)
                            backoff_ms = min(backoff_ms * 2, self._config.max_backoff_ms)
                        continue

                try:
                    self._send_batch(conn, part, batch)
                    sent = True
                    break
                except Exception:
                    if conn is not None:
                        conn.close()
                        conn = None
                    if attempt < self._config.max_retries:
                        time.sleep(backoff_ms / 1000.0)
                        backoff_ms = min(backoff_ms * 2, self._config.max_backoff_ms)

            with self._in_flight_lock:
                self._in_flight[part] -= 1

            if not sent and batch:
                with buf.cond:
                    buf.msgs = batch + buf.msgs
                    buf.cond.notify()

        if conn is not None:
            conn.close()

    def _send_batch(self, conn: SyncConnection, partition: int, batch: list[Message]) -> None:
        data = encode_batch(
            self._config.topic,
            partition,
            self._config.acks.value,
            self._config.idempotent,
            batch,
        )
        data = self._compression.compress(data, self._config.compression_type)
        conn.write_frame(data)
        resp_data = conn.read_frame()

        ack = decode_ack(resp_data)
        if ack.status == "OK":
            with self._ack_lock:
                self._unique_ack_count += len(batch)

    @property
    def unique_ack_count(self) -> int:
        with self._ack_lock:
            return self._unique_ack_count

    def flush(self) -> None:
        for buf in self._buffers:
            with buf.cond:
                buf.cond.notify_all()

        timeout_s = self._config.flush_timeout_ms / 1000.0
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            all_empty = all(len(buf.msgs) == 0 for buf in self._buffers)
            with self._in_flight_lock:
                all_landed = all(c == 0 for c in self._in_flight)
            if all_empty and all_landed:
                return
            time.sleep(0.01)

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        self._done.set()
        for buf in self._buffers:
            with buf.cond:
                buf.closed = True
                buf.cond.notify_all()

        for t in self._senders:
            t.join(timeout=5.0)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
