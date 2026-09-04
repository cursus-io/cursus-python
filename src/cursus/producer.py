import threading
import time

from typing_extensions import Self

from cursus.config import ProducerConfig
from cursus.connection.sync_conn import SyncConnection
from cursus.errors import BrokerError, ConnectionError, ProducerClosedError, ProducerFencedError
from cursus.metrics import ClientMetrics
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_ack, is_terminal_producer_error
from cursus.protocol.encoder import encode_batch, encode_message
from cursus.types import Message


class _PartitionBuffer:
    def __init__(self) -> None:
        self.msgs: list[Message] = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.closed = False


class Producer:
    def __init__(self, config: ProducerConfig, metrics: ClientMetrics | None = None) -> None:
        self._config = config
        self._metrics = metrics or ClientMetrics()
        self._closed = False
        self._close_lock = threading.Lock()
        self._done = threading.Event()

        self._seq_counters: list[int] = [0] * config.partitions
        self._seq_locks: list[threading.Lock] = [threading.Lock() for _ in range(config.partitions)]
        self._rr = 0
        self._rr_lock = threading.Lock()

        self._unique_ack_count = 0
        self._ack_lock = threading.Lock()
        self._background_error: Exception | None = None
        self._error_lock = threading.Lock()

        self._in_flight = [0] * config.partitions
        self._in_flight_lock = threading.Lock()

        self._buffers = [_PartitionBuffer() for _ in range(config.partitions)]
        self._senders: list[threading.Thread] = []
        self._partition_leaders: dict[int, str] = {}
        self._producer_id = f"py-{id(self):x}"
        self._producer_epoch = int(time.time())

        if config.auto_create_topics:
            self._create_topic()
        try:
            self._fetch_metadata()
        except Exception:
            pass
        self._start_senders()

    def _create_topic(self) -> None:
        conn = SyncConnection(
            self._config.brokers[0],
            timeout_ms=self._config.write_timeout_ms,
            tls_cert_path=self._config.tls_cert_path,
            tls_key_path=self._config.tls_key_path,
            compression_type=self._config.compression_type,
            principal=self._config.principal,
            auth_token=self._config.auth_token,
        )
        conn.connect()
        try:
            cmd = CommandBuilder.create(self._config.topic, self._config.partitions)
            conn.write_frame(encode_message("admin", cmd))
            response = conn.read_frame().decode(errors="replace")
            if response != "OK" and not response.startswith("OK "):
                raise ConnectionError(f"topic auto-create failed: {response}")
        finally:
            conn.close()

    def _fetch_metadata(self) -> None:
        for addr in self._config.brokers:
            try:
                conn = SyncConnection(
                    addr,
                    tls_cert_path=self._config.tls_cert_path,
                    tls_key_path=self._config.tls_key_path,
                    compression_type=self._config.compression_type,
                    principal=self._config.principal,
                    auth_token=self._config.auth_token,
                )
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
            producer_id=self._producer_id,
            epoch=self._producer_epoch,
        )

        with buf.cond:
            if buf.closed:
                raise ProducerClosedError("partition buffer closed")
            if len(buf.msgs) >= self._config.buffer_size:
                raise BufferError(f"partition {part} buffer full")
            buf.msgs.append(msg)
            buf.cond.notify()

        self.metrics.increment("cursus.producer.messages.sent")

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
                if batch:
                    with self._in_flight_lock:
                        self._in_flight[part] += 1
                    buf.msgs = buf.msgs[n:]

            if not batch:
                continue

            sent = False
            backoff_ms = 100
            for attempt in range(self._config.max_retries + 1):
                if self._done.is_set():
                    break
                if attempt > 0:
                    self.metrics.increment("cursus.producer.retries")

                if conn is None:
                    try:
                        broker = self._partition_leaders.get(part, self._config.brokers[0])
                        conn = SyncConnection(
                            broker,
                            timeout_ms=self._config.write_timeout_ms,
                            tls_cert_path=self._config.tls_cert_path,
                            tls_key_path=self._config.tls_key_path,
                            compression_type=self._config.compression_type,
                            principal=self._config.principal,
                            auth_token=self._config.auth_token,
                        )
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
                except ProducerFencedError as exc:
                    self.metrics.increment("cursus.producer.messages.failed", len(batch))
                    self._record_background_error(exc)
                    self._done.set()
                    sent = True
                    break
                except BrokerError as exc:
                    if conn is not None:
                        conn.close()
                        conn = None
                    if exc.code.lower() == "not_leader" and exc.fields.get("leader"):
                        self._partition_leaders[part] = exc.fields["leader"]
                    if not exc.can_retry(idempotent=self._config.idempotent):
                        self.metrics.increment("cursus.producer.messages.failed", len(batch))
                        self._record_background_error(exc)
                        sent = True
                        break
                    if attempt < self._config.max_retries:
                        time.sleep(backoff_ms / 1000.0)
                        backoff_ms = min(backoff_ms * 2, self._config.max_backoff_ms)
                except Exception as exc:
                    if conn is not None:
                        conn.close()
                        conn = None
                    if not self._config.idempotent:
                        self.metrics.increment("cursus.producer.messages.failed", len(batch))
                        self._record_background_error(exc)
                        sent = True
                        break
                    if attempt < self._config.max_retries:
                        time.sleep(backoff_ms / 1000.0)
                        backoff_ms = min(backoff_ms * 2, self._config.max_backoff_ms)

            with self._in_flight_lock:
                self._in_flight[part] -= 1

            if not sent and batch:
                self.metrics.increment("cursus.producer.messages.failed", len(batch))
                self._record_background_error(
                    ConnectionError(f"producer retries exhausted for partition {part}")
                )

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
        conn.write_frame(data)
        if self._config.acks.value == "0":
            return
        resp_data = conn.read_frame()

        ack = decode_ack(resp_data)
        if ack.status == "OK":
            with self._ack_lock:
                self._unique_ack_count += len(batch)
            self.metrics.increment("cursus.producer.messages.acked", len(batch))
            return
        if ack.error and is_terminal_producer_error(ack.error):
            raise ProducerFencedError(ack.error)
        if ack.error:
            raise BrokerError(
                ack.error_code,
                ack.error_class,
                ack.retryable,
                ack.error,
                ack.error_fields,
            )
        error = ack.error or f"broker returned status={ack.status}"
        raise ConnectionError(f"broker rejected batch for partition {partition}: {error}")

    @property
    def unique_ack_count(self) -> int:
        with self._ack_lock:
            return self._unique_ack_count

    @property
    def metrics(self) -> ClientMetrics:
        if not hasattr(self, "_metrics"):
            self._metrics = ClientMetrics()
        return self._metrics

    def _record_background_error(self, error: Exception) -> None:
        if not hasattr(self, "_error_lock"):
            self._error_lock = threading.Lock()
        with self._error_lock:
            if getattr(self, "_background_error", None) is None:
                self._background_error = error

    def _raise_background_error(self) -> None:
        if not hasattr(self, "_error_lock"):
            self._error_lock = threading.Lock()
        with self._error_lock:
            error = getattr(self, "_background_error", None)
        if error is not None:
            raise error

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
                self._raise_background_error()
                return
            time.sleep(0.01)
        self._raise_background_error()
        raise TimeoutError("producer flush timed out with buffered or in-flight messages")

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
