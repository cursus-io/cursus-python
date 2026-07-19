import json
import threading
import time

from typing_extensions import Self

from cursus.connection.sync_conn import SyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_batch, decode_snapshot_response, decode_version_response
from cursus.protocol.encoder import encode_message
from cursus.types import AppendResult, Event, Snapshot, StreamData, StreamEvent


class EventStore:
    def __init__(self, addr: str | list[str], topic: str, producer_id: str) -> None:
        self._addrs = [addr] if isinstance(addr, str) else list(addr)
        if not self._addrs:
            raise ValueError("at least one broker address is required")
        self._addr = self._addrs[0]
        self._topic = topic
        self._producer_id = producer_id
        self._conn: SyncConnection | None = None
        self._request_lock = threading.Lock()

    def _get_conn(self) -> SyncConnection:
        if self._conn is not None:
            return self._conn
        conn = SyncConnection(self._addr)
        conn.connect()
        self._conn = conn
        return conn

    def _reset_conn(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _switch_addr(self, addr: str) -> None:
        if addr != self._addr:
            self._addr = addr
        self._reset_conn()

    @staticmethod
    def _leader_from_error(resp: str) -> str | None:
        marker = "NOT_LEADER LEADER_IS"
        if marker not in resp:
            return None
        parts = resp.split(marker, 1)[1].strip().split()
        return parts[0] if parts else None

    @staticmethod
    def _is_retryable_topic_error(resp: str) -> bool:
        text = resp.lower()
        return (
            "topic_not_found" in text or "no_raft_leader" in text or "leader_not_available" in text
        )

    def _send_command_once(self, cmd: str) -> str:
        conn = self._get_conn()
        try:
            conn.write_frame(encode_message("", cmd))
            resp = conn.read_frame()
            return resp.decode()
        except Exception:
            self._reset_conn()
            raise

    def _send_command(self, cmd: str, *, retries: int = 5, retry_topic_errors: bool = False) -> str:
        with self._request_lock:
            return self._send_command_locked(
                cmd,
                retries=retries,
                retry_topic_errors=retry_topic_errors,
            )

    def _send_command_locked(
        self, cmd: str, *, retries: int, retry_topic_errors: bool
    ) -> str:
        last_resp = ""
        for attempt in range(retries + 1):
            resp = self._send_command_once(cmd)
            last_resp = resp
            leader = self._leader_from_error(resp)
            if leader is not None and attempt < retries:
                self._switch_addr(leader)
                continue
            if (
                retry_topic_errors
                and resp.startswith("ERROR:")
                and self._is_retryable_topic_error(resp)
                and attempt < retries
            ):
                self._reset_conn()
                time.sleep(min(0.05 * (attempt + 1), 0.5))
                continue
            return resp
        return last_resp

    def create_topic(self, partitions: int) -> None:
        cmd = CommandBuilder.create(self._topic, partitions, event_sourcing=True)
        resp = self._send_command(cmd, retries=5, retry_topic_errors=True)
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"broker: {resp}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"unexpected create response: {resp}")

    def append(self, key: str, expected_version: int, event: Event) -> AppendResult:
        cmd = CommandBuilder.append_stream(
            topic=self._topic,
            key=key,
            version=expected_version,
            event_type=event.type,
            schema_version=event.schema_version,
            producer_id=self._producer_id,
            payload=event.payload,
            metadata=event.metadata,
        )
        resp = self._send_command(cmd, retries=6, retry_topic_errors=True)
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"broker: {resp}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"unexpected append response: {resp}")
        try:
            return self._parse_append_response(resp)
        except ValueError as exc:
            raise ConnectionError(f"broker: {resp}") from exc

    def _parse_append_response(self, resp: str) -> AppendResult:
        if not resp.startswith("OK"):
            raise ValueError(f"unexpected append response: {resp}")
        fields: dict[str, str] = {}
        for part in resp.split()[1:]:
            key, sep, value = part.partition("=")
            if sep:
                fields[key] = value
        missing = {"version", "offset", "partition"} - fields.keys()
        if missing:
            raise ValueError(f"missing append fields in response: {resp}")
        return AppendResult(
            version=int(fields["version"]),
            offset=int(fields["offset"]),
            partition=int(fields["partition"]),
        )

    def read_stream(self, key: str, from_version: int = 0) -> StreamData:
        with self._request_lock:
            return self._read_stream_locked(key, from_version)

    def _read_stream_locked(self, key: str, from_version: int) -> StreamData:
        cmd = CommandBuilder.read_stream(self._topic, key, from_version=from_version)
        last_error = ""
        for attempt in range(7):
            try:
                return self._read_stream_once(cmd)
            except ConnectionError as exc:
                message = str(exc)
                last_error = message
                leader = self._leader_from_error(message)
                if leader is not None and attempt < 6:
                    self._switch_addr(leader)
                    continue
                if self._is_retryable_topic_error(message) and attempt < 6:
                    self._reset_conn()
                    time.sleep(min(0.05 * (attempt + 1), 0.5))
                    continue
                raise
        raise ConnectionError(last_error or "read stream failed")

    def _read_stream_once(self, cmd: str) -> StreamData:
        conn = self._get_conn()
        try:
            conn.write_frame(encode_message("", cmd))
            env_data = conn.read_frame()
            try:
                envelope = json.loads(env_data)
            except json.JSONDecodeError as exc:
                text = env_data.decode(errors="replace")
                raise ConnectionError(f"broker: {text}") from exc

            status = envelope.get("status")
            if status == "ERROR":
                raise ConnectionError(f"broker: {envelope.get('error', 'read stream failed')}")
            if status != "OK":
                raise ConnectionError(f"unexpected read stream status: {status}")

            snapshot: Snapshot | None = None
            if envelope.get("snapshot"):
                snap = envelope["snapshot"]
                snapshot = Snapshot(version=snap["version"], payload=snap["payload"])

            batch_data = conn.read_frame()
            events: list[StreamEvent] = []
            if len(batch_data) > 0:
                messages, _, _ = decode_batch(batch_data)
                for m in messages:
                    events.append(
                        StreamEvent(
                            version=m.aggregate_version,
                            offset=m.offset,
                            type=m.event_type,
                            schema_version=m.schema_version,
                            payload=m.payload,
                            metadata=m.metadata,
                        )
                    )

            return StreamData(snapshot=snapshot, events=events)
        except Exception:
            self._reset_conn()
            raise

    def save_snapshot(self, key: str, version: int, payload: str) -> None:
        cmd = CommandBuilder.save_snapshot(self._topic, key, version, payload)
        resp = self._send_command(cmd, retries=6, retry_topic_errors=True)
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"broker: {resp}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"unexpected save snapshot response: {resp}")

    def read_snapshot(self, key: str) -> Snapshot | None:
        cmd = CommandBuilder.read_snapshot(self._topic, key)
        resp = self._send_command(cmd, retries=6, retry_topic_errors=True)
        try:
            snapshot_data = decode_snapshot_response(resp)
        except ValueError as exc:
            raise ConnectionError(f"broker: {resp}") from exc
        if snapshot_data is None:
            return None
        obj = json.loads(snapshot_data)
        return Snapshot(version=obj["version"], payload=obj["payload"])

    def stream_version(self, key: str) -> int:
        cmd = CommandBuilder.stream_version(self._topic, key)
        resp = self._send_command(cmd, retries=6, retry_topic_errors=True)
        try:
            return decode_version_response(resp)
        except ValueError as exc:
            raise ConnectionError(f"broker: {resp}") from exc

    def close(self) -> None:
        with self._request_lock:
            self._reset_conn()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
