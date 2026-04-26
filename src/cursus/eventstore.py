import json

from typing_extensions import Self

from cursus.connection.sync_conn import SyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_batch
from cursus.protocol.encoder import encode_message
from cursus.types import AppendResult, Event, Snapshot, StreamData, StreamEvent


class EventStore:
    def __init__(self, addr: str, topic: str, producer_id: str) -> None:
        self._addr = addr
        self._topic = topic
        self._producer_id = producer_id
        self._conn: SyncConnection | None = None

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

    def _send_command(self, cmd: str) -> str:
        conn = self._get_conn()
        try:
            conn.write_frame(encode_message("", cmd))
            resp = conn.read_frame()
            return resp.decode()
        except Exception:
            self._reset_conn()
            raise

    def create_topic(self, partitions: int) -> None:
        cmd = CommandBuilder.create(self._topic, partitions, event_sourcing=True)
        resp = self._send_command(cmd)
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")

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
        resp = self._send_command(cmd)
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")
        return self._parse_append_response(resp)

    def _parse_append_response(self, resp: str) -> AppendResult:
        version = 0
        offset = 0
        partition = 0
        for part in resp.split():
            kv = part.split("=", 1)
            if len(kv) != 2:
                continue
            match kv[0]:
                case "version":
                    version = int(kv[1])
                case "offset":
                    offset = int(kv[1])
                case "partition":
                    partition = int(kv[1])
        return AppendResult(version=version, offset=offset, partition=partition)

    def read_stream(self, key: str, from_version: int = 0) -> StreamData:
        cmd = CommandBuilder.read_stream(self._topic, key, from_version=from_version)
        conn = self._get_conn()
        try:
            conn.write_frame(encode_message("", cmd))
            env_data = conn.read_frame()
            envelope = json.loads(env_data)

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
        resp = self._send_command(cmd)
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")

    def read_snapshot(self, key: str) -> Snapshot | None:
        cmd = CommandBuilder.read_snapshot(self._topic, key)
        resp = self._send_command(cmd)
        if resp == "NULL" or "NOT_FOUND" in resp:
            return None
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")
        obj = json.loads(resp)
        return Snapshot(version=obj["version"], payload=obj["payload"])

    def stream_version(self, key: str) -> int:
        cmd = CommandBuilder.stream_version(self._topic, key)
        resp = self._send_command(cmd)
        return int(resp.strip())

    def close(self) -> None:
        self._reset_conn()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
