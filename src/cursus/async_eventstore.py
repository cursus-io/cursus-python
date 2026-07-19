import asyncio
import json
from types import TracebackType

from typing_extensions import Self

from cursus.connection.async_conn import AsyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_batch, decode_snapshot_response, decode_version_response
from cursus.protocol.encoder import encode_message
from cursus.types import AppendResult, Event, Snapshot, StreamData, StreamEvent


class AsyncEventStore:
    def __init__(self, addr: str, topic: str, producer_id: str) -> None:
        self._addr = addr
        self._topic = topic
        self._producer_id = producer_id
        self._conn: AsyncConnection | None = None
        self._request_lock = asyncio.Lock()

    async def _get_conn(self) -> AsyncConnection:
        if self._conn is not None:
            return self._conn
        conn = AsyncConnection(self._addr)
        await conn.connect()
        self._conn = conn
        return conn

    async def _reset_conn(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _send_command(self, cmd: str) -> str:
        async with self._request_lock:
            return await self._send_command_locked(cmd)

    async def _send_command_locked(self, cmd: str) -> str:
        conn = await self._get_conn()
        try:
            await conn.write_frame(encode_message("", cmd))
            resp = await conn.read_frame()
            return resp.decode()
        except Exception:
            await self._reset_conn()
            raise

    async def create_topic(self, partitions: int) -> None:
        cmd = CommandBuilder.create(self._topic, partitions, event_sourcing=True)
        resp = await self._send_command(cmd)
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"broker: {resp}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"unexpected create response: {resp}")

    async def append(self, key: str, expected_version: int, event: Event) -> AppendResult:
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
        resp = await self._send_command(cmd)
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

    async def read_stream(self, key: str, from_version: int = 0) -> StreamData:
        async with self._request_lock:
            return await self._read_stream_locked(key, from_version)

    async def _read_stream_locked(self, key: str, from_version: int) -> StreamData:
        cmd = CommandBuilder.read_stream(self._topic, key, from_version=from_version)
        conn = await self._get_conn()
        try:
            await conn.write_frame(encode_message("", cmd))
            env_data = await conn.read_frame()
            envelope = json.loads(env_data)
            status = envelope.get("status")
            if status == "ERROR":
                raise ConnectionError(f"broker: {envelope.get('error', 'read stream failed')}")
            if status != "OK":
                raise ConnectionError(f"unexpected read stream status: {status}")

            snapshot: Snapshot | None = None
            if envelope.get("snapshot"):
                snap = envelope["snapshot"]
                snapshot = Snapshot(version=snap["version"], payload=snap["payload"])

            batch_data = await conn.read_frame()
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
            await self._reset_conn()
            raise

    async def save_snapshot(self, key: str, version: int, payload: str) -> None:
        cmd = CommandBuilder.save_snapshot(self._topic, key, version, payload)
        resp = await self._send_command(cmd)
        if resp.startswith("ERROR:"):
            raise ConnectionError(f"broker: {resp}")
        if not resp.startswith("OK"):
            raise ConnectionError(f"unexpected save snapshot response: {resp}")

    async def read_snapshot(self, key: str) -> Snapshot | None:
        cmd = CommandBuilder.read_snapshot(self._topic, key)
        resp = await self._send_command(cmd)
        try:
            snapshot_data = decode_snapshot_response(resp)
        except ValueError as exc:
            raise ConnectionError(f"broker: {resp}") from exc
        if snapshot_data is None:
            return None
        obj = json.loads(snapshot_data)
        return Snapshot(version=obj["version"], payload=obj["payload"])

    async def stream_version(self, key: str) -> int:
        cmd = CommandBuilder.stream_version(self._topic, key)
        resp = await self._send_command(cmd)
        try:
            return decode_version_response(resp)
        except ValueError as exc:
            raise ConnectionError(f"broker: {resp}") from exc

    async def close(self) -> None:
        async with self._request_lock:
            await self._reset_conn()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
