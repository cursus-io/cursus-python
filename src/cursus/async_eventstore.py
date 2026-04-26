import json
from types import TracebackType

from typing_extensions import Self

from cursus.connection.async_conn import AsyncConnection
from cursus.errors import ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_batch
from cursus.protocol.encoder import encode_message
from cursus.types import AppendResult, Event, Snapshot, StreamData, StreamEvent


class AsyncEventStore:
    def __init__(self, addr: str, topic: str, producer_id: str) -> None:
        self._addr = addr
        self._topic = topic
        self._producer_id = producer_id
        self._conn: AsyncConnection | None = None

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
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")

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
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")

        result = AppendResult(version=0, offset=0, partition=0)
        for part in resp.split():
            kv = part.split("=", 1)
            if len(kv) != 2:
                continue
            match kv[0]:
                case "version":
                    result.version = int(kv[1])
                case "offset":
                    result.offset = int(kv[1])
                case "partition":
                    result.partition = int(kv[1])
        return result

    async def read_stream(self, key: str, from_version: int = 0) -> StreamData:
        cmd = CommandBuilder.read_stream(self._topic, key, from_version=from_version)
        conn = await self._get_conn()
        try:
            await conn.write_frame(encode_message("", cmd))
            env_data = await conn.read_frame()
            envelope = json.loads(env_data)

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
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")

    async def read_snapshot(self, key: str) -> Snapshot | None:
        cmd = CommandBuilder.read_snapshot(self._topic, key)
        resp = await self._send_command(cmd)
        if resp == "NULL" or "NOT_FOUND" in resp:
            return None
        if resp.startswith("ERROR"):
            raise ConnectionError(f"broker: {resp}")
        obj = json.loads(resp)
        return Snapshot(version=obj["version"], payload=obj["payload"])

    async def stream_version(self, key: str) -> int:
        cmd = CommandBuilder.stream_version(self._topic, key)
        resp = await self._send_command(cmd)
        return int(resp.strip())

    async def close(self) -> None:
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
