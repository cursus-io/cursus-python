import asyncio
import ssl
import struct
from types import TracebackType

from typing_extensions import Self

from cursus.connection.framing import MAX_MESSAGE_SIZE
from cursus.errors import ConnectionError, ProtocolError


class AsyncConnection:
    def __init__(
        self,
        addr: str,
        timeout_ms: int = 5000,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
    ) -> None:
        self._addr = addr
        self._timeout_s = timeout_ms / 1000.0
        self._tls_cert_path = tls_cert_path
        self._tls_key_path = tls_key_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        host, port_str = self._addr.rsplit(":", 1)
        port = int(port_str)

        ssl_ctx: ssl.SSLContext | None = None
        if self._tls_cert_path and self._tls_key_path:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.load_cert_chain(self._tls_cert_path, self._tls_key_path)

        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_ctx),
                timeout=self._timeout_s,
            )
        except (OSError, asyncio.TimeoutError) as e:
            raise ConnectionError(f"failed to connect to {self._addr}: {e}") from e

    async def write_frame(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("not connected")
        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(f"data size {len(data)} exceeds maximum {MAX_MESSAGE_SIZE}")
        frame = struct.pack(">I", len(data)) + data
        try:
            self._writer.write(frame)
            await self._writer.drain()
        except OSError as e:
            raise ConnectionError(f"write failed: {e}") from e

    async def read_frame(self) -> bytes:
        if self._reader is None:
            raise ConnectionError("not connected")
        try:
            length_buf = await self._reader.readexactly(4)
            length = struct.unpack(">I", length_buf)[0]
            if length > MAX_MESSAGE_SIZE:
                raise ProtocolError(f"frame size {length} exceeds maximum {MAX_MESSAGE_SIZE}")
            return await self._reader.readexactly(length)
        except asyncio.IncompleteReadError as e:
            raise ConnectionError("connection closed by peer") from e
        except OSError as e:
            raise ConnectionError(f"read failed: {e}") from e

    async def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            self._writer = None
            self._reader = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
