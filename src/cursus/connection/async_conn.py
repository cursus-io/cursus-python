import asyncio
import ssl
from types import TracebackType

from typing_extensions import Self

from cursus.errors import ConnectionError, ProtocolError
from cursus.protocol.wire import (
    HEADER_SIZE,
    Command,
    Compression,
    Frame,
    Kind,
    Status,
    compression_from_name,
    decode_error,
    decode_frame,
    decode_negotiation_response,
    encode_frame,
    encode_negotiation_request,
    encoded_frame_size,
    render_error,
    request_frame_payload,
    require_compression,
    response_suppressed,
)


class AsyncConnection:
    def __init__(
        self,
        addr: str,
        timeout_ms: int = 5000,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
        compression_type: str = "none",
        principal: str | None = None,
        auth_token: str | None = None,
    ) -> None:
        self._addr = addr
        self._timeout_s = timeout_ms / 1000.0
        self._tls_cert_path = tls_cert_path
        self._tls_key_path = tls_key_path
        if bool(tls_cert_path) != bool(tls_key_path):
            raise ValueError("tls_cert_path and tls_key_path must be configured together")
        if bool(principal) != bool(auth_token):
            raise ValueError("principal and auth_token must be configured together")
        self._principal = principal
        self._auth_token = auth_token
        self._requested_compression = compression_from_name(compression_type)
        require_compression(self._requested_compression)
        self._compression = Compression.NONE
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_request_id = 0
        self._active_request_id = 0
        self._active_command = Command.UNKNOWN
        self._received = 0
        self._awaiting = False

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
            await self._handshake()
            await self._authenticate()
        except (OSError, asyncio.TimeoutError) as exc:
            await self.close()
            raise ConnectionError(f"failed to connect to {self._addr}: {exc}") from exc
        except Exception:
            await self.close()
            raise

    async def _handshake(self) -> None:
        preferences = [self._requested_compression]
        if self._requested_compression is not Compression.NONE:
            preferences.append(Compression.NONE)
        request = Frame(
            kind=Kind.NEGOTIATION_REQUEST,
            command=Command.NEGOTIATE,
            status=Status.NONE,
            request_id=0,
            payload=encode_negotiation_request(preferences),
        )
        await self._send_wire_frame(request, Compression.NONE)
        response = await self._read_wire_frame(Compression.NONE)
        if (
            response.kind is not Kind.NEGOTIATION_RESPONSE
            or response.command is not Command.NEGOTIATE
            or response.status is not Status.OK
        ):
            raise ProtocolError("Wire v2 negotiation was rejected")
        selected = decode_negotiation_response(response.payload)
        if selected not in preferences:
            raise ProtocolError("broker selected unrequested compression")
        self._compression = selected

    async def _authenticate(self) -> None:
        if not self._principal or not self._auth_token:
            return
        await self.write_frame(
            f"AUTH principal={self._principal} token={self._auth_token}".encode()
        )
        response = (await self.read_frame()).decode(errors="replace")
        if response != "OK" and not response.startswith("OK "):
            raise ProtocolError(f"Wire v2 authentication failed: {response}")

    async def write_frame(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("not connected")
        if self._awaiting:
            raise ProtocolError("Wire v2 connection already has a pending request")
        command, payload = request_frame_payload(data)
        self._next_request_id += 1
        request_id = self._next_request_id
        await self._send_wire_frame(
            Frame(
                kind=Kind.REQUEST,
                command=command,
                status=Status.NONE,
                request_id=request_id,
                payload=payload,
            ),
            self._compression,
        )
        self._active_request_id = request_id
        self._active_command = command
        self._received = 0
        self._awaiting = True
        if response_suppressed(data):
            self._finish_request()

    async def read_frame(self) -> bytes:
        if self._reader is None:
            raise ConnectionError("not connected")
        if not self._awaiting:
            raise ProtocolError("Wire v2 connection has no pending request")
        try:
            frame = await self._read_wire_frame(self._compression)
        except Exception:
            self._finish_request()
            raise
        if frame.kind not in (Kind.RESPONSE, Kind.STREAM):
            self._finish_request()
            raise ProtocolError(f"unexpected Wire v2 response kind: {frame.kind}")
        if frame.request_id != self._active_request_id or frame.command is not self._active_command:
            self._finish_request()
            raise ProtocolError("Wire v2 response correlation mismatch")
        self._received += 1
        terminal = self._response_is_terminal(frame.status)
        if terminal:
            self._finish_request()
        if frame.status is Status.ERROR:
            return render_error(decode_error(frame.payload))
        if frame.status not in (Status.OK, Status.STREAM_END):
            self._finish_request()
            raise ProtocolError(f"unexpected Wire v2 response status: {frame.status}")
        return frame.payload

    def _response_is_terminal(self, status: Status) -> bool:
        if status is Status.ERROR:
            return True
        if self._active_command is Command.READ_STREAM:
            return status is Status.STREAM_END or self._received >= 2
        if self._active_command is Command.STREAM:
            return status is Status.STREAM_END
        return True

    def _finish_request(self) -> None:
        self._active_request_id = 0
        self._active_command = Command.UNKNOWN
        self._received = 0
        self._awaiting = False

    async def _send_wire_frame(self, frame: Frame, compression: Compression) -> None:
        assert self._writer is not None
        self._writer.write(encode_frame(frame, compression))
        try:
            await self._writer.drain()
        except OSError as exc:
            raise ConnectionError(f"write failed: {exc}") from exc

    async def _read_wire_frame(self, compression: Compression) -> Frame:
        assert self._reader is not None
        try:
            header = await self._reader.readexactly(HEADER_SIZE)
            payload = await self._reader.readexactly(encoded_frame_size(header))
        except asyncio.IncompleteReadError as exc:
            raise ConnectionError("connection closed by peer") from exc
        except OSError as exc:
            raise ConnectionError(f"read failed: {exc}") from exc
        return decode_frame(header + payload, compression)

    async def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            self._writer = None
            self._reader = None
        self._finish_request()

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
