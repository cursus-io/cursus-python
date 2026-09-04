import socket
import ssl

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


class SyncConnection:
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
        self._sock: socket.socket | None = None
        self._next_request_id = 0
        self._active_request_id = 0
        self._active_command = Command.UNKNOWN
        self._received = 0
        self._awaiting = False

    def connect(self) -> None:
        host, port_str = self._addr.rsplit(":", 1)
        port = int(port_str)
        try:
            sock = socket.create_connection((host, port), timeout=self._timeout_s)
        except OSError as exc:
            raise ConnectionError(f"failed to connect to {self._addr}: {exc}") from exc

        if self._tls_cert_path and self._tls_key_path:
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(self._tls_cert_path, self._tls_key_path)
            sock = ctx.wrap_socket(sock, server_hostname=host)

        self._sock = sock
        try:
            self._handshake()
            self._authenticate()
        except Exception:
            self.close()
            raise

    def _handshake(self) -> None:
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
        self._send_wire_frame(request, Compression.NONE)
        response = self._read_wire_frame(Compression.NONE)
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

    def _authenticate(self) -> None:
        if not self._principal or not self._auth_token:
            return
        self.write_frame(f"AUTH principal={self._principal} token={self._auth_token}".encode())
        response = self.read_frame().decode(errors="replace")
        if response != "OK" and not response.startswith("OK "):
            raise ProtocolError(f"Wire v2 authentication failed: {response}")

    def write_frame(self, data: bytes) -> None:
        if self._sock is None:
            raise ConnectionError("not connected")
        if self._awaiting:
            raise ProtocolError("Wire v2 connection already has a pending request")
        command, payload = request_frame_payload(data)
        self._next_request_id += 1
        request_id = self._next_request_id
        frame = Frame(
            kind=Kind.REQUEST,
            command=command,
            status=Status.NONE,
            request_id=request_id,
            payload=payload,
        )
        self._send_wire_frame(frame, self._compression)
        self._active_request_id = request_id
        self._active_command = command
        self._received = 0
        self._awaiting = True
        if response_suppressed(data):
            self._finish_request()

    def read_frame(self) -> bytes:
        if self._sock is None:
            raise ConnectionError("not connected")
        if not self._awaiting:
            raise ProtocolError("Wire v2 connection has no pending request")
        try:
            frame = self._read_wire_frame(self._compression)
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

    def _send_wire_frame(self, frame: Frame, compression: Compression) -> None:
        assert self._sock is not None
        try:
            self._sock.sendall(encode_frame(frame, compression))
        except OSError as exc:
            raise ConnectionError(f"write failed: {exc}") from exc

    def _read_wire_frame(self, compression: Compression) -> Frame:
        try:
            header = self._recv_exact(HEADER_SIZE)
            payload = self._recv_exact(encoded_frame_size(header))
        except OSError as exc:
            raise ConnectionError(f"read failed: {exc}") from exc
        return decode_frame(header + payload, compression)

    def _recv_exact(self, size: int) -> bytes:
        assert self._sock is not None
        buf = bytearray()
        while len(buf) < size:
            chunk = self._sock.recv(size - len(buf))
            if not chunk:
                raise ConnectionError("connection closed by peer")
            buf.extend(chunk)
        return bytes(buf)

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._finish_request()

    def set_timeout(self, timeout_ms: int) -> None:
        if self._sock is not None:
            self._sock.settimeout(timeout_ms / 1000.0)

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
