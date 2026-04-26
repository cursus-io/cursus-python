import socket
import ssl
import struct

from typing_extensions import Self

from cursus.connection.framing import MAX_MESSAGE_SIZE
from cursus.errors import ConnectionError, ProtocolError


class SyncConnection:
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
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        host, port_str = self._addr.rsplit(":", 1)
        port = int(port_str)
        try:
            sock = socket.create_connection((host, port), timeout=self._timeout_s)
        except OSError as e:
            raise ConnectionError(f"failed to connect to {self._addr}: {e}") from e

        if self._tls_cert_path and self._tls_key_path:
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(self._tls_cert_path, self._tls_key_path)
            sock = ctx.wrap_socket(sock, server_hostname=host)

        self._sock = sock

    def write_frame(self, data: bytes) -> None:
        if self._sock is None:
            raise ConnectionError("not connected")
        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(f"data size {len(data)} exceeds maximum {MAX_MESSAGE_SIZE}")
        frame = struct.pack(">I", len(data)) + data
        try:
            self._sock.sendall(frame)
        except OSError as e:
            raise ConnectionError(f"write failed: {e}") from e

    def read_frame(self) -> bytes:
        if self._sock is None:
            raise ConnectionError("not connected")
        try:
            length_buf = self._recv_exact(4)
            length = struct.unpack(">I", length_buf)[0]
            if length > MAX_MESSAGE_SIZE:
                raise ProtocolError(f"frame size {length} exceeds maximum {MAX_MESSAGE_SIZE}")
            return self._recv_exact(length)
        except OSError as e:
            raise ConnectionError(f"read failed: {e}") from e

    def _recv_exact(self, n: int) -> bytes:
        assert self._sock is not None
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
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

    def set_timeout(self, timeout_ms: int) -> None:
        if self._sock is not None:
            self._sock.settimeout(timeout_ms / 1000.0)

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
