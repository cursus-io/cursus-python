import socket
import struct
import threading

from cursus.connection.sync_conn import SyncConnection
from cursus.protocol.wire import (
    HEADER_SIZE,
    Command,
    Compression,
    ErrorClass,
    Frame,
    Kind,
    Status,
    decode_frame,
    encode_error,
    encode_frame,
    encoded_frame_size,
)


def _recv_frame(sock: socket.socket, compression: Compression) -> Frame:
    header = _recv_exact(sock, HEADER_SIZE)
    return decode_frame(header + _recv_exact(sock, encoded_frame_size(header)), compression)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        result += sock.recv(size - len(result))
    return bytes(result)


def test_sync_connection_negotiates_correlates_and_decodes_structured_error(monkeypatch):
    client_sock, broker_sock = socket.socketpair()
    failures: list[BaseException] = []

    def broker() -> None:
        try:
            negotiation = _recv_frame(broker_sock, Compression.NONE)
            assert negotiation.kind is Kind.NEGOTIATION_REQUEST
            broker_sock.sendall(
                encode_frame(
                    Frame(
                        kind=Kind.NEGOTIATION_RESPONSE,
                        command=Command.NEGOTIATE,
                        status=Status.OK,
                        request_id=negotiation.request_id,
                        payload=struct.pack(">HB", 2, int(Compression.NONE)),
                    ),
                    Compression.NONE,
                )
            )
            request = _recv_frame(broker_sock, Compression.NONE)
            assert request.command is Command.METADATA
            assert request.request_id == 1
            broker_sock.sendall(
                encode_frame(
                    Frame(
                        kind=Kind.RESPONSE,
                        command=request.command,
                        status=Status.ERROR,
                        request_id=request.request_id,
                        payload=encode_error(
                            code="replication_unavailable",
                            error_class=ErrorClass.AVAILABILITY,
                            retryable=True,
                            message="",
                            fields={"offset": "7"},
                        ),
                    ),
                    Compression.NONE,
                )
            )
        except BaseException as exc:
            failures.append(exc)
        finally:
            broker_sock.close()

    thread = threading.Thread(target=broker)
    thread.start()
    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: client_sock)

    connection = SyncConnection("localhost:9000")
    connection.connect()
    connection.write_frame(b"METADATA topic=orders")
    response = connection.read_frame().decode()
    connection.close()
    thread.join(timeout=2)

    assert not failures
    assert response.startswith("ERROR: replication_unavailable")
    assert "class=availability" in response
    assert "retryable=true" in response
    assert "offset=7" in response
