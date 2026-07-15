from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from cursus.connection.sync_conn import SyncConnection
from cursus.errors import BrokerError, ConnectionError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_not_coordinator, require_ok
from cursus.protocol.encoder import encode_message


class BrokerCommandClient:
    def __init__(
        self,
        brokers: list[str] | None = None,
        *,
        timeout_ms: int = 5000,
        max_retries: int = 3,
        backoff_ms: int = 100,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
    ) -> None:
        self._brokers = brokers or ["localhost:9000"]
        self._timeout_ms = timeout_ms
        self._max_retries = max(1, max_retries)
        self._backoff_ms = max(0, backoff_ms)
        self._tls_cert_path = tls_cert_path
        self._tls_key_path = tls_key_path
        self._coordinators: dict[str, str] = {}

    def send_any(self, cmd: str, *, operation: str) -> str:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            for addr in self._brokers:
                try:
                    resp = self._send_to_addr(addr, cmd)
                    require_ok(resp, operation=operation)
                    return resp
                except BrokerError:
                    raise
                except Exception as exc:
                    last_error = exc
            self._sleep_backoff(attempt)
        raise ConnectionError(f"{operation} failed after retries: {last_error}")

    def send_transaction_coordinator(self, transactional_id: str, cmd: str) -> str:
        last_response = ""
        for attempt in range(self._max_retries):
            addr = self._coordinators.get(transactional_id) or self._brokers[0]
            resp = self._send_to_addr(addr, cmd)
            last_response = resp
            redirect = decode_not_coordinator(resp)
            if redirect:
                self._coordinators[transactional_id] = redirect
                self._sleep_backoff(attempt)
                continue
            require_ok(resp, operation="transaction command")
            return resp
        require_ok(last_response, operation="transaction command")
        raise ConnectionError(f"transaction command failed after redirects: {cmd}")

    def find_transaction_coordinator(self, transactional_id: str) -> str:
        cmd = CommandBuilder.find_coordinator(transactional_id=transactional_id)
        resp = self.send_any(cmd, operation="find transaction coordinator")
        fields = require_ok(resp, operation="find transaction coordinator")
        host = fields.get("host")
        port = fields.get("port")
        if not host or not port:
            raise ConnectionError(f"find coordinator missing host/port: {resp}")
        addr = f"{host}:{port}"
        self._coordinators[transactional_id] = addr
        return addr

    def _send_to_addr(self, addr: str, cmd: str) -> str:
        conn = SyncConnection(
            addr,
            timeout_ms=self._timeout_ms,
            tls_cert_path=self._tls_cert_path,
            tls_key_path=self._tls_key_path,
        )
        try:
            conn.connect()
            conn.write_frame(encode_message("", cmd))
            return conn.read_frame().decode()
        finally:
            conn.close()

    def _sleep_backoff(self, attempt: int) -> None:
        if self._backoff_ms <= 0:
            return
        delay = min(self._backoff_ms * (2**attempt), self._timeout_ms) / 1000.0
        time.sleep(delay)


class TransactionContext:
    def __init__(self, producer: Any) -> None:
        self._producer = producer

    def __enter__(self) -> Any:
        self._producer.begin_transaction()
        return self._producer

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is None:
            self._producer.commit_transaction()
        else:
            self._producer.abort_transaction()


RetryPredicate = Callable[[Exception], bool]
