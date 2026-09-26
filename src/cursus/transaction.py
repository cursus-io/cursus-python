from typing_extensions import Self

from cursus.broker_client import BrokerCommandClient, TransactionContext
from cursus.errors import ProducerFencedError
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import (
    decode_producer_session,
    decode_transaction_status,
    error_from_response,
    is_error_response,
    require_ok,
)
from cursus.types import ProducerSession, TransactionStatus


class TransactionalProducer:
    """Synchronous producer for broker-managed staged record+offset transactions."""

    def __init__(
        self,
        transactional_id: str,
        brokers: list[str] | None = None,
        *,
        principal: str | None = None,
        auth_token: str | None = None,
        timeout_ms: int = 5000,
        max_retries: int = 3,
        backoff_ms: int = 100,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
        compression_type: str = "none",
    ) -> None:
        if not transactional_id:
            raise ValueError("transactional_id is required")
        self.transactional_id = transactional_id
        self.producer_id = ""
        self.epoch = 0
        # The broker fences idempotent sequences per topic-partition. A
        # broker-native Saga writes state, commands, and history to separate
        # topics, so a global counter would make a later topic start at 2.
        self._seq_num_by_topic: dict[str, int] = {}
        self._principal = principal
        self._auth_token = auth_token
        self._client = BrokerCommandClient(
            brokers,
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            backoff_ms=backoff_ms,
            tls_cert_path=tls_cert_path,
            tls_key_path=tls_key_path,
            compression_type=compression_type,
            principal=principal,
            auth_token=auth_token,
        )

    def transaction(self) -> TransactionContext:
        """Return a context manager that aborts on exceptions and commits otherwise."""
        return TransactionContext(self)

    def __enter__(self) -> Self:
        self.begin_transaction()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is None:
            try:
                self.commit_transaction()
            except Exception:
                # A failed commit may have left an OPEN transaction and its
                # stream reservations behind.  Abort only after a fresh
                # status read proves that outcome; an unavailable status must
                # remain retryable rather than being guessed as an abort.
                try:
                    if self.status().state == "open":
                        self.abort_transaction()
                except Exception:
                    pass
                raise
        else:
            self.abort_transaction()

    def init_producer_id(self) -> ProducerSession:
        """Initialize or refresh the broker-owned producer ID and epoch."""
        resp = self._send(CommandBuilder.init_producer_id(self.transactional_id))
        session = decode_producer_session(resp)
        self.producer_id = session.producer_id
        self.epoch = session.epoch
        self._seq_num_by_topic.clear()
        return session

    def begin_transaction(self) -> None:
        """Start a transaction, initializing producer identity if needed."""
        self._ensure_session()
        resp = self._send(
            CommandBuilder.begin_txn(self.transactional_id, self.producer_id, self.epoch)
        )
        require_ok(resp, operation="begin transaction")

    def publish(
        self,
        topic: str,
        message: str,
        *,
        partition: int = -1,
        key: str = "",
    ) -> None:
        """Stage one record in the open transaction."""
        self._ensure_session()
        sequence = self._next_sequence(topic)
        cmd = CommandBuilder.txn_publish(
            self.transactional_id,
            topic,
            partition,
            self.producer_id,
            sequence,
            self.epoch,
            message,
            key=key,
            principal=self._principal,
            auth_token=self._auth_token,
        )
        resp = self._send(cmd)
        require_ok(resp, operation="transactional publish")

    def append_stream(
        self,
        topic: str,
        key: str,
        expected_version: int,
        payload: str,
        *,
        event_type: str = "",
        schema_version: int = 1,
        metadata: str = "",
    ) -> None:
        """Append one version-checked stream event in the open broker transaction."""
        if not topic or not key or expected_version <= 0 or not payload:
            raise ValueError("topic, key, positive expected_version, and payload are required")
        if any(char.isspace() for char in key) or any(char.isspace() for char in event_type):
            raise ValueError("stream key and event_type must not contain whitespace")
        if "\n" in payload or "\r" in payload or any(char.isspace() for char in metadata):
            raise ValueError("payload line breaks and metadata whitespace are not supported")
        self._ensure_session()
        sequence = self._next_sequence(topic)
        resp = self._send(
            CommandBuilder.txn_append_stream(
                self.transactional_id,
                topic,
                key,
                expected_version,
                self.producer_id,
                sequence,
                self.epoch,
                payload,
                event_type=event_type,
                schema_version=schema_version,
                metadata=metadata,
                principal=self._principal,
                auth_token=self._auth_token,
            )
        )
        require_ok(resp, operation="transactional append stream")

    def send_offsets_to_transaction(
        self,
        topic: str,
        group: str,
        member: str,
        generation: int,
        offsets: dict[int, int],
    ) -> None:
        """Stage consumer offsets using nextOffset values sorted by partition."""
        self._ensure_session()
        if not offsets:
            raise ValueError("offsets must not be empty")
        cmd = CommandBuilder.send_offsets_to_txn(
            self.transactional_id,
            self.producer_id,
            self.epoch,
            topic,
            group,
            member,
            generation,
            offsets,
        )
        resp = self._send(cmd)
        require_ok(resp, operation="send offsets to transaction")

    def commit_transaction(self) -> None:
        """Commit the transaction. Retrying this method is idempotent for the same epoch."""
        self._end_transaction(commit=True)

    def abort_transaction(self) -> None:
        """Abort the transaction. Retrying this method is idempotent for the same epoch."""
        self._end_transaction(commit=False)

    def status(self) -> TransactionStatus:
        """Return broker transaction state."""
        resp = self._send(CommandBuilder.txn_status(self.transactional_id))
        return decode_transaction_status(resp)

    def _end_transaction(self, *, commit: bool) -> None:
        self._ensure_session()
        cmd = CommandBuilder.end_txn(
            self.transactional_id, self.producer_id, self.epoch, commit=commit
        )
        resp = self._send(cmd)
        require_ok(resp, operation="end transaction")

    def _ensure_session(self) -> None:
        if not self.producer_id:
            self.init_producer_id()

    def _next_sequence(self, topic: str) -> int:
        sequence = self._seq_num_by_topic.get(topic, 0) + 1
        self._seq_num_by_topic[topic] = sequence
        return sequence

    def _send(self, cmd: str) -> str:
        resp = self._client.send_transaction_coordinator(self.transactional_id, cmd)
        if is_error_response(resp):
            err = error_from_response(resp)
            if isinstance(err, ProducerFencedError):
                raise err
            raise err
        return resp
