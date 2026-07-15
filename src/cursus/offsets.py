from cursus.broker_client import BrokerCommandClient
from cursus.protocol.command import CommandBuilder
from cursus.protocol.decoder import decode_list_offsets_response
from cursus.types import PartitionOffsetRange


class OffsetClient:
    """Synchronous broker offset discovery client."""

    def __init__(
        self,
        brokers: list[str] | None = None,
        *,
        principal: str | None = None,
        auth_token: str | None = None,
        timeout_ms: int = 5000,
        max_retries: int = 3,
        backoff_ms: int = 100,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
    ) -> None:
        self._principal = principal
        self._auth_token = auth_token
        self._client = BrokerCommandClient(
            brokers,
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            backoff_ms=backoff_ms,
            tls_cert_path=tls_cert_path,
            tls_key_path=tls_key_path,
        )

    def list_offsets(self, topic: str, partition: int | None = None) -> list[PartitionOffsetRange]:
        """Return broker-retained offset ranges for one topic or partition."""
        cmd = CommandBuilder.list_offsets(topic, partition, self._principal, self._auth_token)
        resp = self._client.send_any(cmd, operation="list offsets")
        return decode_list_offsets_response(resp)
