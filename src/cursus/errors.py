class CursusError(Exception):
    pass


class ConnectionError(CursusError):
    pass


class ProducerOutcomeUnknownError(ConnectionError):
    """A publish may have reached the broker but has no trustworthy acknowledgement."""

    def __init__(self, partition: int, stage: str, cause: Exception) -> None:
        super().__init__(
            f"producer outcome is unknown for partition {partition} during {stage}: {cause}"
        )
        self.partition = partition
        self.stage = stage
        self.cause = cause


class ProtocolError(CursusError):
    pass


class BrokerError(CursusError):
    def __init__(
        self,
        code: str,
        error_class: str = "",
        retryable: bool = False,
        message: str = "",
        fields: dict[str, str] | None = None,
        *,
        response: str = "",
    ) -> None:
        rendered = message or response or code
        super().__init__(rendered)
        self.code = code
        self.error_class = error_class
        self.retryable = retryable
        self.fields = dict(fields or {})
        self.response = response or rendered

    def can_retry(self, *, idempotent: bool) -> bool:
        return self.retryable and idempotent


class AuthenticationRequiredError(BrokerError):
    pass


class AuthorizationDeniedError(BrokerError):
    @property
    def topic(self) -> str:
        return self.fields.get("topic", "")

    @property
    def operation(self) -> str:
        return self.fields.get("operation", "")


class ValidationError(BrokerError):
    pass


class ProducerClosedError(CursusError):
    pass


class ConsumerClosedError(CursusError):
    pass


class TopicNotFoundError(CursusError):
    pass


class NotLeaderError(CursusError):
    pass


class ProducerFencedError(BrokerError):
    pass
