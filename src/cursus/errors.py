class CursusError(Exception):
    pass


class ConnectionError(CursusError):
    pass


class ProtocolError(CursusError):
    pass


class BrokerError(CursusError):
    def __init__(self, code: str, fields: dict[str, str] | None = None, response: str = "") -> None:
        self.code = code
        self.fields = fields or {}
        self.response = response
        super().__init__(response or code)


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
