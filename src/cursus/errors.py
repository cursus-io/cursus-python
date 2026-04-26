class CursusError(Exception):
    pass


class ConnectionError(CursusError):
    pass


class ProtocolError(CursusError):
    pass


class ProducerClosedError(CursusError):
    pass


class ConsumerClosedError(CursusError):
    pass


class TopicNotFoundError(CursusError):
    pass


class NotLeaderError(CursusError):
    pass
