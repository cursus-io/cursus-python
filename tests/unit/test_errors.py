from cursus.errors import (
    ConnectionError as CursusConnectionError,
)
from cursus.errors import (
    ConsumerClosedError,
    CursusError,
    NotLeaderError,
    ProducerClosedError,
    ProtocolError,
    TopicNotFoundError,
)


def test_all_errors_inherit_from_cursus_error():
    for exc_cls in [
        CursusConnectionError,
        ProtocolError,
        ProducerClosedError,
        ConsumerClosedError,
        TopicNotFoundError,
        NotLeaderError,
    ]:
        err = exc_cls("test message")
        assert isinstance(err, CursusError)
        assert str(err) == "test message"


def test_cursus_error_is_exception():
    err = CursusError("base error")
    assert isinstance(err, Exception)
