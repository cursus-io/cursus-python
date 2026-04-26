"""Cursus Python SDK — client library for the Cursus message broker."""

__version__ = "0.1.0"

from cursus.async_consumer import AsyncConsumer
from cursus.async_eventstore import AsyncEventStore
from cursus.async_producer import AsyncProducer
from cursus.config import ConsumerConfig, ProducerConfig
from cursus.consumer import Consumer
from cursus.errors import (
    ConnectionError,
    ConsumerClosedError,
    CursusError,
    NotLeaderError,
    ProducerClosedError,
    ProtocolError,
    TopicNotFoundError,
)
from cursus.eventstore import EventStore
from cursus.producer import Producer
from cursus.types import (
    AckResponse,
    Acks,
    AppendResult,
    ConsumerMode,
    Event,
    Message,
    Snapshot,
    StreamData,
    StreamEvent,
)

__all__ = [
    "ProducerConfig",
    "ConsumerConfig",
    "Acks",
    "ConsumerMode",
    "Message",
    "AckResponse",
    "Event",
    "StreamEvent",
    "Snapshot",
    "StreamData",
    "AppendResult",
    "CursusError",
    "ConnectionError",
    "ProtocolError",
    "ProducerClosedError",
    "ConsumerClosedError",
    "TopicNotFoundError",
    "NotLeaderError",
    "Producer",
    "Consumer",
    "EventStore",
    "AsyncProducer",
    "AsyncConsumer",
    "AsyncEventStore",
]
