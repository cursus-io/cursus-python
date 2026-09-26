"""Cursus Python SDK — client library for the Cursus message broker."""

__version__ = "0.1.0"

from cursus.admin import (
    AdminClient,
    AdminConfig,
    DeleteTopicOptions,
    DeleteTopicResult,
    TopicCleanupPolicy,
    TopicDefinition,
    TopicDefinitionPatch,
    TruncateTopicOptions,
    TruncateTopicResult,
)
from cursus.async_consumer import AsyncConsumer
from cursus.async_eventstore import AsyncEventStore
from cursus.async_producer import AsyncProducer
from cursus.config import ConsumerConfig, ProducerConfig
from cursus.consumer import Consumer, TransactionalOffsetMetadata
from cursus.errors import (
    AuthenticationRequiredError,
    AuthorizationDeniedError,
    BrokerError,
    ConnectionError,
    ConsumerClosedError,
    CursusError,
    NotLeaderError,
    ProducerClosedError,
    ProducerFencedError,
    ProtocolError,
    TopicNotFoundError,
    ValidationError,
)
from cursus.event_framework import (
    AggregateRepository,
    DeadlineManager,
    EventEnvelope,
    RetryPolicy,
    UpcasterRegistry,
    replay,
)
from cursus.eventstore import EventStore
from cursus.metrics import ClientMetrics, MetricsSnapshot, classify_error
from cursus.offsets import OffsetClient
from cursus.producer import Producer
from cursus.saga import (
    Command,
    CompensationState,
    EffectState,
    SagaDefinition,
    SagaState,
)
from cursus.broker_saga import (
    BrokerSagaCommandEnvelope,
    BrokerSagaHistoryDraft,
    BrokerSagaInput,
    BrokerSagaRuntime,
    BrokerSagaRuntimeConfig,
    BrokerSagaStateRecord,
    BrokerSagaTopics,
)
from cursus.transaction import TransactionalProducer
from cursus.types import (
    AckResponse,
    Acks,
    AppendResult,
    AutoOffsetReset,
    ConsumerMode,
    Event,
    IsolationLevel,
    Message,
    PartitionOffsetRange,
    ProducerSession,
    Snapshot,
    StreamData,
    StreamEvent,
    TransactionStatus,
)

__all__ = [
    "ProducerConfig",
    "AdminClient",
    "AdminConfig",
    "TopicCleanupPolicy",
    "TopicDefinitionPatch",
    "TopicDefinition",
    "DeleteTopicOptions",
    "DeleteTopicResult",
    "TruncateTopicOptions",
    "TruncateTopicResult",
    "ConsumerConfig",
    "Acks",
    "ConsumerMode",
    "AutoOffsetReset",
    "IsolationLevel",
    "Message",
    "PartitionOffsetRange",
    "ProducerSession",
    "TransactionStatus",
    "AckResponse",
    "Event",
    "StreamEvent",
    "Snapshot",
    "StreamData",
    "AppendResult",
    "CursusError",
    "BrokerError",
    "AuthenticationRequiredError",
    "AuthorizationDeniedError",
    "ValidationError",
    "ConnectionError",
    "ProtocolError",
    "ProducerClosedError",
    "ProducerFencedError",
    "ConsumerClosedError",
    "TopicNotFoundError",
    "NotLeaderError",
    "Producer",
    "Consumer",
    "TransactionalOffsetMetadata",
    "EventStore",
    "ClientMetrics",
    "MetricsSnapshot",
    "classify_error",
    "EventEnvelope",
    "AggregateRepository",
    "RetryPolicy",
    "UpcasterRegistry",
    "DeadlineManager",
    "replay",
    "SagaState",
    "EffectState",
    "CompensationState",
    "Command",
    "SagaDefinition",
    "BrokerSagaTopics",
    "BrokerSagaRuntimeConfig",
    "BrokerSagaInput",
    "BrokerSagaHistoryDraft",
    "BrokerSagaStateRecord",
    "BrokerSagaCommandEnvelope",
    "BrokerSagaRuntime",
    "OffsetClient",
    "TransactionalProducer",
    "AsyncProducer",
    "AsyncConsumer",
    "AsyncEventStore",
]
