"""Database-neutral, opt-in Saga execution history v1 support.

The classes in this module deliberately contain no SQL or broker dependency.
Applications provide transaction-scoped stores; database-specific packages such
as :mod:`cursus.sagapg` implement those stores atomically.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

HISTORY_SCHEMA_VERSION = 1
RUN_STARTED = "run.started"
RUN_WAITING = "run.waiting"
STEP_STARTED = "step.started"
STEP_COMPLETED = "step.completed"
STEP_FAILED = "step.failed"
COMMAND_ENQUEUED = "command.enqueued"
COMMAND_PUBLISHED = "command.published"
COMMAND_SUCCEEDED = "command.succeeded"
COMMAND_FAILED = "command.failed"
COMPENSATION_STARTED = "compensation.started"
COMPENSATION_COMPLETED = "compensation.completed"
COMPENSATION_FAILED = "compensation.failed"
RUN_COMPLETED = "run.completed"
RUN_FAILED = "run.failed"
RUN_COMPENSATED = "run.compensated"

RUNNING = "RUNNING"
WAITING = "WAITING"
COMPLETED = "COMPLETED"
COMPENSATING = "COMPENSATING"
FAILED = "FAILED"
SUCCEEDED = "SUCCEEDED"
COMPENSATED = "COMPENSATED"

PENDING = "PENDING"
EFFECT_SUCCEEDED = "SUCCEEDED"
EFFECT_FAILED = "FAILED"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    timespec = "seconds" if value.microsecond == 0 else "microseconds"
    return value.isoformat(timespec=timespec).replace("+00:00", "Z")


@dataclass(frozen=True)
class SagaHistoryEvent:
    """The language-independent Cursus Saga execution-history v1 record."""

    environment_id: str
    service_name: str
    saga_type: str
    saga_id: str
    run_id: str
    sequence: int
    event_type: str
    occurred_at: datetime
    recorded_at: datetime
    history_event_id: str = field(default_factory=lambda: str(uuid4()))
    history_schema_version: int = HISTORY_SCHEMA_VERSION
    step_id: str = ""
    attempt: int | None = None
    command_id: str = ""
    effect_id: str = ""
    source_event_id: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    source_topic: str = ""
    source_partition: int | None = None
    source_offset: int | None = None
    aggregate_type: str = ""
    aggregate_id: str = ""
    aggregate_version: int | None = None
    payload: str = ""
    error: str = ""

    def __post_init__(self) -> None:
        if self.history_schema_version != HISTORY_SCHEMA_VERSION:
            raise ValueError("history_schema_version must be 1")
        if not all(
            (self.environment_id, self.service_name, self.saga_type, self.saga_id, self.run_id)
        ):
            raise ValueError("Saga history identity fields are required")
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        if not self.event_type:
            raise ValueError("event_type is required")
        if self.attempt is not None and self.attempt < 1:
            raise ValueError("attempt must be positive when supplied")
        if self.source_partition is not None and self.source_partition < 0:
            raise ValueError("source_partition must be non-negative")
        if self.source_offset is not None and self.source_offset < 0:
            raise ValueError("source_offset must be non-negative")
        if self.aggregate_version is not None and self.aggregate_version < 0:
            raise ValueError("aggregate_version must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        """Return the schema-v1 JSON object, omitting empty optional fields."""

        result: dict[str, Any] = {
            "history_schema_version": self.history_schema_version,
            "history_event_id": self.history_event_id,
            "environment_id": self.environment_id,
            "service_name": self.service_name,
            "saga_type": self.saga_type,
            "saga_id": self.saga_id,
            "run_id": self.run_id,
            "sequence": str(self.sequence),
            "event_type": self.event_type,
            "occurred_at": _timestamp(self.occurred_at),
            "recorded_at": _timestamp(self.recorded_at),
        }
        optional = {
            "step_id": self.step_id,
            "attempt": self.attempt,
            "command_id": self.command_id,
            "effect_id": self.effect_id,
            "source_event_id": self.source_event_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "source_topic": self.source_topic,
            "source_partition": self.source_partition,
            "source_offset": None if self.source_offset is None else str(self.source_offset),
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_version": None
            if self.aggregate_version is None
            else str(self.aggregate_version),
            "payload": self.payload,
            "error": self.error,
        }
        result.update({key: value for key, value in optional.items() if value not in (None, "")})
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    event_type: str
    association_key: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    source_topic: str = ""
    source_partition: int | None = None
    source_offset: int | None = None
    aggregate_type: str = ""
    aggregate_id: str = ""
    aggregate_version: int | None = None
    payload: str = ""


@dataclass
class Command:
    type: str
    payload: str = ""
    effect_id: str = ""
    command_id: str = ""
    saga_id: str = ""
    correlation_id: str = ""
    causation_id: str = ""


@dataclass
class EffectState:
    effect_id: str
    step_id: str
    status: str = PENDING
    command_id: str = ""
    published: bool = False
    attempts: int = 0
    last_error: str = ""
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass
class CompensationState:
    step_id: str
    status: str = COMPENSATING
    attempts: int = 0
    last_error: str = ""
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass
class SagaState:
    saga_id: str
    saga_type: str
    association_key: str
    correlation_id: str = ""
    status: str = RUNNING
    step_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    last_error: str = ""
    run_id: str = field(default_factory=lambda: str(uuid4()))
    next_sequence: int = 0
    outcome: str = ""
    updated_at: datetime = field(default_factory=_utcnow)
    effects: dict[str, EffectState] = field(default_factory=dict)
    compensation: CompensationState | None = None


class InboxStore(Protocol):
    def claim(self, consumer_name: str, event_id: str) -> bool: ...
    def complete(self, consumer_name: str, event_id: str) -> None: ...
    def fail(self, consumer_name: str, event_id: str, cause: Exception) -> None: ...


class SagaStateStore(Protocol):
    def load_for_update(self, saga_type: str, saga_id: str) -> SagaState | None: ...
    def save(self, state: SagaState) -> None: ...


class CommandOutboxStore(Protocol):
    def enqueue(self, saga_type: str, command: Command) -> None: ...


class SagaHistoryStore(Protocol):
    def append(self, event: SagaHistoryEvent) -> None: ...


@dataclass(frozen=True)
class SagaTransactionStores:
    inbox: InboxStore
    state: SagaStateStore
    command_outbox: CommandOutboxStore
    history: SagaHistoryStore


class SagaTransaction(Protocol):
    def run(self, operation: Callable[[SagaTransactionStores], Any]) -> Any: ...


SagaHandler = Callable[[SagaState, EventEnvelope], list[Command]]


@dataclass(frozen=True)
class SagaDefinition:
    saga_type: str
    handlers: dict[str, SagaHandler]


@dataclass(frozen=True)
class SagaHistoryOptions:
    environment_id: str
    service_name: str

    def __post_init__(self) -> None:
        if not self.environment_id or not self.service_name:
            raise ValueError("environment_id and service_name are required")


class TransactionalSagaManager:
    """Opt-in manager which requires one atomic, service-owned transaction."""

    def __init__(
        self, definition: SagaDefinition, transaction: SagaTransaction, options: SagaHistoryOptions
    ):
        if not definition.saga_type or not definition.handlers:
            raise ValueError("Saga definition requires a type and handlers")
        self.definition = definition
        self.transaction = transaction
        self.options = options

    def handle(self, event: EventEnvelope) -> None:
        if not event.event_id or not event.event_type:
            raise ValueError("event_id and event_type are required")
        association_key = event.association_key or event.correlation_id or event.aggregate_id
        if not association_key:
            raise ValueError("event requires association_key, correlation_id, or aggregate_id")
        try:
            self.transaction.run(lambda stores: self._handle(stores, association_key, event))
        except Exception as cause:
            self._record_rolled_back_failure(association_key, event, cause)
            raise

    def _handle(
        self, stores: SagaTransactionStores, association_key: str, event: EventEnvelope
    ) -> None:
        if not stores.inbox.claim(self.definition.saga_type, event.event_id):
            return
        state = stores.state.load_for_update(self.definition.saga_type, association_key)
        if state is None:
            state = SagaState(
                saga_id=association_key,
                saga_type=self.definition.saga_type,
                association_key=association_key,
                correlation_id=event.correlation_id,
            )
            self._record(stores, state, event, RUN_STARTED)
        handler = self.definition.handlers.get(event.event_type)
        if handler is None:
            stores.state.save(state)
            stores.inbox.complete(self.definition.saga_type, event.event_id)
            return
        attempt = state.retry_count + 1
        commands = handler(state, event)
        step_id = state.step_id or event.event_type
        self._record(stores, state, event, STEP_STARTED, step_id=step_id, attempt=attempt)
        for index, command in enumerate(commands):
            effect_id = command.effect_id or f"{event.event_id}:{index}"
            effect = state.effects.get(effect_id)
            if effect is not None and effect.status != EFFECT_FAILED:
                continue
            command.effect_id = effect_id
            command.saga_id = command.saga_id or state.saga_id
            command.correlation_id = command.correlation_id or state.correlation_id
            command.causation_id = command.causation_id or event.event_id
            command.command_id = command.command_id or str(uuid4())
            effect = effect or EffectState(effect_id=effect_id, step_id=command.type)
            effect.step_id, effect.attempts, effect.status = (
                command.type,
                effect.attempts + 1,
                PENDING,
            )
            effect.command_id, effect.last_error, effect.updated_at = (
                command.command_id,
                "",
                _utcnow(),
            )
            state.effects[effect_id] = effect
            stores.command_outbox.enqueue(self.definition.saga_type, command)
            self._record(
                stores, state, event, COMMAND_ENQUEUED, command.type, effect.attempts, command
            )
        self._record(stores, state, event, STEP_COMPLETED, step_id=step_id, attempt=attempt)
        if state.status == WAITING:
            self._record(stores, state, event, RUN_WAITING, step_id=step_id, attempt=attempt)
        elif state.status == COMPLETED:
            state.outcome = SUCCEEDED
            self._record(stores, state, event, RUN_COMPLETED)
        elif state.status == FAILED:
            state.outcome = FAILED
            self._record(stores, state, event, RUN_FAILED, error=state.last_error)
        state.updated_at = _utcnow()
        stores.state.save(state)
        stores.inbox.complete(self.definition.saga_type, event.event_id)

    def _record_rolled_back_failure(
        self, association_key: str, event: EventEnvelope, cause: Exception
    ) -> None:
        """Failure records are written only after the successful attempt rolled back."""

        def persist(stores: SagaTransactionStores) -> None:
            state = stores.state.load_for_update(self.definition.saga_type, association_key)
            if state is not None:
                state.retry_count += 1
                state.last_error, state.updated_at = str(cause), _utcnow()
                self._record(
                    stores,
                    state,
                    event,
                    STEP_FAILED,
                    state.step_id,
                    state.retry_count,
                    error=str(cause),
                )
                stores.state.save(state)
            stores.inbox.fail(self.definition.saga_type, event.event_id, cause)

        try:
            self.transaction.run(persist)
        except Exception:
            # Preserve the handler exception. Infrastructure failures are still
            # observable through the caller's normal retry/error handling.
            pass

    def start_new_run(self, association_key: str) -> SagaState:
        if not association_key:
            raise ValueError("association_key is required")

        def start(stores: SagaTransactionStores) -> SagaState:
            state = stores.state.load_for_update(self.definition.saga_type, association_key)
            if state is None:
                raise ValueError("cannot start a new run before an initial run exists")
            if state.status not in (COMPLETED, FAILED):
                raise ValueError(f"cannot start a new run while status is {state.status}")
            state.run_id, state.next_sequence = str(uuid4()), 0
            state.status, state.outcome, state.step_id, state.retry_count, state.last_error = (
                RUNNING,
                "",
                "",
                0,
                "",
            )
            state.effects, state.compensation, state.updated_at = {}, None, _utcnow()
            self._record(stores, state, EventEnvelope("", ""), RUN_STARTED)
            stores.state.save(state)
            return state

        return self.transaction.run(start)

    def record_command_published(self, association_key: str, effect_id: str) -> None:
        def record(stores: SagaTransactionStores) -> None:
            state, effect = self._effect(stores, association_key, effect_id)
            if effect.published or effect.status != PENDING:
                return
            command = Command(
                effect.step_id,
                effect_id=effect.effect_id,
                command_id=effect.command_id,
                saga_id=state.saga_id,
                correlation_id=state.correlation_id,
            )
            self._record(
                stores,
                state,
                EventEnvelope("", ""),
                COMMAND_PUBLISHED,
                effect.step_id,
                effect.attempts,
                command,
            )
            effect.published, effect.updated_at, state.updated_at = True, _utcnow(), _utcnow()
            stores.state.save(state)

        self.transaction.run(record)

    def record_effect_result(
        self, association_key: str, effect_id: str, succeeded: bool, error: Exception | None = None
    ) -> None:
        if succeeded and error is not None:
            raise ValueError("successful effect cannot include an error")

        def record(stores: SagaTransactionStores) -> None:
            state, effect = self._effect(stores, association_key, effect_id)
            if effect.status in (EFFECT_SUCCEEDED, EFFECT_FAILED):
                return
            command = Command(
                effect.step_id,
                effect_id=effect.effect_id,
                command_id=effect.command_id,
                saga_id=state.saga_id,
                correlation_id=state.correlation_id,
            )
            effect.status = EFFECT_SUCCEEDED if succeeded else EFFECT_FAILED
            effect.last_error = "" if succeeded else str(error or "effect failed")
            self._record(
                stores,
                state,
                EventEnvelope("", ""),
                COMMAND_SUCCEEDED if succeeded else COMMAND_FAILED,
                effect.step_id,
                effect.attempts,
                command,
                effect.last_error,
            )
            effect.updated_at, state.updated_at = _utcnow(), _utcnow()
            stores.state.save(state)

        self.transaction.run(record)

    def start_compensation(
        self, association_key: str, step_id: str, error: Exception | None = None
    ) -> SagaState:
        if not step_id:
            raise ValueError("compensation step_id is required")

        def record(stores: SagaTransactionStores) -> SagaState:
            state = self._state(stores, association_key)
            compensation = state.compensation or CompensationState(step_id=step_id)
            compensation.step_id, compensation.status, compensation.attempts = (
                step_id,
                COMPENSATING,
                compensation.attempts + 1,
            )
            compensation.last_error, compensation.updated_at = str(error or ""), _utcnow()
            state.compensation, state.status, state.outcome, state.updated_at = (
                compensation,
                COMPENSATING,
                "",
                _utcnow(),
            )
            self._record(
                stores,
                state,
                EventEnvelope("", ""),
                COMPENSATION_STARTED,
                step_id,
                compensation.attempts,
                Command(step_id, effect_id=step_id),
                compensation.last_error,
            )
            stores.state.save(state)
            return state

        return self.transaction.run(record)

    def complete_compensation(self, association_key: str) -> None:
        def record(stores: SagaTransactionStores) -> None:
            state = self._state(stores, association_key)
            if state.compensation is None:
                raise ValueError("compensation is not active")
            compensation = state.compensation
            compensation.status, compensation.last_error, compensation.updated_at = (
                COMPLETED,
                "",
                _utcnow(),
            )
            state.status, state.outcome, state.updated_at = COMPLETED, COMPENSATED, _utcnow()
            self._record(
                stores,
                state,
                EventEnvelope("", ""),
                COMPENSATION_COMPLETED,
                compensation.step_id,
                compensation.attempts,
                Command(compensation.step_id, effect_id=compensation.step_id),
            )
            self._record(stores, state, EventEnvelope("", ""), RUN_COMPENSATED)
            stores.state.save(state)

        self.transaction.run(record)

    def fail_compensation(self, association_key: str, error: Exception) -> None:
        def record(stores: SagaTransactionStores) -> None:
            state = self._state(stores, association_key)
            if state.compensation is None:
                raise ValueError("compensation is not active")
            compensation = state.compensation
            compensation.status, compensation.last_error, compensation.updated_at = (
                FAILED,
                str(error),
                _utcnow(),
            )
            state.status, state.outcome, state.updated_at = FAILED, FAILED, _utcnow()
            self._record(
                stores,
                state,
                EventEnvelope("", ""),
                COMPENSATION_FAILED,
                compensation.step_id,
                compensation.attempts,
                Command(compensation.step_id, effect_id=compensation.step_id),
                str(error),
            )
            self._record(stores, state, EventEnvelope("", ""), RUN_FAILED, error=str(error))
            stores.state.save(state)

        self.transaction.run(record)

    def _state(self, stores: SagaTransactionStores, association_key: str) -> SagaState:
        state = stores.state.load_for_update(self.definition.saga_type, association_key)
        if state is None:
            raise ValueError("Saga state not found")
        return state

    def _effect(
        self, stores: SagaTransactionStores, association_key: str, effect_id: str
    ) -> tuple[SagaState, EffectState]:
        if not association_key or not effect_id:
            raise ValueError("association_key and effect_id are required")
        state = self._state(stores, association_key)
        effect = state.effects.get(effect_id)
        if effect is None:
            raise ValueError(f"effect {effect_id} not found")
        return state, effect

    def _record(
        self,
        stores: SagaTransactionStores,
        state: SagaState,
        source: EventEnvelope,
        event_type: str,
        step_id: str = "",
        attempt: int | None = None,
        command: Command | None = None,
        error: str = "",
    ) -> None:
        state.next_sequence += 1
        now = _utcnow()
        command = command or Command("")
        stores.history.append(
            SagaHistoryEvent(
                environment_id=self.options.environment_id,
                service_name=self.options.service_name,
                saga_type=state.saga_type,
                saga_id=state.saga_id,
                run_id=state.run_id,
                sequence=state.next_sequence,
                event_type=event_type,
                occurred_at=now,
                recorded_at=now,
                step_id=step_id,
                attempt=attempt,
                command_id=command.command_id,
                effect_id=command.effect_id,
                source_event_id=source.event_id,
                correlation_id=source.correlation_id,
                causation_id=source.causation_id,
                source_topic=source.source_topic,
                source_partition=source.source_partition,
                source_offset=source.source_offset,
                aggregate_type=source.aggregate_type,
                aggregate_id=source.aggregate_id,
                aggregate_version=source.aggregate_version,
                payload=command.payload,
                error=error,
            )
        )
