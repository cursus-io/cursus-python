from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from cursus.event_framework import EventEnvelope

SAGA_RUNNING = "RUNNING"
SAGA_WAITING = "WAITING"
SAGA_COMPLETED = "COMPLETED"
SAGA_COMPENSATING = "COMPENSATING"
SAGA_FAILED = "FAILED"
EFFECT_ENQUEUED = "ENQUEUED"
EFFECT_SUCCEEDED = "SUCCEEDED"
EFFECT_FAILED = "FAILED"


@dataclass
class EffectState:
    id: str = ""
    step: str = ""
    status: str = ""
    command_id: str = ""
    attempts: int = 0
    last_error: str = ""
    updated_at: datetime | None = None


@dataclass
class CompensationState:
    step: str = ""
    status: str = ""
    attempts: int = 0
    last_error: str = ""
    updated_at: datetime | None = None


@dataclass
class SagaState:
    id: str
    type: str
    association_key: str
    correlation_id: str = ""
    status: str = SAGA_RUNNING
    step: str = ""
    data: str = ""
    retry_count: int = 0
    last_error: str = ""
    updated_at: datetime | None = None
    version: int = 0
    effects: dict[str, EffectState] = field(default_factory=dict)
    compensation: CompensationState | None = None


@dataclass
class Command:
    id: str = ""
    effect_id: str = ""
    type: str = ""
    saga_id: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    payload: str = ""


class SagaTransaction(Protocol):
    def claim(self, consumer: str, event_id: str) -> bool: ...

    def load(self, saga_type: str, association_key: str) -> SagaState | None: ...

    def save_cas(self, state: SagaState, expected_version: int) -> None: ...

    def enqueue(self, command: Command) -> None: ...

    def complete(self, consumer: str, event_id: str) -> None: ...

    def fail(self, consumer: str, event_id: str, cause: Exception) -> None: ...


class SagaRepository(Protocol):
    def transact(self, apply: Callable[[SagaTransaction], None]) -> None: ...


SagaHandler = Callable[[SagaState, EventEnvelope], list[Command]]


@dataclass(frozen=True)
class SagaDefinition:
    type: str
    handlers: dict[str, SagaHandler]


class SagaManager:
    def __init__(self, definition: SagaDefinition, repository: SagaRepository) -> None:
        if not definition.type or not definition.handlers:
            raise ValueError("saga definition requires a type and handlers")
        if repository is None:
            raise ValueError("saga repository is required")
        self.definition = definition
        self.repository = repository

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _load_or_create(
        self, transaction: SagaTransaction, association_key: str
    ) -> tuple[SagaState, int]:
        if not association_key:
            raise ValueError("association key is required")
        state = transaction.load(self.definition.type, association_key)
        if state is None:
            state = SagaState(association_key, self.definition.type, association_key)
        return state, state.version

    @staticmethod
    def _save(transaction: SagaTransaction, state: SagaState, expected: int) -> None:
        state.version = expected + 1
        try:
            transaction.save_cas(state, expected)
        except Exception:
            state.version = expected
            raise

    def handle(self, event: EventEnvelope) -> None:
        if not event.event_id or not event.event_type:
            raise ValueError("saga event identity is incomplete")
        association = event.association_key or event.correlation_id or event.aggregate_id
        if not association:
            raise ValueError("saga association key is required")
        handler_failure: Exception | None = None

        def apply(transaction: SagaTransaction) -> None:
            nonlocal handler_failure
            if not transaction.claim(self.definition.type, event.event_id):
                return
            state, expected = self._load_or_create(transaction, association)
            state.correlation_id = state.correlation_id or event.correlation_id
            handler = self.definition.handlers.get(event.event_type)
            if handler is None:
                transaction.complete(self.definition.type, event.event_id)
                return
            try:
                commands = handler(state, event)
            except Exception as exc:
                state.retry_count += 1
                state.last_error = str(exc)
                state.updated_at = self._now()
                self._save(transaction, state, expected)
                transaction.fail(self.definition.type, event.event_id, exc)
                handler_failure = exc
                return
            state.last_error = ""
            for index, command in enumerate(commands):
                if not command.type:
                    raise ValueError(f"saga command type is required at index {index}")
                effect_id = command.effect_id or f"{event.event_id}:{index}"
                existing = state.effects.get(effect_id)
                if existing and existing.status in {EFFECT_ENQUEUED, EFFECT_SUCCEEDED}:
                    continue
                command.effect_id = effect_id
                command.saga_id = command.saga_id or state.id
                command.correlation_id = command.correlation_id or state.correlation_id
                command.causation_id = command.causation_id or event.event_id
                command.id = f"{self.definition.type}:{state.id}:{effect_id}"
                transaction.enqueue(command)
                effect = existing or EffectState()
                effect.id = effect_id
                effect.step = command.type
                effect.status = EFFECT_ENQUEUED
                effect.command_id = command.id
                effect.attempts += 1
                effect.last_error = ""
                effect.updated_at = self._now()
                state.effects[effect_id] = effect
            state.updated_at = self._now()
            self._save(transaction, state, expected)
            transaction.complete(self.definition.type, event.event_id)

        self.repository.transact(apply)
        if handler_failure is not None:
            raise handler_failure

    def acknowledge_effect(self, association: str, effect_id: str, command_id: str) -> None:
        self._update_effect(association, effect_id, command_id, EFFECT_SUCCEEDED, None)

    def fail_effect(
        self, association: str, effect_id: str, command_id: str, cause: Exception
    ) -> None:
        if cause is None:
            raise ValueError("effect failure is required")
        self._update_effect(association, effect_id, command_id, EFFECT_FAILED, cause)

    def _update_effect(
        self,
        association: str,
        effect_id: str,
        command_id: str,
        status: str,
        cause: Exception | None,
    ) -> None:
        if not effect_id or not command_id:
            raise ValueError("effect and command identities are required")

        def apply(transaction: SagaTransaction) -> None:
            state, expected = self._load_or_create(transaction, association)
            if effect_id not in state.effects:
                raise ValueError(f"effect {effect_id!r} does not exist")
            effect = state.effects[effect_id]
            if effect.command_id != command_id:
                raise ValueError(f"effect {effect_id!r} command fence mismatch")
            if effect.status == status:
                return
            if effect.status != EFFECT_ENQUEUED:
                raise ValueError(f"effect {effect_id!r} is not awaiting acknowledgement")
            effect.status = status
            effect.last_error = str(cause) if cause else ""
            effect.updated_at = self._now()
            state.updated_at = self._now()
            self._save(transaction, state, expected)

        self.repository.transact(apply)

    def start_compensation(
        self, association: str, step: str, cause: Exception | None = None
    ) -> SagaState:
        if not step:
            raise ValueError("compensation step is required")
        result: SagaState | None = None

        def apply(transaction: SagaTransaction) -> None:
            nonlocal result
            state, expected = self._load_or_create(transaction, association)
            compensation = state.compensation or CompensationState()
            compensation.step = step
            compensation.status = SAGA_COMPENSATING
            compensation.attempts += 1
            compensation.last_error = str(cause) if cause else ""
            compensation.updated_at = self._now()
            state.compensation = compensation
            state.status = SAGA_COMPENSATING
            state.updated_at = self._now()
            self._save(transaction, state, expected)
            result = state

        self.repository.transact(apply)
        assert result is not None
        return result

    def complete_compensation(self, association: str) -> None:
        self._update_compensation(association, SAGA_COMPLETED, None)

    def fail_compensation(self, association: str, cause: Exception) -> None:
        if cause is None:
            raise ValueError("compensation failure is required")
        self._update_compensation(association, SAGA_FAILED, cause)
        raise cause

    def _update_compensation(self, association: str, status: str, cause: Exception | None) -> None:
        def apply(transaction: SagaTransaction) -> None:
            state, expected = self._load_or_create(transaction, association)
            if state.compensation is None or not state.compensation.step:
                raise ValueError("compensation is not active")
            state.compensation.status = status
            state.compensation.last_error = str(cause) if cause else ""
            state.compensation.updated_at = self._now()
            state.status = status
            state.updated_at = self._now()
            self._save(transaction, state, expected)

        self.repository.transact(apply)


def compensation_command(
    command_type: str, state: SagaState, causation_id: str, payload: str
) -> Command:
    return Command(
        type=command_type,
        saga_id=state.id,
        correlation_id=state.correlation_id,
        causation_id=causation_id,
        payload=payload,
    )
