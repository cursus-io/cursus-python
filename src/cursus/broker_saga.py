"""Broker-native Saga runtime.

This opt-in runtime persists a Saga run as an event-store stream and uses one
Cursus broker transaction for state, commands, history, and the input offset.
It intentionally has no dependency on PostgreSQL, MySQL, or a service outbox.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid5

from cursus.broker_saga_types import (
    COMMAND_ENQUEUED,
    PENDING,
    RUN_STARTED,
    STEP_FAILED,
    Command,
    CompensationState,
    EffectState,
    EventEnvelope,
    SagaHistoryEvent,
    SagaState,
)
from cursus.eventstore import EventStore
from cursus.transaction import TransactionalProducer

_NAMESPACE = UUID("2ce850f6-b151-5e5a-a160-6b8d82527d54")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _id(*parts: str) -> str:
    return str(uuid5(_NAMESPACE, "\0".join(parts)))


@dataclass(frozen=True)
class BrokerSagaTopics:
    inbox: str = "cursus.saga-inbox.v1"
    state: str = "cursus.saga-state.v1"
    commands: str = "cursus.saga-commands.v1"
    history: str = "cursus.saga-history.v1"

    def __post_init__(self) -> None:
        for name, topic in self.__dict__.items():
            if not topic or topic.startswith("__"):
                raise ValueError(f"broker saga {name} topic must be public and non-empty")


@dataclass(frozen=True)
class BrokerSagaRuntimeConfig:
    saga_type: str
    environment_id: str
    service_name: str
    topics: BrokerSagaTopics = field(default_factory=BrokerSagaTopics)

    def __post_init__(self) -> None:
        if not self.saga_type or any(char.isspace() for char in self.saga_type):
            raise ValueError("saga_type is required and must not contain whitespace")
        if not self.environment_id or not self.service_name:
            raise ValueError("environment_id and service_name are required")


@dataclass(frozen=True)
class BrokerSagaInput:
    saga_id: str
    run_id: str
    event: EventEnvelope
    topic: str
    partition: int
    offset: int
    group: str
    member: str
    generation: int

    def __post_init__(self) -> None:
        if (
            not self.saga_id
            or not self.run_id
            or any(char.isspace() for char in self.saga_id + self.run_id)
        ):
            raise ValueError("explicit saga_id and run_id without whitespace are required")
        if not self.event.event_id or not self.event.event_type:
            raise ValueError("event_id and event_type are required")
        if not self.topic or self.partition < 0 or self.offset < 0:
            raise ValueError("input topic, non-negative partition, and offset are required")
        if not self.group or not self.member or self.generation < 0:
            raise ValueError("consumer group membership is required")


@dataclass(frozen=True)
class BrokerSagaHistoryDraft:
    event_type: str
    step_id: str = ""
    attempt: int | None = None
    command: Command | None = None
    payload: str = ""
    error: str = ""


@dataclass
class BrokerSagaStateRecord:
    saga_type: str
    saga_id: str
    run_id: str
    state: SagaState
    processed_event_ids: list[str] = field(default_factory=list)
    recorded_at: datetime = field(default_factory=_utcnow)
    schema_version: int = 1

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "saga_type": self.saga_type,
                "saga_id": self.saga_id,
                "run_id": self.run_id,
                "state": _state_to_dict(self.state),
                "processed_event_ids": self.processed_event_ids,
                "recorded_at": _stamp(self.recorded_at),
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, raw: str) -> BrokerSagaStateRecord:
        value = json.loads(raw)
        if value.get("schema_version") != 1:
            raise ValueError("unsupported broker saga state schema")
        return cls(
            saga_type=value["saga_type"],
            saga_id=value["saga_id"],
            run_id=value["run_id"],
            state=_state_from_dict(value["state"]),
            processed_event_ids=list(value.get("processed_event_ids", [])),
            recorded_at=_parse_stamp(value["recorded_at"]),
        )


@dataclass(frozen=True)
class BrokerSagaCommandEnvelope:
    command_id: str
    effect_id: str
    command_type: str
    saga_type: str
    saga_id: str
    run_id: str
    correlation_id: str = ""
    causation_id: str = ""
    payload: str = ""
    schema_version: int = 1

    def to_json(self) -> str:
        return json.dumps(self.__dict__, separators=(",", ":"))


BrokerSagaTransitionHandler = Callable[
    [SagaState, EventEnvelope], tuple[list[Command], list[BrokerSagaHistoryDraft]]
]


class BrokerSagaRuntime:
    """A synchronous, DB-free Saga transaction boundary.

    Use this with ``ConsumerConfig(enable_auto_commit=False)``. The runtime
    commits the source offset only through ``SEND_OFFSETS_TO_TXN``.
    """

    def __init__(
        self,
        config: BrokerSagaRuntimeConfig,
        state_store: EventStore,
        producer_factory: Callable[[str], TransactionalProducer],
    ) -> None:
        self.config = config
        self._state_store = state_store
        self._producer_factory = producer_factory

    def stream_key(self, saga_id: str, run_id: str) -> str:
        return f"{self.config.saga_type}:{saga_id}:{run_id}"

    def handle(self, input: BrokerSagaInput, handler: BrokerSagaTransitionHandler) -> None:
        record, version = self._load(input.saga_id, input.run_id)
        if input.event.event_id in record.processed_event_ids:
            self._commit_duplicate(input)
            return
        new_run = version == 0
        if new_run:
            record = BrokerSagaStateRecord(
                saga_type=self.config.saga_type,
                saga_id=input.saga_id,
                run_id=input.run_id,
                state=SagaState(
                    saga_id=input.saga_id,
                    saga_type=self.config.saga_type,
                    association_key=input.saga_id,
                    correlation_id=input.event.correlation_id,
                    run_id=input.run_id,
                ),
            )
        # Preserve the last durable state until the handler completes. A failed
        # handler may mutate the state object it receives, but those mutations
        # must not become part of the retryable failure record.
        transition_state = _clone_state(record.state)
        try:
            commands, drafts = handler(transition_state, input.event)
        except Exception as cause:
            if not new_run:
                try:
                    self._record_failure(input, record, version, cause)
                except Exception:
                    pass
            raise

        record.state = transition_state
        now = _utcnow()
        if new_run:
            drafts = [BrokerSagaHistoryDraft(RUN_STARTED), *drafts]
        prepared = [
            self._prepare_command(record.state, input.event.event_id, index, command)
            for index, command in enumerate(commands)
        ]
        drafts.extend(
            BrokerSagaHistoryDraft(
                COMMAND_ENQUEUED, command.type, command=command, payload=command.payload
            )
            for command in prepared
        )
        record.processed_event_ids.append(input.event.event_id)
        record.recorded_at, record.state.updated_at = now, now
        history = self._materialize_history(record.state, input, drafts, now)
        self._commit(input, record, version + 1, prepared, history, acknowledge=True)

    def _load(self, saga_id: str, run_id: str) -> tuple[BrokerSagaStateRecord, int]:
        stream = self._state_store.read_stream(self.stream_key(saga_id, run_id))
        if not stream.events:
            return BrokerSagaStateRecord("", "", "", SagaState("", "", "")), 0
        record = BrokerSagaStateRecord.from_json(stream.events[-1].payload)
        if (record.saga_type, record.saga_id, record.run_id) != (
            self.config.saga_type,
            saga_id,
            run_id,
        ):
            raise ValueError("broker saga state stream identity mismatch")
        return record, stream.events[-1].version

    def _prepare_command(
        self, state: SagaState, causation_id: str, index: int, command: Command
    ) -> Command:
        command.effect_id = command.effect_id or f"{causation_id}:{index}"
        command.command_id = command.command_id or _id(
            "command", self.config.saga_type, state.saga_id, state.run_id, command.effect_id
        )
        command.saga_id = command.saga_id or state.saga_id
        command.correlation_id = command.correlation_id or state.correlation_id
        command.causation_id = command.causation_id or causation_id
        effect = state.effects.get(command.effect_id) or EffectState(
            command.effect_id, command.type
        )
        effect.step_id = command.type
        effect.status = PENDING
        effect.command_id = command.command_id
        effect.attempts += 1
        effect.last_error = ""
        effect.updated_at = _utcnow()
        state.effects[command.effect_id] = effect
        return command

    def _materialize_history(
        self,
        state: SagaState,
        input: BrokerSagaInput,
        drafts: list[BrokerSagaHistoryDraft],
        now: datetime,
    ) -> list[SagaHistoryEvent]:
        history: list[SagaHistoryEvent] = []
        for draft in drafts:
            state.next_sequence += 1
            command = draft.command or Command("")
            history.append(
                SagaHistoryEvent(
                    environment_id=self.config.environment_id,
                    service_name=self.config.service_name,
                    saga_type=self.config.saga_type,
                    saga_id=state.saga_id,
                    run_id=state.run_id,
                    sequence=state.next_sequence,
                    event_type=draft.event_type,
                    occurred_at=now,
                    recorded_at=now,
                    history_event_id=_id(
                        "history",
                        self.config.saga_type,
                        state.saga_id,
                        state.run_id,
                        str(state.next_sequence),
                    ),
                    step_id=draft.step_id,
                    attempt=draft.attempt,
                    command_id=command.command_id,
                    effect_id=command.effect_id,
                    source_event_id=input.event.event_id,
                    correlation_id=input.event.correlation_id,
                    causation_id=input.event.causation_id,
                    source_topic=input.topic,
                    source_partition=input.partition,
                    source_offset=input.offset,
                    aggregate_type=input.event.aggregate_type,
                    aggregate_id=input.event.aggregate_id,
                    aggregate_version=input.event.aggregate_version,
                    payload=draft.payload,
                    error=draft.error,
                )
            )
        return history

    def _commit_duplicate(self, input: BrokerSagaInput) -> None:
        producer = self._producer_factory(self._transaction_id(input, "duplicate"))
        with producer:
            producer.send_offsets_to_transaction(
                input.topic,
                input.group,
                input.member,
                input.generation,
                {input.partition: input.offset + 1},
            )

    def _commit(
        self,
        input: BrokerSagaInput,
        record: BrokerSagaStateRecord,
        expected_version: int,
        commands: list[Command],
        history: list[SagaHistoryEvent],
        *,
        acknowledge: bool,
    ) -> None:
        producer = self._producer_factory(self._transaction_id(input, "apply"))
        with producer:
            producer.append_stream(
                self.config.topics.state,
                self.stream_key(record.saga_id, record.run_id),
                expected_version,
                record.to_json(),
                event_type="saga.state.transitioned",
            )
            for command in commands:
                producer.publish(
                    self.config.topics.commands,
                    BrokerSagaCommandEnvelope(
                        command.command_id,
                        command.effect_id,
                        command.type,
                        self.config.saga_type,
                        record.saga_id,
                        record.run_id,
                        command.correlation_id,
                        command.causation_id,
                        command.payload,
                    ).to_json(),
                    key=command.command_id,
                )
            for event in history:
                producer.publish(
                    self.config.topics.history, event.to_json(), key=event.history_event_id
                )
            if acknowledge:
                producer.send_offsets_to_transaction(
                    input.topic,
                    input.group,
                    input.member,
                    input.generation,
                    {input.partition: input.offset + 1},
                )

    def _record_failure(
        self, input: BrokerSagaInput, record: BrokerSagaStateRecord, version: int, cause: Exception
    ) -> None:
        now = _utcnow()
        record.state.retry_count += 1
        record.state.last_error = str(cause)
        record.state.updated_at, record.recorded_at = now, now
        history = self._materialize_history(
            record.state,
            input,
            [
                BrokerSagaHistoryDraft(
                    STEP_FAILED, record.state.step_id, record.state.retry_count, error=str(cause)
                )
            ],
            now,
        )
        self._commit_state_only(input, record, version + 1, history)

    def _commit_state_only(
        self,
        input: BrokerSagaInput,
        record: BrokerSagaStateRecord,
        expected_version: int,
        history: list[SagaHistoryEvent],
    ) -> None:
        producer = self._producer_factory(self._transaction_id(input, "failure"))
        with producer:
            producer.append_stream(
                self.config.topics.state,
                self.stream_key(record.saga_id, record.run_id),
                expected_version,
                record.to_json(),
                event_type="saga.state.failed",
            )
            for event in history:
                producer.publish(
                    self.config.topics.history, event.to_json(), key=event.history_event_id
                )

    def _transaction_id(self, input: BrokerSagaInput, phase: str) -> str:
        return "saga-" + _id(
            "transaction",
            self.config.service_name,
            self.config.saga_type,
            input.group,
            input.saga_id,
            input.run_id,
            input.topic,
            str(input.partition),
            str(input.offset),
            phase,
        )


def _state_to_dict(state: SagaState) -> dict[str, Any]:
    return {
        "saga_id": state.saga_id,
        "saga_type": state.saga_type,
        "association_key": state.association_key,
        "correlation_id": state.correlation_id,
        "status": state.status,
        "step_id": state.step_id,
        "data": state.data,
        "retry_count": state.retry_count,
        "last_error": state.last_error,
        "run_id": state.run_id,
        "next_sequence": state.next_sequence,
        "outcome": state.outcome,
        "updated_at": _stamp(state.updated_at),
        "effects": {
            key: {
                "effect_id": value.effect_id,
                "step_id": value.step_id,
                "status": value.status,
                "command_id": value.command_id,
                "published": value.published,
                "attempts": value.attempts,
                "last_error": value.last_error,
                "updated_at": _stamp(value.updated_at),
            }
            for key, value in state.effects.items()
        },
        "compensation": None
        if state.compensation is None
        else {
            "step_id": state.compensation.step_id,
            "status": state.compensation.status,
            "attempts": state.compensation.attempts,
            "last_error": state.compensation.last_error,
            "updated_at": _stamp(state.compensation.updated_at),
        },
    }


def _state_from_dict(value: dict[str, Any]) -> SagaState:
    effects = {
        key: EffectState(**{**item, "updated_at": _parse_stamp(item["updated_at"])})
        for key, item in value.get("effects", {}).items()
    }
    compensation_value = value.get("compensation")
    compensation = (
        None
        if compensation_value is None
        else CompensationState(
            **{**compensation_value, "updated_at": _parse_stamp(compensation_value["updated_at"])}
        )
    )
    return SagaState(
        **{
            **value,
            "updated_at": _parse_stamp(value["updated_at"]),
            "effects": effects,
            "compensation": compensation,
        }
    )


def _clone_state(state: SagaState) -> SagaState:
    """Return a deep copy for one handler transition."""
    return deepcopy(state)
