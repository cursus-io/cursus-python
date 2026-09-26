"""Types shared by the DB-free broker-native Saga runtime."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
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
