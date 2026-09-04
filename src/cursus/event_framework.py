from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Generic, Protocol, TypeVar
from uuid import uuid4

from cursus.types import AppendResult, Event, StreamData, StreamEvent


@dataclass
class EventEnvelope:
    event_id: str
    event_type: str
    schema_version: int
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    occurred_at: datetime
    payload: Any
    correlation_id: str = ""
    association_key: str = ""
    causation_id: str = ""

    @classmethod
    def create(
        cls, aggregate_type: str, aggregate_id: str, event_type: str, payload: Any
    ) -> EventEnvelope:
        if not aggregate_type or not aggregate_id or not event_type:
            raise ValueError("event envelope identity is incomplete")
        json.dumps(payload, ensure_ascii=False)
        return cls(
            event_id=str(uuid4()),
            event_type=event_type,
            schema_version=1,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            aggregate_version=0,
            occurred_at=datetime.now(timezone.utc),
            payload=payload,
        )

    def validate(self) -> None:
        if (
            not self.event_id
            or not self.event_type
            or not self.aggregate_type
            or not self.aggregate_id
        ):
            raise ValueError("event envelope identity is incomplete")
        if self.schema_version <= 0:
            raise ValueError("event schema version must be positive")
        if self.aggregate_version <= 0:
            raise ValueError("aggregate version must be positive")
        if self.payload is None:
            raise ValueError("event payload must not be empty")

    def to_json(self) -> str:
        self.validate()
        value = asdict(self)
        value["occurred_at"] = (
            self.occurred_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

    def to_event(self) -> Event:
        return Event(
            type=self.event_type,
            schema_version=self.schema_version,
            payload=self.to_json(),
        )

    @classmethod
    def from_stream_event(cls, raw: StreamEvent) -> EventEnvelope:
        try:
            value = json.loads(raw.payload)
            occurred_at = datetime.fromisoformat(value["occurred_at"].replace("Z", "+00:00"))
            value["occurred_at"] = occurred_at
            envelope = cls(**value)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"decode event envelope at offset {raw.offset}: {exc}") from exc
        if not envelope.event_type:
            envelope.event_type = raw.type
        if not envelope.schema_version:
            envelope.schema_version = raw.schema_version
        if not envelope.aggregate_version:
            envelope.aggregate_version = raw.version
        if envelope.event_type != raw.type:
            raise ValueError("event envelope type does not match stream type")
        if envelope.schema_version != raw.schema_version:
            raise ValueError("event envelope schema version does not match stream schema version")
        if envelope.aggregate_version != raw.version:
            raise ValueError("event envelope version does not match stream version")
        envelope.validate()
        return envelope


class StreamStore(Protocol):
    def read_stream(self, key: str, from_version: int = 0) -> StreamData: ...

    def append(self, key: str, expected_version: int, event: Event) -> AppendResult: ...


class Aggregate(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def type(self) -> str: ...

    @property
    def version(self) -> int: ...

    def apply(self, event: EventEnvelope) -> None: ...


A = TypeVar("A", bound=Aggregate)


class AggregateRepository(Generic[A]):
    def __init__(self, store: StreamStore, factory: Callable[[str], A]) -> None:
        if store is None or factory is None:
            raise ValueError("stream store and aggregate factory are required")
        self._store = store
        self._factory = factory

    def load(self, aggregate_id: str) -> A:
        aggregate = self._factory(aggregate_id)
        if aggregate is None:
            raise ValueError(f"aggregate factory returned None for {aggregate_id!r}")
        stream = self._store.read_stream(aggregate_id)
        if stream.snapshot is not None:
            restore = getattr(aggregate, "restore_snapshot", None)
            if restore is None:
                raise ValueError("aggregate has a snapshot but does not implement restore_snapshot")
            restore(stream.snapshot.payload, stream.snapshot.version)
        for raw in stream.events:
            event = EventEnvelope.from_stream_event(raw)
            if event.aggregate_id != aggregate_id:
                raise ValueError("event aggregate id does not match requested aggregate")
            try:
                aggregate.apply(event)
            except Exception as exc:
                raise RuntimeError(
                    f"apply {event.event_type} v{event.aggregate_version}: {exc}"
                ) from exc
        return aggregate

    def save(self, aggregate: A, events: list[EventEnvelope]) -> None:
        if aggregate is None:
            raise ValueError("aggregate is required")
        if len(events) > 1:
            raise ValueError("saving multiple events requires atomic batch append")
        expected = aggregate.version
        for event in events:
            expected += 1
            event.aggregate_type = event.aggregate_type or aggregate.type
            event.aggregate_id = event.aggregate_id or aggregate.id
            if event.aggregate_id != aggregate.id:
                raise ValueError("event aggregate id does not match aggregate")
            event.aggregate_version = expected
            result = self._store.append(aggregate.id, expected - 1, event.to_event())
            if result.version != expected:
                raise ValueError(f"append returned version {result.version}, expected {expected}")
            aggregate.apply(event)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    initial_delay_s: float = 1.0
    max_delay_s: float = 0.0
    multiplier: float = 2.0

    def should_retry(self, attempt: int) -> bool:
        return self.max_attempts > 0 and attempt < self.max_attempts

    def delay(self, attempt: int) -> float:
        attempt = max(1, attempt)
        initial = self.initial_delay_s if self.initial_delay_s > 0 else 1.0
        multiplier = self.multiplier if self.multiplier >= 1 else 2.0
        delay = initial * multiplier ** (attempt - 1)
        return min(delay, self.max_delay_s) if self.max_delay_s > 0 else delay


EventUpcaster = Callable[[EventEnvelope], EventEnvelope]


class UpcasterRegistry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[tuple[str, int], EventUpcaster] = {}

    def register(self, event_type: str, from_version: int, upcaster: EventUpcaster) -> None:
        if not event_type or from_version <= 0 or upcaster is None:
            raise ValueError("event type, source version, and upcaster are required")
        key = (event_type, from_version)
        with self._lock:
            if key in self._entries:
                raise ValueError(f"upcaster already registered for {event_type} v{from_version}")
            self._entries[key] = upcaster

    def upcast(self, event: EventEnvelope) -> EventEnvelope:
        while True:
            with self._lock:
                upcaster = self._entries.get((event.event_type, event.schema_version))
            if upcaster is None:
                return event
            previous_version = event.schema_version
            updated = upcaster(event)
            if updated.schema_version <= previous_version:
                raise ValueError(f"upcaster for {event.event_type} did not advance schema version")
            event = updated


def replay(
    store: StreamStore,
    key: str,
    from_version: int,
    handler: Callable[[EventEnvelope], None],
    registry: UpcasterRegistry | None = None,
) -> None:
    for raw in store.read_stream(key).events:
        event = EventEnvelope.from_stream_event(raw)
        if from_version > 0 and event.aggregate_version < from_version:
            continue
        if registry is not None:
            event = registry.upcast(event)
        try:
            handler(event)
        except Exception as exc:
            raise RuntimeError(
                f"replay {event.event_type} v{event.aggregate_version}: {exc}"
            ) from exc


class DeadlineManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[str, tuple[datetime, Callable[[], None]]] = {}

    def schedule(self, deadline_id: str, at: datetime, callback: Callable[[], None]) -> None:
        if not deadline_id or callback is None:
            raise ValueError("deadline id and callback are required")
        with self._lock:
            self._entries[deadline_id] = (at, callback)

    def cancel(self, deadline_id: str) -> None:
        with self._lock:
            self._entries.pop(deadline_id, None)

    def run_due(self, now: datetime) -> int:
        with self._lock:
            due = [entry for entry in self._entries.values() if entry[0] <= now]
            self._entries = {key: value for key, value in self._entries.items() if value[0] > now}
        for _, callback in due:
            callback()
        return len(due)
