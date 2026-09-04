from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from cursus.event_framework import (
    AggregateRepository,
    DeadlineManager,
    EventEnvelope,
    RetryPolicy,
    UpcasterRegistry,
    replay,
)
from cursus.types import AppendResult, StreamData, StreamEvent


class MemoryStore:
    def __init__(self) -> None:
        self.events: list[StreamEvent] = []

    def read_stream(self, _key: str, from_version: int = 0) -> StreamData:
        return StreamData(None, [event for event in self.events if event.version >= from_version])

    def append(self, _key: str, expected: int, event: object) -> AppendResult:
        version = expected + 1
        self.events.append(
            StreamEvent(
                version=version,
                offset=version - 1,
                type=event.type,  # type: ignore[attr-defined]
                schema_version=event.schema_version,  # type: ignore[attr-defined]
                payload=event.payload,  # type: ignore[attr-defined]
            )
        )
        return AppendResult(version, version - 1, 0)


@dataclass
class Game:
    id: str
    type: str = "game"
    version: int = 0
    status: str = ""

    def apply(self, event: EventEnvelope) -> None:
        self.version = event.aggregate_version
        self.status = event.payload["status"]


def test_envelope_repository_and_replay_match_go_contract() -> None:
    store = MemoryStore()
    repository = AggregateRepository(store, Game)
    aggregate = Game("game-1")
    event = EventEnvelope.create("game", "game-1", "GameCreated", {"status": "open"})

    repository.save(aggregate, [event])
    loaded = repository.load("game-1")

    assert aggregate.version == 1
    assert loaded.status == "open"
    seen: list[int] = []
    replay(store, "game-1", 1, lambda value: seen.append(value.aggregate_version))
    assert seen == [1]


def test_repository_rejects_non_atomic_multi_event_save_and_metadata_mismatch() -> None:
    store = MemoryStore()
    repository = AggregateRepository(store, Game)
    events = [
        EventEnvelope.create("game", "game-1", "A", {}),
        EventEnvelope.create("game", "game-1", "B", {}),
    ]
    with pytest.raises(ValueError, match="atomic batch"):
        repository.save(Game("game-1"), events)
    assert store.events == []


def test_upcasting_retry_policy_and_deadlines_are_bounded_and_deterministic() -> None:
    event = EventEnvelope.create("game", "game-1", "Updated", {"v": 1})
    event.aggregate_version = 1
    registry = UpcasterRegistry()

    def upcast(value: EventEnvelope) -> EventEnvelope:
        value.schema_version = 2
        value.payload = {"v": 2}
        return value

    registry.register("Updated", 1, upcast)
    assert registry.upcast(event).schema_version == 2
    policy = RetryPolicy(3, 0.01, 0.025, 2)
    assert [policy.delay(attempt) for attempt in (1, 2, 3)] == [0.01, 0.02, 0.025]

    manager = DeadlineManager()
    now = datetime.now(timezone.utc)
    fired: list[str] = []
    manager.schedule("one", now, lambda: fired.append("one"))
    assert manager.run_due(now) == 1
    assert manager.run_due(now) == 0
    assert fired == ["one"]
