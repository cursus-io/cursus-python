from copy import deepcopy

import pytest

from cursus.event_framework import EventEnvelope
from cursus.saga import (
    EFFECT_ENQUEUED,
    EFFECT_SUCCEEDED,
    SAGA_COMPENSATING,
    SAGA_COMPLETED,
    SAGA_WAITING,
    Command,
    SagaDefinition,
    SagaManager,
    SagaState,
)


class MemoryTransaction:
    def __init__(self, repository: "MemoryRepository") -> None:
        self.repository = repository

    def claim(self, consumer: str, event_id: str) -> bool:
        key = (consumer, event_id)
        if key in self.repository.claimed:
            return False
        self.repository.claimed.add(key)
        return True

    def load(self, saga_type: str, association: str) -> SagaState | None:
        return deepcopy(self.repository.states.get((saga_type, association)))

    def save_cas(self, state: SagaState, expected: int) -> None:
        current = self.repository.states.get((state.type, state.id))
        if (current.version if current else 0) != expected:
            raise ValueError("saga version conflict")
        if state.version != expected + 1:
            raise ValueError("invalid next saga version")
        self.repository.states[(state.type, state.id)] = deepcopy(state)

    def enqueue(self, command: Command) -> None:
        existing = next(
            (value for value in self.repository.commands if value.id == command.id), None
        )
        if existing is not None and existing != command:
            raise ValueError("outbox command identity conflict")
        if existing is None:
            self.repository.commands.append(deepcopy(command))

    def complete(self, consumer: str, event_id: str) -> None:
        self.repository.completed.append((consumer, event_id))

    def fail(self, consumer: str, event_id: str, cause: Exception) -> None:
        self.repository.failed.append((consumer, event_id, str(cause)))


class MemoryRepository:
    def __init__(self) -> None:
        self.claimed: set[tuple[str, str]] = set()
        self.states: dict[tuple[str, str], SagaState] = {}
        self.commands: list[Command] = []
        self.completed: list[tuple[str, str]] = []
        self.failed: list[tuple[str, str, str]] = []

    def transact(self, apply: object) -> None:
        snapshot = deepcopy(self.__dict__)
        try:
            apply(MemoryTransaction(self))  # type: ignore[operator]
        except Exception:
            self.__dict__.update(snapshot)
            raise


def event() -> EventEnvelope:
    value = EventEnvelope.create("game", "game-1", "GameFinished", {"winner": "p1"})
    value.event_id = "event-1"
    value.correlation_id = "saga-1"
    value.aggregate_version = 1
    return value


def test_saga_claim_state_and_outbox_are_atomic_and_idempotent() -> None:
    repository = MemoryRepository()

    def handler(state: SagaState, _event: EventEnvelope) -> list[Command]:
        state.status = SAGA_WAITING
        return [Command(type="UpdatePlayerElo")]

    manager = SagaManager(SagaDefinition("finish-game", {"GameFinished": handler}), repository)
    manager.handle(event())
    manager.handle(event())

    assert len(repository.commands) == 1
    assert repository.commands[0].id == "finish-game:saga-1:event-1:0"
    state = repository.states[("finish-game", "saga-1")]
    assert state.status == SAGA_WAITING
    assert state.effects["event-1:0"].status == EFFECT_ENQUEUED


def test_effect_command_fence_and_compensation_lifecycle() -> None:
    repository = MemoryRepository()
    manager = SagaManager(
        SagaDefinition(
            "finish-game",
            {"GameFinished": lambda state, value: [Command(effect_id="elo", type="UpdateElo")]},
        ),
        repository,
    )
    manager.handle(event())
    state = repository.states[("finish-game", "saga-1")]
    command_id = state.effects["elo"].command_id
    with pytest.raises(ValueError, match="fence"):
        manager.acknowledge_effect("saga-1", "elo", "stale")
    manager.acknowledge_effect("saga-1", "elo", command_id)
    assert repository.states[("finish-game", "saga-1")].effects["elo"].status == EFFECT_SUCCEEDED

    compensation = manager.start_compensation("saga-1", "rollback", RuntimeError("failed"))
    assert compensation.status == SAGA_COMPENSATING
    manager.complete_compensation("saga-1")
    assert repository.states[("finish-game", "saga-1")].status == SAGA_COMPLETED
