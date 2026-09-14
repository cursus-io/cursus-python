import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

from cursus.saga import (
    COMMAND_ENQUEUED,
    COMMAND_SUCCEEDED,
    COMPENSATED,
    RUN_COMPENSATED,
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryEvent,
    SagaHistoryOptions,
    SagaTransactionStores,
    TransactionalSagaManager,
)


class MemoryStores:
    def __init__(self):
        self.claimed = set()
        self.failed = set()
        self.states = {}
        self.commands = []
        self.history = []
        self.fail_enqueue = False

    def claim(self, consumer, event_id):
        key = (consumer, event_id)
        if key in self.claimed:
            return False
        self.claimed.add(key)
        return True

    def complete(self, consumer, event_id):
        return None

    def fail(self, consumer, event_id, cause):
        self.failed.add((consumer, event_id))

    def load_for_update(self, saga_type, saga_id):
        return self.states.get((saga_type, saga_id))

    def save(self, state):
        self.states[(state.saga_type, state.saga_id)] = state

    def enqueue(self, saga_type, command):
        if self.fail_enqueue:
            raise RuntimeError("outbox unavailable")
        self.commands.append(command)

    def append(self, event):
        self.history.append(event)


class MemoryTransaction:
    def __init__(self):
        self.stores = MemoryStores()

    def run(self, operation):
        before = copy.deepcopy(self.stores)
        try:
            return operation(
                SagaTransactionStores(self.stores, self.stores, self.stores, self.stores)
            )
        except Exception:
            self.stores = before
            raise


def manager(transaction, handler=None):
    def default_handler(state, _event):
        state.status, state.step_id = WAITING, "reserve"
        return [Command("Reserve", '{"order":"42"}', effect_id="reserve:1")]

    return TransactionalSagaManager(
        SagaDefinition("orders", {"OrderCreated": handler or default_handler}),
        transaction,
        SagaHistoryOptions("test", "orders"),
    )


def event(event_id="event-1"):
    return EventEnvelope(
        event_id, "OrderCreated", association_key="order-42", correlation_id="order-42"
    )


def test_go_fixture_round_trips_with_identical_json_meaning():
    fixture = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "saga-history-v1.json").read_text()
    )
    parsed_time = datetime.fromisoformat(fixture["occurred_at"].replace("Z", "+00:00"))
    actual = SagaHistoryEvent(
        environment_id=fixture["environment_id"],
        service_name=fixture["service_name"],
        saga_type=fixture["saga_type"],
        saga_id=fixture["saga_id"],
        run_id=fixture["run_id"],
        sequence=int(fixture["sequence"]),
        event_type=fixture["event_type"],
        occurred_at=parsed_time,
        recorded_at=parsed_time,
        history_event_id=fixture["history_event_id"],
        step_id=fixture["step_id"],
        attempt=fixture["attempt"],
        command_id=fixture["command_id"],
        effect_id=fixture["effect_id"],
        source_event_id=fixture["source_event_id"],
        correlation_id=fixture["correlation_id"],
        source_topic=fixture["source_topic"],
        source_partition=fixture["source_partition"],
        source_offset=int(fixture["source_offset"]),
        aggregate_type=fixture["aggregate_type"],
        aggregate_id=fixture["aggregate_id"],
        aggregate_version=int(fixture["aggregate_version"]),
        payload=fixture["payload"],
    )
    assert actual.to_dict() == fixture


def test_success_duplicate_and_monotonic_history_sequence():
    transaction = MemoryTransaction()
    saga = manager(transaction)
    saga.handle(event())
    saga.handle(event())
    assert [entry.sequence for entry in transaction.stores.history] == [1, 2, 3, 4, 5]
    assert [entry.event_type for entry in transaction.stores.history][2] == COMMAND_ENQUEUED
    assert len(transaction.stores.commands) == 1


def test_effect_result_is_the_only_business_success():
    transaction = MemoryTransaction()
    saga = manager(transaction)
    saga.handle(event())
    saga.record_command_published("order-42", "reserve:1")
    assert (
        transaction.stores.states[("orders", "order-42")].effects["reserve:1"].status == "PENDING"
    )
    saga.record_effect_result("order-42", "reserve:1", True)
    assert transaction.stores.history[-1].event_type == COMMAND_SUCCEEDED


def test_rollback_removes_success_history_then_records_failure_in_new_transaction():
    transaction = MemoryTransaction()
    transaction.stores.fail_enqueue = True
    saga = manager(transaction)
    with pytest.raises(RuntimeError, match="outbox unavailable"):
        saga.handle(event())
    assert transaction.stores.history == []
    assert ("orders", "event-1") in transaction.stores.failed


def test_compensation_records_terminal_outcome():
    transaction = MemoryTransaction()
    saga = manager(transaction)
    saga.handle(event())
    saga.start_compensation("order-42", "release")
    saga.complete_compensation("order-42")
    state = transaction.stores.states[("orders", "order-42")]
    assert state.outcome == COMPENSATED
    assert transaction.stores.history[-1].event_type == RUN_COMPENSATED
