import json
from datetime import datetime, timezone

import pytest

from cursus.broker_saga import (
    BrokerSagaInput,
    BrokerSagaRuntime,
    BrokerSagaRuntimeConfig,
    BrokerSagaStateRecord,
    BrokerSagaTopics,
    _id,
)
from cursus.broker_saga_types import EventEnvelope, SagaState
from cursus.types import StreamData, StreamEvent


def test_broker_saga_state_record_round_trips_without_database() -> None:
    state = SagaState(
        saga_id="order-42",
        saga_type="orders",
        association_key="order-42",
        run_id="de94b8eb-50c4-4a35-b324-59b9318af658",
        updated_at=datetime(2026, 9, 26, tzinfo=timezone.utc),
    )
    record = BrokerSagaStateRecord("orders", "order-42", state.run_id, state, ["event-1"])

    restored = BrokerSagaStateRecord.from_json(record.to_json())

    assert restored.saga_type == "orders"
    assert restored.state.run_id == state.run_id
    assert restored.processed_event_ids == ["event-1"]


def test_broker_saga_identity_is_deterministic_across_redelivery() -> None:
    run_id = "de94b8eb-50c4-4a35-b324-59b9318af658"
    assert _id("history", "orders", "order-42", run_id, "1") == _id(
        "history", "orders", "order-42", run_id, "1"
    )


def test_broker_saga_topics_reject_reserved_name() -> None:
    with pytest.raises(ValueError, match="public"):
        BrokerSagaTopics(inbox="__internal")

    assert (
        BrokerSagaRuntimeConfig("orders", "test", "orders").topics.history
        == "cursus.saga-history.v1"
    )


class _StateStore:
    def __init__(self, record: BrokerSagaStateRecord) -> None:
        self._record = record

    def read_stream(self, _: str) -> StreamData:
        return StreamData(
            snapshot=None,
            events=[
                StreamEvent(
                    version=1,
                    offset=0,
                    payload=self._record.to_json(),
                    type="saga.state.transitioned",
                    schema_version=1,
                )
            ],
        )


class _Producer:
    def __init__(self, transaction_id: str) -> None:
        self.transaction_id = transaction_id
        self.appended_payload = ""

    def __enter__(self) -> "_Producer":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def append_stream(
        self, topic: str, key: str, expected_version: int, payload: str, **kwargs: object
    ) -> None:
        self.appended_payload = payload

    def publish(self, *args: object, **kwargs: object) -> None:
        return None

    def send_offsets_to_transaction(self, *args: object, **kwargs: object) -> None:
        return None


def _input(*, group: str = "orders-workers") -> BrokerSagaInput:
    return BrokerSagaInput(
        saga_id="order-42",
        run_id="de94b8eb-50c4-4a35-b324-59b9318af658",
        event=EventEnvelope("event-1", "order.created"),
        topic="orders",
        partition=0,
        offset=7,
        group=group,
        member="member-1",
        generation=1,
    )


def test_broker_saga_transaction_identity_scopes_the_consumer_and_run() -> None:
    config = BrokerSagaRuntimeConfig("orders", "test", "checkout")
    runtime = BrokerSagaRuntime(
        config,
        _StateStore(BrokerSagaStateRecord("", "", "", SagaState("", "", ""))),
        _Producer,
    )

    first = runtime._transaction_id(_input(), "apply")
    assert first != runtime._transaction_id(_input(group="billing-workers"), "apply")
    assert first != runtime._transaction_id(
        BrokerSagaInput(
            saga_id="order-43",
            run_id="other-run",
            event=EventEnvelope("event-1", "order.created"),
            topic="orders",
            partition=0,
            offset=7,
            group="orders-workers",
            member="member-1",
            generation=1,
        ),
        "apply",
    )


def test_broker_saga_failure_discards_handler_state_mutations() -> None:
    state = SagaState(
        saga_id="order-42",
        saga_type="orders",
        association_key="order-42",
        run_id="de94b8eb-50c4-4a35-b324-59b9318af658",
        status="WAITING",
        data={"stable": {"value": 1}},
    )
    record = BrokerSagaStateRecord("orders", state.saga_id, state.run_id, state)
    producers: list[_Producer] = []

    def factory(transaction_id: str) -> _Producer:
        producer = _Producer(transaction_id)
        producers.append(producer)
        return producer

    runtime = BrokerSagaRuntime(
        BrokerSagaRuntimeConfig("orders", "test", "checkout"), _StateStore(record), factory
    )

    def failing_handler(
        handler_state: SagaState, _: EventEnvelope
    ) -> tuple[list[object], list[object]]:
        handler_state.status = "MUTATED"
        handler_state.data["stable"]["value"] = 2
        raise RuntimeError("handler failed")

    with pytest.raises(RuntimeError, match="handler failed"):
        runtime.handle(_input(), failing_handler)

    failure = json.loads(producers[0].appended_payload)["state"]
    assert failure["status"] == "WAITING"
    assert failure["data"] == {"stable": {"value": 1}}
    assert failure["retry_count"] == 1
    assert failure["last_error"] == "handler failed"
