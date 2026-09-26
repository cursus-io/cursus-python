from datetime import datetime, timezone

import pytest

from cursus.broker_saga import (
    BrokerSagaRuntimeConfig,
    BrokerSagaStateRecord,
    BrokerSagaTopics,
    _id,
)
from cursus.broker_saga_types import SagaState


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
