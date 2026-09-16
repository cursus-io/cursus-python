"""Run with CURSUS_SAGA_POSTGRES_DSN and CURSUS_SAGA_BROKER_ADDR."""

import json
import os
import threading
from uuid import uuid4

import pytest
from conftest import provision_topic

from cursus import Acks, Consumer, ConsumerConfig, ConsumerMode, Producer, ProducerConfig
from cursus.saga import (
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryOptions,
    TransactionalSagaManager,
)
from cursus.sagapg import (
    CursusHistoryPublisher,
    HistoryOutboxPublisher,
    PostgresSagaTransaction,
    migrate,
)

DSN = os.getenv("CURSUS_SAGA_POSTGRES_DSN")
BROKER = os.getenv("CURSUS_SAGA_BROKER_ADDR")
pytestmark = pytest.mark.skipif(
    not DSN or not BROKER,
    reason="CURSUS_SAGA_POSTGRES_DSN and CURSUS_SAGA_BROKER_ADDR are required",
)


def test_history_outbox_publishes_immutable_events_to_cursus_topic():
    assert DSN is not None and BROKER is not None
    migrate(DSN)
    topic = f"observability-saga-history-{uuid4().hex[:10]}"
    provision_topic(BROKER, topic, partitions=1)
    received: list[dict[str, object]] = []
    complete = threading.Event()
    consumer = Consumer(
        ConsumerConfig(
            brokers=[BROKER],
            topic=topic,
            group_id=f"saga-history-{uuid4().hex[:8]}",
            mode=ConsumerMode.POLLING,
            immediate_commit=True,
        )
    )

    def consume(message) -> None:
        received.append(json.loads(message.payload))
        if len(received) == 5:
            complete.set()

    thread = threading.Thread(target=consumer.start, args=(consume,), daemon=True)
    thread.start()
    try:

        def handler(state, _event):
            state.status, state.step_id = WAITING, "reserve"
            return [Command("Reserve", "{}", effect_id="reserve:1")]

        saga_id = f"broker-e2e-{uuid4()}"
        manager = TransactionalSagaManager(
            SagaDefinition("broker-e2e", {"OrderCreated": handler}),
            PostgresSagaTransaction(DSN, topic),
            SagaHistoryOptions("test", "orders"),
        )
        manager.handle(EventEnvelope(f"event-{saga_id}", "OrderCreated", association_key=saga_id))
        with Producer(
            ProducerConfig(
                brokers=[BROKER],
                topic=topic,
                partitions=1,
                acks=Acks.ALL,
                batch_size=1,
                linger_ms=0,
            )
        ) as producer:
            published = HistoryOutboxPublisher(DSN, CursusHistoryPublisher(producer, topic))
            assert published.publish_pending(limit=10) == 5
        assert complete.wait(timeout=15), f"timed out waiting for history: {received}"
    finally:
        consumer.close()
        thread.join(timeout=5)

    assert {event["event_type"] for event in received} == {
        "run.started",
        "step.started",
        "command.enqueued",
        "step.completed",
        "run.waiting",
    }
    assert {event["history_schema_version"] for event in received} == {1}
    assert len({event["history_event_id"] for event in received}) == 5
    assert {event["saga_id"] for event in received} == {saga_id}
