from datetime import datetime, timezone
from uuid import uuid4

from conftest import provision_topic

from cursus import (
    BrokerSagaHistoryDraft,
    BrokerSagaInput,
    BrokerSagaRuntime,
    BrokerSagaRuntimeConfig,
    BrokerSagaTopics,
    Command,
    Consumer,
    ConsumerConfig,
    ConsumerMode,
    EventEnvelope,
    EventStore,
    IsolationLevel,
    TransactionalProducer,
)
from cursus.broker_saga_types import STEP_COMPLETED, WAITING


def test_broker_saga_runtime_commits_state_history_command_and_offset(broker_addr):
    suffix = uuid4().hex[:10]
    topics = BrokerSagaTopics(
        inbox=f"py-saga-inbox-{suffix}",
        state=f"py-saga-state-{suffix}",
        commands=f"py-saga-commands-{suffix}",
        history=f"py-saga-history-{suffix}",
    )
    provision_topic(broker_addr, topics.inbox)
    provision_topic(broker_addr, topics.state, event_sourcing=True)
    provision_topic(broker_addr, topics.commands)
    provision_topic(broker_addr, topics.history)

    # Materialize a real source record, then obtain the membership metadata
    # that the runtime must atomically acknowledge through SEND_OFFSETS_TO_TXN.
    source = TransactionalProducer(f"py-saga-source-{suffix}", [broker_addr])
    with source:
        source.publish(topics.inbox, "source", partition=0)

    consumer = Consumer(
        ConsumerConfig(
            brokers=[broker_addr],
            topic=topics.inbox,
            group_id=f"py-saga-group-{suffix}",
            mode=ConsumerMode.POLLING,
            isolation_level=IsolationLevel.READ_COMMITTED,
            enable_auto_commit=False,
        )
    )
    try:
        next(iter(consumer))
        metadata = consumer.transactional_offset_metadata()

        store = EventStore(broker_addr, topics.state, f"py-saga-state-producer-{suffix}")
        runtime = BrokerSagaRuntime(
            BrokerSagaRuntimeConfig("orders", "integration", "python-sdk", topics),
            store,
            lambda transaction_id: TransactionalProducer(transaction_id, [broker_addr]),
        )
        run_id = "23cce7bf-8ba9-4d98-b5c8-4e132ff787c5"
        event = EventEnvelope(
            event_id=f"source-{suffix}",
            event_type="OrderCreated",
            schema_version=1,
            aggregate_type="order",
            aggregate_id="order-42",
            aggregate_version=1,
            occurred_at=datetime.now(timezone.utc),
            payload={"order_id": "order-42"},
            correlation_id="order-42",
        )
        runtime.handle(
            BrokerSagaInput(
                saga_id="order-42",
                run_id=run_id,
                event=event,
                topic=metadata.topic,
                partition=0,
                offset=0,
                group=metadata.group,
                member=metadata.member,
                generation=metadata.generation,
            ),
            lambda state, _: (
                _waiting_command(state),
                [
                    BrokerSagaHistoryDraft(STEP_COMPLETED, "reserve"),
                ],
            ),
        )

        stream = store.read_stream(runtime.stream_key("order-42", run_id))
        assert len(stream.events) == 1
        assert '"processed_event_ids":["source-' in stream.events[0].payload
        assert '"next_sequence":3' in stream.events[0].payload

        # The same input is an acknowledgement-only transaction: it cannot
        # append another state record or duplicate history/command output.
        runtime.handle(
            BrokerSagaInput(
                "order-42",
                run_id,
                event,
                metadata.topic,
                0,
                0,
                metadata.group,
                metadata.member,
                metadata.generation,
            ),
            lambda *_: (_ for _ in ()).throw(AssertionError("duplicate invoked handler")),
        )
        assert len(store.read_stream(runtime.stream_key("order-42", run_id)).events) == 1
    finally:
        consumer.close()


def _waiting_command(state):
    state.status = WAITING
    return [Command(type="ReserveInventory", payload='{"order_id":"order-42"}')]
