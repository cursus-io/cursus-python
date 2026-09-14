"""Run with CURSUS_SAGA_POSTGRES_DSN and cursus-client[postgres] installed."""

import os

from cursus.saga import (
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryOptions,
    TransactionalSagaManager,
)
from cursus.sagapg import PostgresSagaTransaction, migrate

dsn = os.environ["CURSUS_SAGA_POSTGRES_DSN"]
history_topic = os.environ[
    "CURSUS_SAGA_HISTORY_TOPIC"
]  # service-owned, not a Cursus reserved topic
migrate(dsn)


def reserve_inventory(state, event):
    state.status, state.step_id = WAITING, "reserve-inventory"
    return [Command("ReserveInventory", event.payload, effect_id=f"reserve:{state.saga_id}")]


manager = TransactionalSagaManager(
    SagaDefinition("order-fulfillment", {"OrderCreated": reserve_inventory}),
    PostgresSagaTransaction(dsn, history_topic),
    SagaHistoryOptions(os.getenv("CURSUS_ENVIRONMENT", "development"), "orders"),
)
manager.handle(
    EventEnvelope(
        "order-42-created",
        "OrderCreated",
        association_key="order-42",
        payload='{"order_id":"order-42"}',
    )
)
