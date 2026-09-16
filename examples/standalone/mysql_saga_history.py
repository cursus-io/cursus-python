"""Run with CURSUS_SAGA_MYSQL_DSN and cursus-client[mysql] installed."""

import os

from cursus.saga import (
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryOptions,
    TransactionalSagaManager,
)
from cursus.sagamysql import MySQLSagaTransaction, migrate

dsn = os.environ["CURSUS_SAGA_MYSQL_DSN"]
history_topic = os.environ["CURSUS_SAGA_HISTORY_TOPIC"]
migrate(dsn)


def reserve_inventory(state, event):
    state.status, state.step_id = WAITING, "reserve-inventory"
    return [Command("ReserveInventory", event.payload, effect_id=f"reserve:{state.saga_id}")]


manager = TransactionalSagaManager(
    SagaDefinition("order-fulfillment", {"OrderCreated": reserve_inventory}),
    MySQLSagaTransaction(dsn, history_topic),
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
