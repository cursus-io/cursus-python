"""Set CURSUS_SAGA_MYSQL_DSN to run against MySQL 8+."""

import os

import pytest

from cursus.saga import (
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryOptions,
    TransactionalSagaManager,
)
from cursus.sagamysql import MySQLSagaTransaction, migrate

DSN = os.getenv("CURSUS_SAGA_MYSQL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="CURSUS_SAGA_MYSQL_DSN is not configured")


def test_mysql_transaction_persists_history_and_outbox_atomically():
    assert DSN is not None
    migrate(DSN)

    def handler(state, _event):
        state.status, state.step_id = WAITING, "reserve"
        return [Command("Reserve", "{}", effect_id="reserve:1")]

    manager = TransactionalSagaManager(
        SagaDefinition("mysql-contract", {"OrderCreated": handler}),
        MySQLSagaTransaction(DSN, "observability.saga-history.v1"),
        SagaHistoryOptions("test", "orders"),
    )
    manager.handle(
        EventEnvelope("mysql-contract-event", "OrderCreated", association_key="mysql-contract-run")
    )
