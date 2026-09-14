"""Set CURSUS_SAGA_POSTGRES_DSN to run against a real PostgreSQL database."""

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
from cursus.sagapg import PostgresSagaTransaction, migrate

DSN = os.getenv("CURSUS_SAGA_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="CURSUS_SAGA_POSTGRES_DSN is not configured")


def test_postgres_transaction_persists_history_and_outbox_atomically():
    assert DSN is not None
    migrate(DSN)

    def handler(state, _event):
        state.status, state.step_id = WAITING, "reserve"
        return [Command("Reserve", "{}", effect_id="reserve:1")]

    manager = TransactionalSagaManager(
        SagaDefinition("postgres-contract", {"OrderCreated": handler}),
        PostgresSagaTransaction(DSN, "observability.saga-history.v1"),
        SagaHistoryOptions("test", "orders"),
    )
    manager.handle(
        EventEnvelope("pg-contract-event", "OrderCreated", association_key="pg-contract-run")
    )
