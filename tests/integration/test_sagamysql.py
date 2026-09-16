"""Set CURSUS_SAGA_MYSQL_DSN to run against MySQL 8+."""

import json
import os
from uuid import uuid4

import pytest

from cursus.saga import (
    WAITING,
    Command,
    EventEnvelope,
    SagaDefinition,
    SagaHistoryOptions,
    TransactionalSagaManager,
)
from cursus.sagamysql import HistoryOutboxPublisher, MySQLSagaTransaction, _connect, migrate

DSN = os.getenv("CURSUS_SAGA_MYSQL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="CURSUS_SAGA_MYSQL_DSN is not configured")


class RecordingPublisher:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[tuple[str, str]] = []

    def publish(self, topic: str, payload: str) -> None:
        self.calls.append((topic, payload))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("broker unavailable")


def _row(sql: str, params: tuple[object, ...]) -> tuple[object, ...]:
    assert DSN is not None
    connection = _connect(DSN)
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        connection.commit()
        return row
    finally:
        connection.close()


def _execute(sql: str, params: tuple[object, ...]) -> None:
    assert DSN is not None
    connection = _connect(DSN)
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()
    finally:
        connection.close()


def test_mysql_transaction_persists_all_stores_and_retries_history_outbox():
    assert DSN is not None
    migrate(DSN)

    def handler(state, _event):
        state.status, state.step_id = WAITING, "reserve"
        return [Command("Reserve", "{}", effect_id="reserve:1")]

    saga_id = f"mysql-contract-{uuid4()}"
    manager = TransactionalSagaManager(
        SagaDefinition("mysql-contract", {"OrderCreated": handler}),
        MySQLSagaTransaction(DSN, "observability.saga-history.v1"),
        SagaHistoryOptions("test", "orders"),
    )
    manager.handle(
        EventEnvelope(f"mysql-contract-event-{saga_id}", "OrderCreated", association_key=saga_id)
    )
    manager.handle(
        EventEnvelope(f"mysql-contract-event-{saga_id}", "OrderCreated", association_key=saga_id)
    )

    counts = _row(
        """SELECT
             (SELECT count(*) FROM cursus_saga_state WHERE saga_id=%s),
             (SELECT count(*) FROM cursus_saga_inbox WHERE event_id=%s),
             (SELECT count(*) FROM cursus_saga_outbox WHERE saga_id=%s),
             (SELECT count(*) FROM cursus_saga_history WHERE saga_id=%s),
             (SELECT count(*) FROM cursus_saga_history_outbox h
                JOIN cursus_saga_history e USING(history_event_id) WHERE e.saga_id=%s)""",
        (saga_id, f"mysql-contract-event-{saga_id}", saga_id, saga_id, saga_id),
    )
    assert counts == (1, 1, 1, 5, 5)
    (run_id,) = _row(
        "SELECT run_id FROM cursus_saga_history WHERE saga_id=%s ORDER BY sequence LIMIT 1",
        (saga_id,),
    )
    with pytest.raises(Exception):
        _execute(
            """INSERT INTO cursus_saga_history
               (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,
                run_id,sequence,event_type,occurred_at,recorded_at)
               VALUES (UUID(),1,'test','orders','mysql-contract',%s,%s,
                       1,'run.started',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))""",
            (saga_id, run_id),
        )

    publisher = RecordingPublisher(failures=1)
    worker = HistoryOutboxPublisher(DSN, publisher)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        worker.publish_pending(limit=1)
    first_id = json.loads(publisher.calls[0][1])["history_event_id"]
    _execute(
        """UPDATE cursus_saga_history_outbox SET status='PUBLISHED'
           WHERE history_event_id IN (
             SELECT history_event_id FROM cursus_saga_history WHERE saga_id=%s
           ) AND history_event_id <> %s""",
        (saga_id, first_id),
    )
    assert worker.publish_pending(limit=1) == 1
    assert [json.loads(call[1])["history_event_id"] for call in publisher.calls] == [
        first_id,
        first_id,
    ]
    status, attempts = _row(
        "SELECT status,attempts FROM cursus_saga_history_outbox WHERE history_event_id=%s",
        (first_id,),
    )
    assert (status, attempts) == ("PUBLISHED", 2)
