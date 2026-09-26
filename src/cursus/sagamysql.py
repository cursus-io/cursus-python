"""MySQL 8+ adapter for the database-neutral Cursus Saga API.

Install with ``cursus-client[mysql]``. MySQL 8.0+ is required for JSON,
CHECK constraints, and ``FOR UPDATE SKIP LOCKED`` outbox leasing.
"""

# ruff: noqa: E501  # SQL statements retain their database column order.

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar, cast
from urllib.parse import parse_qs, unquote, urlparse

from cursus.saga import (
    Command,
    CompensationState,
    EffectState,
    SagaHistoryEvent,
    SagaState,
    SagaTransactionStores,
)
from cursus.sagapg import DEFAULT_LEASE_SECONDS, HistoryPublisher

_T = TypeVar("_T")


def migration_sql() -> str:
    return (Path(__file__).parent / "migrations" / "001_saga_history_v1.mysql.sql").read_text(
        "utf-8"
    )


def _connect(dsn: str) -> Any:
    try:
        import mysql.connector
    except ImportError as exc:  # pragma: no cover - application setup
        raise RuntimeError("MySQL Saga support requires cursus-client[mysql]") from exc
    parsed = urlparse(dsn)
    if parsed.scheme not in {"mysql", "mysql+tcp"} or not parsed.hostname or not parsed.path:
        raise ValueError("dsn must be mysql://user:password@host:3306/database")
    query = parse_qs(parsed.query)
    return mysql.connector.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=False,
        connection_timeout=int(query.get("connect_timeout", ["10"])[0]),
    )


def migrate(dsn: str) -> None:
    connection = _connect(dsn)
    try:
        cursor = connection.cursor()
        for statement in migration_sql().split(";"):
            if statement.strip():
                cursor.execute(statement)
        connection.commit()
    finally:
        connection.close()


class MySQLSagaTransaction:
    """Serializable MySQL transaction with deadlock/timeout retry."""

    def __init__(self, dsn: str, history_topic: str, max_attempts: int = 3) -> None:
        if not dsn or not history_topic:
            raise ValueError("dsn and service-configured history_topic are required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._dsn, self._history_topic, self._max_attempts = dsn, history_topic, max_attempts

    def run(self, operation: Callable[[SagaTransactionStores], _T]) -> _T:
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            connection = _connect(self._dsn)
            try:
                cursor = connection.cursor()
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute("START TRANSACTION")
                result = operation(
                    SagaTransactionStores(*([_MySQLStores(connection, self._history_topic)] * 4))
                )
                connection.commit()
                return result
            except Exception as exc:
                connection.rollback()
                if attempt + 1 >= self._max_attempts or not _is_retryable(exc):
                    raise
                last_error = exc
            finally:
                connection.close()
        assert last_error is not None
        raise last_error


def _is_retryable(error: Exception) -> bool:
    return (
        getattr(error, "errno", None) in {1205, 1213} or getattr(error, "sqlstate", "") == "40001"
    )


class _MySQLStores:
    def __init__(self, connection: Any, history_topic: str) -> None:
        self._connection, self._history_topic = connection, history_topic

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        cursor = self._connection.cursor()
        cursor.execute(sql, params)
        return cursor

    def claim(self, consumer_name: str, event_id: str) -> bool:
        result = self._execute(
            "INSERT IGNORE INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) "
            "VALUES (%s,%s,'CLAIMED','',UTC_TIMESTAMP(6))",
            (consumer_name, event_id),
        )
        return cast(int, result.rowcount) == 1

    def complete(self, consumer_name: str, event_id: str) -> None:
        self._execute(
            "UPDATE cursus_saga_inbox SET status='COMPLETED',updated_at=UTC_TIMESTAMP(6) "
            "WHERE consumer_name=%s AND event_id=%s",
            (consumer_name, event_id),
        )

    def fail(self, consumer_name: str, event_id: str, cause: Exception) -> None:
        self._execute(
            """INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at)
               VALUES (%s,%s,'FAILED',%s,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE
               status='FAILED',last_error=VALUES(last_error),updated_at=VALUES(updated_at)""",
            (consumer_name, event_id, str(cause)),
        )

    def load_for_update(self, saga_type: str, saga_id: str) -> SagaState | None:
        # MySQL named locks serialize first creation, when no state row exists.
        lock = self._execute(
            "SELECT GET_LOCK(SHA2(CONCAT(%s,':',%s),256), 10)",
            (saga_type, saga_id),
        ).fetchone()
        if lock is None or lock[0] != 1:
            raise TimeoutError("could not acquire Saga creation lock")
        row = self._execute(
            """SELECT association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,
                      retry_count,last_error,effects,compensation,updated_at FROM cursus_saga_state
               WHERE saga_type=%s AND saga_id=%s FOR UPDATE""",
            (saga_type, saga_id),
        ).fetchone()
        if row is None:
            return None
        effects = {
            key: EffectState(
                key,
                value.get("step_id", ""),
                value.get("status", "PENDING"),
                value.get("command_id", ""),
                value.get("published", False),
                value.get("attempts", 0),
                value.get("last_error", ""),
                _parse_time(value.get("updated_at")),
            )
            for key, value in (_json(row[10]) or {}).items()
        }
        comp = _json(row[11])
        compensation = (
            None
            if not comp
            else CompensationState(
                comp["step_id"],
                comp.get("status", "COMPENSATING"),
                comp.get("attempts", 0),
                comp.get("last_error", ""),
                _parse_time(comp.get("updated_at")),
            )
        )
        return SagaState(
            saga_id,
            saga_type,
            row[0],
            row[1],
            row[4],
            row[6],
            _json(row[7]) or {},
            row[8],
            row[9],
            row[2],
            row[3],
            row[5],
            _parse_time(row[12]),
            effects,
            compensation,
        )

    def save(self, state: SagaState) -> None:
        effects = {
            key: {
                "step_id": effect.step_id,
                "status": effect.status,
                "command_id": effect.command_id,
                "published": effect.published,
                "attempts": effect.attempts,
                "last_error": effect.last_error,
                "updated_at": effect.updated_at.isoformat(),
            }
            for key, effect in state.effects.items()
        }
        comp = (
            None
            if state.compensation is None
            else {
                "step_id": state.compensation.step_id,
                "status": state.compensation.status,
                "attempts": state.compensation.attempts,
                "last_error": state.compensation.last_error,
                "updated_at": state.compensation.updated_at.isoformat(),
            }
        )
        self._execute(
            """INSERT INTO cursus_saga_state
               (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
               association_key=VALUES(association_key),correlation_id=VALUES(correlation_id),run_id=VALUES(run_id),
               next_sequence=VALUES(next_sequence),status=VALUES(status),outcome=VALUES(outcome),step_id=VALUES(step_id),
               data=VALUES(data),retry_count=VALUES(retry_count),last_error=VALUES(last_error),effects=VALUES(effects),
               compensation=VALUES(compensation),updated_at=VALUES(updated_at)""",
            (
                state.saga_type,
                state.saga_id,
                state.association_key,
                state.correlation_id,
                state.run_id,
                state.next_sequence,
                state.status,
                state.outcome,
                state.step_id,
                json.dumps(state.data),
                state.retry_count,
                state.last_error,
                json.dumps(effects),
                json.dumps(comp),
                state.updated_at,
            ),
        )

    def enqueue(self, saga_type: str, command: Command) -> None:
        self._execute(
            """INSERT IGNORE INTO cursus_saga_outbox
               (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(6))""",
            (
                command.command_id,
                saga_type,
                command.saga_id,
                command.effect_id,
                command.type,
                command.correlation_id,
                command.causation_id,
                _object_json(command.payload),
            ),
        )

    def append(self, event: SagaHistoryEvent) -> None:
        self._execute(
            """INSERT INTO cursus_saga_history
               (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,source_event_id,correlation_id,causation_id,source_topic,source_partition,source_offset,aggregate_type,aggregate_id,aggregate_version,payload,error)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                event.history_event_id,
                event.history_schema_version,
                event.environment_id,
                event.service_name,
                event.saga_type,
                event.saga_id,
                event.run_id,
                event.sequence,
                event.event_type,
                event.occurred_at,
                event.recorded_at,
                event.step_id,
                event.attempt,
                event.command_id,
                event.effect_id,
                event.source_event_id,
                event.correlation_id,
                event.causation_id,
                event.source_topic,
                event.source_partition,
                event.source_offset,
                event.aggregate_type,
                event.aggregate_id,
                event.aggregate_version,
                event.payload,
                event.error,
            ),
        )
        self._execute(
            "INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload,status,attempts,last_error,created_at) "
            "VALUES (%s,%s,%s,'PENDING',0,'',UTC_TIMESTAMP(6))",
            (event.history_event_id, self._history_topic, event.to_json()),
        )


class HistoryOutboxPublisher:
    def __init__(
        self, dsn: str, publisher: HistoryPublisher, lease_seconds: int = DEFAULT_LEASE_SECONDS
    ) -> None:
        self._dsn, self._publisher, self._lease_seconds = dsn, publisher, lease_seconds

    def publish_pending(self, limit: int = 100) -> int:
        published = 0
        while published < limit:
            entry = self._claim()
            if entry is None:
                return published
            event_id, topic, payload = entry
            try:
                self._publisher.publish(topic, payload)
            except Exception as exc:
                self._release(event_id, exc)
                raise
            self._mark_published(event_id)
            published += 1
        return published

    def _claim(self) -> tuple[str, str, str] | None:
        connection = _connect(self._dsn)
        try:
            cursor = connection.cursor()
            cursor.execute("START TRANSACTION")
            cursor.execute("""SELECT history_event_id,topic_name,CAST(payload AS CHAR) FROM cursus_saga_history_outbox
                WHERE status='PENDING' OR (status='PUBLISHING' AND lease_expires_at < UTC_TIMESTAMP(6))
                ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1""")
            row = cursor.fetchone()
            if row is None:
                connection.rollback()
                return None
            cursor.execute(
                """UPDATE cursus_saga_history_outbox SET status='PUBLISHING',attempts=attempts+1,last_error='',
                lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND) WHERE history_event_id=%s""",
                (self._lease_seconds, row[0]),
            )
            connection.commit()
            return row[0], row[1], row[2]
        finally:
            connection.close()

    def _release(self, event_id: str, cause: Exception) -> None:
        connection = _connect(self._dsn)
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE cursus_saga_history_outbox SET status='PENDING',last_error=%s,lease_expires_at=NULL WHERE history_event_id=%s",
                (str(cause), event_id),
            )
            connection.commit()
        finally:
            connection.close()

    def _mark_published(self, event_id: str) -> None:
        connection = _connect(self._dsn)
        try:
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE cursus_saga_history_outbox SET status='PUBLISHED',published_at=UTC_TIMESTAMP(6),last_error='',lease_expires_at=NULL WHERE history_event_id=%s",
                (event_id,),
            )
            connection.commit()
        finally:
            connection.close()


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    if value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _object_json(value: str) -> str:
    parsed = json.loads(value or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("command payload must be a JSON object")
    return value or "{}"
