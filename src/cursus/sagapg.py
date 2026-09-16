"""PostgreSQL adapter for :mod:`cursus.saga`.

Install it with ``pip install cursus-client[postgres]``.  The core Saga API
does not import psycopg, keeping database choice in the service boundary.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from cursus.saga import (
    Command,
    CompensationState,
    EffectState,
    SagaHistoryEvent,
    SagaState,
    SagaTransactionStores,
)

DEFAULT_LEASE_SECONDS = 120
_T = TypeVar("_T")


def migration_sql() -> str:
    """Return the idempotent service-owned PostgreSQL migration."""

    return (Path(__file__).parent / "migrations" / "001_saga_history_v1.sql").read_text("utf-8")


def migrate(dsn: str) -> None:
    """Apply the Saga PostgreSQL schema using a short, independent connection."""

    with _connect(dsn) as connection:
        connection.execute(migration_sql())
        connection.commit()


def _connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised by application setup
        raise RuntimeError("PostgreSQL Saga support requires cursus-client[postgres]") from exc
    return psycopg.connect(dsn)


class PostgresSagaTransaction:
    """Serializable PostgreSQL transaction with retryable conflict handling."""

    def __init__(self, dsn: str, history_topic: str, max_attempts: int = 3) -> None:
        if not dsn:
            raise ValueError("dsn is required")
        if not history_topic:
            raise ValueError("history_topic is required and must be service configured")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._dsn = dsn
        self._history_topic = history_topic
        self._max_attempts = max_attempts

    def run(self, operation: Callable[[SagaTransactionStores], _T]) -> _T:
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            connection = _connect(self._dsn)
            try:
                connection.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                store = _PostgresStores(connection, self._history_topic)
                result = operation(SagaTransactionStores(store, store, store, store))
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
    return getattr(error, "sqlstate", "") in {"40001", "40P01"}


class _PostgresStores:
    def __init__(self, connection: Any, history_topic: str) -> None:
        self._connection = connection
        self._history_topic = history_topic

    def claim(self, consumer_name: str, event_id: str) -> bool:
        result = self._connection.execute(
            """INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,updated_at)
               VALUES (%s,%s,'CLAIMED',NOW()) ON CONFLICT DO NOTHING""",
            (consumer_name, event_id),
        )
        return result.rowcount == 1

    def complete(self, consumer_name: str, event_id: str) -> None:
        self._connection.execute(
            "UPDATE cursus_saga_inbox SET status='COMPLETED',updated_at=NOW() "
            "WHERE consumer_name=%s AND event_id=%s",
            (consumer_name, event_id),
        )

    def fail(self, consumer_name: str, event_id: str, cause: Exception) -> None:
        self._connection.execute(
            """INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at)
               VALUES (%s,%s,'FAILED',%s,NOW())
               ON CONFLICT (consumer_name,event_id) DO UPDATE
               SET status='FAILED',last_error=EXCLUDED.last_error,updated_at=EXCLUDED.updated_at""",
            (consumer_name, event_id, str(cause)),
        )

    def load_for_update(self, saga_type: str, saga_id: str) -> SagaState | None:
        # A row lock protects established runs. The advisory lock also covers
        # the first event, before a state row exists to lock.
        self._connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
            (saga_type, saga_id),
        )
        row = self._connection.execute(
            """SELECT association_key,correlation_id,run_id::text,next_sequence,status,outcome,
                      step_id,
                      data,retry_count,last_error,effects,compensation,updated_at
               FROM cursus_saga_state WHERE saga_type=%s AND saga_id=%s FOR UPDATE""",
            (saga_type, saga_id),
        ).fetchone()
        if row is None:
            return None
        effects_value = _json_value(row[10]) or {}
        effects = {
            effect_id: EffectState(
                effect_id=effect_id,
                step_id=value.get("step_id", ""),
                status=value.get("status", "PENDING"),
                command_id=value.get("command_id", ""),
                published=value.get("published", False),
                attempts=value.get("attempts", 0),
                last_error=value.get("last_error", ""),
                updated_at=_parse_time(value.get("updated_at")),
            )
            for effect_id, value in effects_value.items()
        }
        comp_value = _json_value(row[11])
        compensation = None
        if comp_value:
            compensation = CompensationState(
                step_id=comp_value["step_id"],
                status=comp_value.get("status", "COMPENSATING"),
                attempts=comp_value.get("attempts", 0),
                last_error=comp_value.get("last_error", ""),
                updated_at=_parse_time(comp_value.get("updated_at")),
            )
        return SagaState(
            saga_id=saga_id,
            saga_type=saga_type,
            association_key=row[0],
            correlation_id=row[1],
            run_id=row[2],
            next_sequence=row[3],
            status=row[4],
            outcome=row[5],
            step_id=row[6],
            data=_json_value(row[7]) or {},
            retry_count=row[8],
            last_error=row[9],
            effects=effects,
            compensation=compensation,
            updated_at=_parse_time(row[12]),
        )

    def save(self, state: SagaState) -> None:
        effects = {
            key: {
                "step_id": value.step_id,
                "status": value.status,
                "command_id": value.command_id,
                "published": value.published,
                "attempts": value.attempts,
                "last_error": value.last_error,
                "updated_at": value.updated_at.isoformat(),
            }
            for key, value in state.effects.items()
        }
        compensation = None
        if state.compensation is not None:
            compensation = {
                "step_id": state.compensation.step_id,
                "status": state.compensation.status,
                "attempts": state.compensation.attempts,
                "last_error": state.compensation.last_error,
                "updated_at": state.compensation.updated_at.isoformat(),
            }
        self._connection.execute(
            """INSERT INTO cursus_saga_state
               (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at)
               VALUES (%s,%s,%s,%s,%s::uuid,%s,%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb,%s::jsonb,%s)
               ON CONFLICT (saga_type,saga_id) DO UPDATE SET
               association_key=EXCLUDED.association_key,correlation_id=EXCLUDED.correlation_id,run_id=EXCLUDED.run_id,
               next_sequence=EXCLUDED.next_sequence,status=EXCLUDED.status,outcome=EXCLUDED.outcome,step_id=EXCLUDED.step_id,
               data=EXCLUDED.data,retry_count=EXCLUDED.retry_count,last_error=EXCLUDED.last_error,effects=EXCLUDED.effects,
               compensation=EXCLUDED.compensation,updated_at=EXCLUDED.updated_at""",
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
                json.dumps(compensation),
                state.updated_at,
            ),
        )

    def enqueue(self, saga_type: str, command: Command) -> None:
        self._connection.execute(
            """INSERT INTO cursus_saga_outbox
               (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW()) ON CONFLICT (command_id) DO NOTHING""",
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
        self._connection.execute(
            """INSERT INTO cursus_saga_history
               (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,
                run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,
                source_event_id,correlation_id,causation_id,source_topic,source_partition,source_offset,
                aggregate_type,aggregate_id,aggregate_version,payload,error)
               VALUES (%s::uuid,%s,%s,%s,%s,%s,%s::uuid,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
        self._connection.execute(
            """INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload)
               VALUES (%s::uuid,%s,%s::jsonb)""",
            (event.history_event_id, self._history_topic, event.to_json()),
        )


class HistoryPublisher:
    """Protocol-like base class for publisher transports used by the worker."""

    def publish(self, topic: str, payload: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class CursusHistoryPublisher(HistoryPublisher):
    """Publish history with a configured Cursus :class:`Producer`.

    The producer remains application-owned and must be configured for exactly
    ``topic``. A history event ID is used as the message key so a downstream
    collector can safely deduplicate at-least-once redelivery.
    """

    def __init__(self, producer: Any, topic: str) -> None:
        if not topic:
            raise ValueError("history topic is required")
        self._producer, self._topic = producer, topic

    def publish(self, topic: str, payload: str) -> None:
        if topic != self._topic:
            raise ValueError("history publisher topic does not match the configured topic")
        event_id = json.loads(payload)["history_event_id"]
        self._producer.send(payload, key=event_id)
        self._producer.flush()


class HistoryOutboxPublisher:
    """Lease/retry worker with at-least-once delivery and stable event IDs."""

    def __init__(
        self, dsn: str, publisher: HistoryPublisher, lease_seconds: int = DEFAULT_LEASE_SECONDS
    ) -> None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        self._dsn, self._publisher, self._lease_seconds = dsn, publisher, lease_seconds

    def publish_pending(self, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be positive")
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
        with _connect(self._dsn) as connection:
            connection.execute("BEGIN")
            row = connection.execute(
                """SELECT history_event_id::text,topic_name,payload::text
                   FROM cursus_saga_history_outbox
                   WHERE status='PENDING' OR (status='PUBLISHING' AND lease_expires_at < NOW())
                   ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1"""
            ).fetchone()
            if row is None:
                connection.rollback()
                return None
            connection.execute(
                """UPDATE cursus_saga_history_outbox
                   SET status='PUBLISHING',attempts=attempts+1,last_error='',
                       lease_expires_at=NOW() + (%s * INTERVAL '1 second')
                   WHERE history_event_id=%s::uuid""",
                (self._lease_seconds, row[0]),
            )
            connection.commit()
            return row[0], row[1], row[2]

    def _release(self, event_id: str, cause: Exception) -> None:
        with _connect(self._dsn) as connection:
            connection.execute(
                """UPDATE cursus_saga_history_outbox
                   SET status='PENDING',last_error=%s,lease_expires_at=NULL
                   WHERE history_event_id=%s::uuid""",
                (str(cause), event_id),
            )
            connection.commit()

    def _mark_published(self, event_id: str) -> None:
        with _connect(self._dsn) as connection:
            connection.execute(
                """UPDATE cursus_saga_history_outbox
                   SET status='PUBLISHED',published_at=NOW(),last_error='',lease_expires_at=NULL
                   WHERE history_event_id=%s::uuid""",
                (event_id,),
            )
            connection.commit()


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _object_json(value: str) -> str:
    if not value:
        return "{}"
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("command payload must be a JSON object")
    return value
