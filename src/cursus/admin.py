from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

from cursus.broker_client import BrokerCommandClient
from cursus.protocol.decoder import require_ok


class TopicCleanupPolicy(str, Enum):
    DELETE = "delete"
    COMPACT = "compact"


@dataclass(frozen=True)
class AdminConfig:
    brokers: list[str] = field(default_factory=lambda: ["localhost:9000"])
    max_retries: int = 3
    retry_backoff_ms: int = 100
    request_timeout_ms: int = 5000
    compression_type: str = "none"
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    principal: str | None = None
    auth_token: str | None = None


@dataclass(frozen=True)
class TopicDefinitionPatch:
    partitions: int | None = None
    replication_factor: int | None = None
    idempotent: bool | None = None
    event_sourcing: bool | None = None
    cleanup_policy: TopicCleanupPolicy | None = None
    retention_hours: int | None = None
    retention_bytes: int | None = None
    partitioner: str | None = None
    auth_policy: str | None = None
    read_acl: list[str] | None = None
    write_acl: list[str] | None = None


@dataclass(frozen=True)
class TopicDefinition:
    topic: str
    revision: int
    lifecycle_epoch: int
    partitions: int
    replication_factor: int
    idempotent: bool
    event_sourcing: bool
    cleanup_policy: TopicCleanupPolicy
    retention_hours: int
    retention_bytes: int
    partitioner: str
    auth_policy: str
    read_acl: list[str] = field(default_factory=list)
    write_acl: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DeleteTopicOptions:
    if_exists: bool = False


@dataclass(frozen=True)
class DeleteTopicResult:
    topic: str
    deleted: bool
    cleanup_pending: bool = False


@dataclass(frozen=True)
class TruncateTopicOptions:
    expected_revision: int


@dataclass(frozen=True)
class TruncateTopicResult:
    topic: str
    truncated: bool
    revision: int
    lifecycle_epoch: int
    leo: int
    hwm: int
    cleanup_pending: bool = False


_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_TOKEN_PATTERN = re.compile(r"^[^\s,=]+$")


def _topic(value: str) -> str:
    if not value or not _TOPIC_PATTERN.fullmatch(value):
        raise ValueError(f"invalid topic name: {value!r}")
    return value


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _response_bool(fields: dict[str, str], name: str, *, default: bool | None = None) -> bool:
    if name not in fields:
        if default is not None:
            return default
        raise ValueError(f"missing {name} in admin response")
    value = fields[name].lower()
    if value not in {"true", "false"}:
        raise ValueError(f"invalid {name} in admin response")
    return value == "true"


class AdminClient:
    """Dedicated client for privileged topic mutations.

    Applications can depend on producer/consumer modules without importing this class;
    the broker remains the authority that grants or denies the configured principal.
    """

    def __init__(self, config: AdminConfig | None = None) -> None:
        self.config = config or AdminConfig()
        if not self.config.brokers or any(not value.strip() for value in self.config.brokers):
            raise ValueError("at least one non-empty broker address is required")
        self._client = BrokerCommandClient(
            self.config.brokers,
            timeout_ms=self.config.request_timeout_ms,
            max_retries=self.config.max_retries,
            backoff_ms=self.config.retry_backoff_ms,
            tls_cert_path=self.config.tls_cert_path,
            tls_key_path=self.config.tls_key_path,
            compression_type=self.config.compression_type,
            principal=self.config.principal,
            auth_token=self.config.auth_token,
        )

    def create_topic(self, topic: str, definition: TopicDefinitionPatch) -> TopicDefinition:
        return self._apply_topic_patch(topic, definition)

    def update_topic(self, topic: str, patch: TopicDefinitionPatch) -> TopicDefinition:
        return self._apply_topic_patch(topic, patch)

    def delete_topic(
        self, topic: str, options: DeleteTopicOptions | None = None
    ) -> DeleteTopicResult:
        options = options or DeleteTopicOptions()
        command = f"DELETE topic={_topic(topic)}"
        if options.if_exists:
            command += " if_exists=true"
        response = self._client.send_any(
            command,
            operation="delete topic",
            retry_ambiguous=options.if_exists,
        )
        fields = require_ok(response, operation="delete topic")
        return DeleteTopicResult(
            topic=fields.get("topic", ""),
            deleted=_response_bool(fields, "deleted"),
            cleanup_pending=_response_bool(fields, "cleanup_pending", default=False),
        )

    def truncate_topic(self, topic: str, options: TruncateTopicOptions) -> TruncateTopicResult:
        if options.expected_revision <= 0:
            raise ValueError("expected_revision must be positive")
        command = f"TRUNCATE topic={_topic(topic)} expected_revision={options.expected_revision}"
        response = self._client.send_any(command, operation="truncate topic", retry_ambiguous=False)
        fields = require_ok(response, operation="truncate topic")
        return TruncateTopicResult(
            topic=fields.get("topic", ""),
            truncated=_response_bool(fields, "truncated"),
            revision=int(fields["revision"]),
            lifecycle_epoch=int(fields["lifecycle_epoch"]),
            leo=int(fields["leo"]),
            hwm=int(fields["hwm"]),
            cleanup_pending=_response_bool(fields, "cleanup_pending", default=False),
        )

    def _apply_topic_patch(self, topic: str, patch: TopicDefinitionPatch) -> TopicDefinition:
        parts = ["CREATE", f"topic={_topic(topic)}"]
        positive = {
            "partitions": patch.partitions,
            "replication_factor": patch.replication_factor,
        }
        for name, value in positive.items():
            if value is not None:
                if value <= 0:
                    raise ValueError(f"{name} must be positive")
                parts.append(f"{name}={value}")
        for name, value in {
            "idempotent": patch.idempotent,
            "event_sourcing": patch.event_sourcing,
        }.items():
            if value is not None:
                parts.append(f"{name}={_bool(value)}")
        if patch.cleanup_policy is not None:
            parts.append(f"cleanup_policy={patch.cleanup_policy.value}")
        for name, value in {
            "retention_hours": patch.retention_hours,
            "retention_bytes": patch.retention_bytes,
        }.items():
            if value is not None:
                if value < 0:
                    raise ValueError(f"{name} must be non-negative")
                parts.append(f"{name}={value}")
        if patch.partitioner is not None:
            if patch.partitioner not in {"hash_key", "round_robin"}:
                raise ValueError(f"invalid partitioner: {patch.partitioner}")
            parts.append(f"partitioner={patch.partitioner}")
        if patch.auth_policy is not None:
            if patch.auth_policy not in {"open", "deny_write", "deny_read", "acl"}:
                raise ValueError(f"invalid auth_policy: {patch.auth_policy}")
            parts.append(f"auth_policy={patch.auth_policy}")
        for name, values in (("read_acl", patch.read_acl), ("write_acl", patch.write_acl)):
            if values is not None:
                if any(not _TOKEN_PATTERN.fullmatch(value) for value in values):
                    raise ValueError(f"invalid {name} principal")
                parts.append(f"{name}={','.join(values)}")
        response = self._client.send_any(
            " ".join(parts), operation="create or update topic", retry_ambiguous=True
        )
        fields = require_ok(response, operation="create or update topic")
        required = {
            "topic",
            "revision",
            "lifecycle_epoch",
            "partitions",
            "replication_factor",
            "idempotent",
            "event_sourcing",
            "cleanup_policy",
            "retention_hours",
            "retention_bytes",
            "partitioner",
            "auth_policy",
        }
        missing = required - fields.keys()
        if missing:
            raise ValueError(f"incomplete topic definition: missing {sorted(missing)}")
        return TopicDefinition(
            topic=fields["topic"],
            revision=int(fields["revision"]),
            lifecycle_epoch=int(fields["lifecycle_epoch"]),
            partitions=int(fields["partitions"]),
            replication_factor=int(fields["replication_factor"]),
            idempotent=_response_bool(fields, "idempotent"),
            event_sourcing=_response_bool(fields, "event_sourcing"),
            cleanup_policy=TopicCleanupPolicy(fields["cleanup_policy"]),
            retention_hours=int(fields["retention_hours"]),
            retention_bytes=int(fields["retention_bytes"]),
            partitioner=fields["partitioner"],
            auth_policy=fields["auth_policy"],
            read_acl=[value for value in fields.get("read_acl", "").split(",") if value],
            write_acl=[value for value in fields.get("write_acl", "").split(",") if value],
        )
