from dataclasses import dataclass, field
from uuid import uuid4

from cursus.types import Acks, ConsumerMode


def _default_brokers() -> list[str]:
    return ["localhost:9000"]


def _generate_consumer_id() -> str:
    return f"consumer-{uuid4().hex[:8]}"


@dataclass
class ProducerConfig:
    topic: str
    brokers: list[str] = field(default_factory=_default_brokers)
    partitions: int = 4
    acks: Acks = Acks.ONE
    batch_size: int = 500
    buffer_size: int = 10000
    linger_ms: int = 100
    max_inflight_requests: int = 5
    idempotent: bool = False
    write_timeout_ms: int = 5000
    flush_timeout_ms: int = 30000
    leader_staleness_ms: int = 30000
    compression_type: str = "none"
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    max_retries: int = 3
    max_backoff_ms: int = 10000


@dataclass
class ConsumerConfig:
    topic: str
    brokers: list[str] = field(default_factory=_default_brokers)
    group_id: str | None = None
    consumer_id: str = field(default_factory=_generate_consumer_id)
    mode: ConsumerMode = ConsumerMode.STREAMING
    auto_commit_interval_s: float = 5.0
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    max_poll_records: int = 100
    batch_size: int = 100
    immediate_commit: bool = False
    commit_batch_size: int = 100
    compression_type: str = "none"
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    metadata_refresh_interval_ms: int = 30000
    max_retries: int = 3
    max_backoff_ms: int = 10000
