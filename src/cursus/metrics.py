from __future__ import annotations

import threading
from dataclasses import dataclass

from cursus.errors import BrokerError, ConnectionError, ProducerFencedError, ProtocolError


@dataclass(frozen=True)
class MetricsSnapshot:
    counters: dict[str, int]
    latency_count: dict[str, int]
    latency_total_s: dict[str, float]
    latency_max_s: dict[str, float]


class ClientMetrics:
    """Thread-safe, dependency-free client metrics with stable cross-language names."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._latency_count: dict[str, int] = {}
        self._latency_total_s: dict[str, float] = {}
        self._latency_max_s: dict[str, float] = {}

    def increment(self, name: str, count: int = 1) -> None:
        if not name or count < 0:
            raise ValueError("metric name is required and count must be non-negative")
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + count

    def observe(self, name: str, duration_s: float) -> None:
        if not name or duration_s < 0:
            raise ValueError("metric name is required and duration must be non-negative")
        with self._lock:
            self._latency_count[name] = self._latency_count.get(name, 0) + 1
            self._latency_total_s[name] = self._latency_total_s.get(name, 0.0) + duration_s
            self._latency_max_s[name] = max(duration_s, self._latency_max_s.get(name, 0.0))

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                counters=dict(self._counters),
                latency_count=dict(self._latency_count),
                latency_total_s=dict(self._latency_total_s),
                latency_max_s=dict(self._latency_max_s),
            )


def classify_error(error: BaseException) -> str:
    if isinstance(error, ProducerFencedError):
        return "fencing"
    if isinstance(error, BrokerError):
        return error.error_class.lower() or "broker"
    if isinstance(error, ProtocolError):
        return "protocol"
    if isinstance(error, ConnectionError):
        return "transport"
    return "internal"
