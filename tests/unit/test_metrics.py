from cursus.errors import BrokerError, ConnectionError, ProducerFencedError, ProtocolError
from cursus.metrics import ClientMetrics, classify_error


def test_client_metrics_snapshot_is_stable_and_isolated() -> None:
    metrics = ClientMetrics()
    metrics.increment("cursus.producer.messages.sent", 2)
    metrics.increment("cursus.producer.messages.acked")
    metrics.observe("cursus.producer.send.latency", 0.25)
    metrics.observe("cursus.producer.send.latency", 0.5)
    snapshot = metrics.snapshot()
    assert snapshot.counters == {
        "cursus.producer.messages.sent": 2,
        "cursus.producer.messages.acked": 1,
    }
    assert snapshot.latency_count["cursus.producer.send.latency"] == 2
    assert snapshot.latency_total_s["cursus.producer.send.latency"] == 0.75
    assert snapshot.latency_max_s["cursus.producer.send.latency"] == 0.5
    snapshot.counters["mutated"] = 1
    assert "mutated" not in metrics.snapshot().counters


def test_error_classification_matches_typed_client_errors() -> None:
    assert classify_error(ConnectionError("down")) == "transport"
    assert classify_error(ProtocolError("bad frame")) == "protocol"
    assert classify_error(BrokerError("unavailable", "availability")) == "availability"
    assert classify_error(ProducerFencedError("fenced", "fencing")) == "fencing"
    assert classify_error(RuntimeError("unknown")) == "internal"
