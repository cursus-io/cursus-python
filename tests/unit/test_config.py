from cursus.config import ConsumerConfig, ProducerConfig
from cursus.types import Acks, ConsumerMode


def test_producer_config_defaults():
    cfg = ProducerConfig(topic="test-topic")
    assert cfg.brokers == ["localhost:9000"]
    assert cfg.topic == "test-topic"
    assert cfg.partitions == 4
    assert cfg.acks == Acks.ONE
    assert cfg.batch_size == 500
    assert cfg.buffer_size == 10000
    assert cfg.linger_ms == 100
    assert cfg.max_inflight_requests == 5
    assert cfg.idempotent is False
    assert cfg.write_timeout_ms == 5000
    assert cfg.flush_timeout_ms == 30000
    assert cfg.leader_staleness_ms == 30000
    assert cfg.compression_type == "none"
    assert cfg.tls_cert_path is None
    assert cfg.tls_key_path is None
    assert cfg.max_retries == 3
    assert cfg.max_backoff_ms == 10000


def test_producer_config_override():
    cfg = ProducerConfig(
        topic="orders",
        brokers=["broker1:9000", "broker2:9000"],
        partitions=8,
        acks=Acks.ALL,
        batch_size=1000,
    )
    assert cfg.brokers == ["broker1:9000", "broker2:9000"]
    assert cfg.partitions == 8
    assert cfg.acks == Acks.ALL
    assert cfg.batch_size == 1000


def test_consumer_config_defaults():
    cfg = ConsumerConfig(topic="test-topic")
    assert cfg.brokers == ["localhost:9000"]
    assert cfg.topic == "test-topic"
    assert cfg.group_id is None
    assert cfg.consumer_id is not None
    assert cfg.mode == ConsumerMode.STREAMING
    assert cfg.auto_commit_interval_s == 5.0
    assert cfg.session_timeout_ms == 30000
    assert cfg.heartbeat_interval_ms == 3000
    assert cfg.max_poll_records == 100
    assert cfg.batch_size == 100
    assert cfg.immediate_commit is False
    assert cfg.commit_batch_size == 100
    assert cfg.compression_type == "none"
    assert cfg.max_retries == 3
    assert cfg.max_backoff_ms == 10000


def test_consumer_config_auto_id_is_unique():
    cfg1 = ConsumerConfig(topic="t")
    cfg2 = ConsumerConfig(topic="t")
    assert cfg1.consumer_id != cfg2.consumer_id
