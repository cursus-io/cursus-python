from cursus.producer import Producer


def test_fnv1a_32_known_values():
    assert Producer._fnv1a_32(b"") == 0x811C9DC5
    assert Producer._fnv1a_32(b"a") == 0xE40C292C


def test_partition_for_key_deterministic():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=4)
    p = Producer.__new__(Producer)
    p._config = cfg
    part1 = p._partition_for_key("order-123")
    part2 = p._partition_for_key("order-123")
    assert part1 == part2


def test_partition_for_key_within_range():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=8)
    p = Producer.__new__(Producer)
    p._config = cfg
    for key in ["a", "b", "order-1", "order-2", "user-xyz"]:
        part = p._partition_for_key(key)
        assert 0 <= part < 8


def test_partition_for_key_distributes():
    from cursus.config import ProducerConfig

    cfg = ProducerConfig(topic="t", partitions=4)
    p = Producer.__new__(Producer)
    p._config = cfg
    partitions_seen = set()
    for i in range(100):
        partitions_seen.add(p._partition_for_key(f"key-{i}"))
    assert len(partitions_seen) > 1
