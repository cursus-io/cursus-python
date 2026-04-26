import time

from cursus import Producer, ProducerConfig, Acks


def main():
    config = ProducerConfig(
        brokers=["localhost:9000"],
        topic="batch-topic",
        partitions=4,
        acks=Acks.ONE,
        batch_size=500,
        linger_ms=100,
        buffer_size=10000,
    )

    with Producer(config) as p:
        start = time.monotonic()
        for i in range(10000):
            p.send(f"message-{i}")
        p.flush()
        elapsed = time.monotonic() - start
        print(f"Sent 10000 messages in {elapsed:.3f}s  acked={p.unique_ack_count}")


if __name__ == "__main__":
    main()
