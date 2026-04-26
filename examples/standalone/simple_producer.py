from cursus import Producer, ProducerConfig, Acks


def main():
    config = ProducerConfig(
        brokers=["localhost:9000"],
        topic="hello-topic",
        partitions=1,
        acks=Acks.ONE,
        batch_size=1,
        linger_ms=0,
    )

    with Producer(config) as p:
        seq = p.send("Hello, Cursus!")
        p.flush()
        print(f"Sent seq={seq}  acked={p.unique_ack_count}")


if __name__ == "__main__":
    main()
