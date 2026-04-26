from cursus import Producer, ProducerConfig, Acks


def main():
    config = ProducerConfig(
        brokers=["localhost:9000"],
        topic="orders",
        partitions=4,
        acks=Acks.ONE,
    )

    with Producer(config) as p:
        for i in range(20):
            key = f"user-{i % 5}"
            seq = p.send(f'{{"order_id": {i}, "user": "{key}"}}', key=key)
            print(f"Sent order {i} with key={key} seq={seq}")
        p.flush()
        print(f"Total acked: {p.unique_ack_count}")


if __name__ == "__main__":
    main()
