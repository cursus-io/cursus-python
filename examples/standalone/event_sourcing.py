from cursus import EventStore, Event


def main():
    es = EventStore(addr="localhost:9000", topic="orders-es", producer_id="order-svc")

    es.create_topic(partitions=4)

    key = "order-1001"

    result = es.append(
        key=key,
        expected_version=1,
        event=Event(type="OrderCreated", payload='{"item": "widget", "qty": 5}'),
    )
    print(f"Appended: version={result.version} offset={result.offset}")

    result = es.append(
        key=key,
        expected_version=2,
        event=Event(type="OrderShipped", payload='{"tracking": "ABC123"}'),
    )
    print(f"Appended: version={result.version} offset={result.offset}")

    stream = es.read_stream(key)
    print(f"\nStream for {key} ({len(stream.events)} events):")
    for evt in stream.events:
        print(f"  v{evt.version}: {evt.type} -- {evt.payload}")

    if stream.snapshot:
        print(f"  Snapshot at v{stream.snapshot.version}")

    es.close()


if __name__ == "__main__":
    main()
