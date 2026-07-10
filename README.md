# Cursus Python Client

Python client library for the [Cursus](https://github.com/cursus-io/cursus) message broker with sync and async support.

## Features

- **Producer** — Partition batching, compression, idempotent delivery
- **Consumer** — Polling and streaming modes, consumer groups with rebalance handling
- **EventStore** — Event sourcing with optimistic concurrency, snapshots
- **Sync + Async** — Both `Producer` and `AsyncProducer` (and Consumer, EventStore)

## Requirements

- Python 3.10 or later
- A running Cursus broker (default port `9000`)

## Quick Start

### Install

```bash
pip install cursus-client
```

Verify the installed package:

```bash
python -c "from cursus import Producer, Consumer, EventStore; print('ok')"
```

### Send a message

```python
from cursus import Producer, ProducerConfig, Acks

config = ProducerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    partitions=4,
    acks=Acks.ONE,
)

with Producer(config) as p:
    seq = p.send("Hello, Cursus!")
    p.flush()
    print(f"acked={p.unique_ack_count}")
```

### Consume messages

```python
from cursus import Consumer, ConsumerConfig, ConsumerMode

config = ConsumerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    group_id="my-group",
    mode=ConsumerMode.STREAMING,
)

with Consumer(config) as consumer:
    for msg in consumer:
        print(f"offset={msg.offset} payload={msg.payload}")
```

### Event sourcing

```python
from cursus import EventStore, Event

store = EventStore(addr="localhost:9000", topic="orders-es", producer_id="orders")
store.create_topic(partitions=4)

result = store.append(
    key="order-1001",
    expected_version=1,
    event=Event(type="OrderCreated", payload='{"item": "widget"}'),
)
print(result.version)

stream = store.read_stream("order-1001")
print([event.type for event in stream.events])
store.close()
```

For a cluster, pass bootstrap broker addresses to the clients:

```python
brokers = ["localhost:9001", "localhost:9002", "localhost:9003"]

producer_config = ProducerConfig(brokers=brokers, topic="my-topic")
events = EventStore(addr=brokers, topic="orders-es", producer_id="orders")
```

### Async

```python
from cursus import AsyncProducer, ProducerConfig

async with AsyncProducer(ProducerConfig(topic="my-topic")) as p:
    await p.send("Hello, async!")
    await p.flush()
```

## Optional dependencies

```bash
pip install cursus-client[snappy]    # Snappy compression
pip install cursus-client[lz4]       # LZ4 compression
```

## More Examples

- [Getting Started](docs/getting-started.md)
- [Producer Guide](docs/producer-guide.md)
- [Consumer Guide](docs/consumer-guide.md)
- [Standalone examples](examples/standalone/README.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
