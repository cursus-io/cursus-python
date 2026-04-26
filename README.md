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

## License

Apache License 2.0. See [LICENSE](LICENSE).
