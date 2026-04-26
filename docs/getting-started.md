# Getting Started

## Quick Start Flow

```mermaid
flowchart LR
    Install["pip install cursus-client"]
    Broker["docker run cursusio/cursus\n→ broker on :9000"]
    Produce["Producer.send()\n→ flush()"]
    Consume["Consumer iterate\n→ yield Message"]

    Install --> Broker --> Produce --> Consume
```

## Prerequisites

- **Python 3.10 or later**
- **A running Cursus broker** — listens on TCP port `9000` by default.

## Installation

```bash
pip install cursus-client
```

Optional compression support:

```bash
pip install cursus-client[snappy]   # Snappy compression
pip install cursus-client[lz4]      # LZ4 compression
```

## Start the broker

```bash
docker run -d --name cursus -p 9000:9000 cursusio/cursus:latest
```

Verify: `docker logs cursus` — look for `Cursus broker listening on :9000`.

## Send your first message

```python
from cursus import Producer, ProducerConfig, Acks

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
```

## Consume messages

```python
from cursus import Consumer, ConsumerConfig, ConsumerMode

config = ConsumerConfig(
    brokers=["localhost:9000"],
    topic="hello-topic",
    group_id="hello-group",
    mode=ConsumerMode.STREAMING,
)

with Consumer(config) as consumer:
    for msg in consumer:
        print(f"offset={msg.offset}  payload={msg.payload}")
```

## Next steps

- [Producer Guide](producer-guide.md)
- [Consumer Guide](consumer-guide.md)
- [Configuration Reference](configuration-reference.md)
- [Examples](../examples/standalone/README.md)
