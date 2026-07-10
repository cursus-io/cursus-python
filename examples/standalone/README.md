# Standalone Examples

These examples demonstrate cursus-client without any framework.

## Prerequisites

- Python 3.10+
- A running Cursus broker on `localhost:9000`
- `pip install cursus-client`
- Optional import check: `python -c "from cursus import Producer, Consumer, EventStore; print('ok')"`

## Examples

| File | Description |
|---|---|
| `simple_producer.py` | Send a single message |
| `simple_consumer.py` | Consume messages with iterator pattern |
| `keyed_producer.py` | Key-based partition routing |
| `batch_producer.py` | High-throughput batch tuning |
| `consumer_group.py` | 3 consumers sharing partitions |
| `event_sourcing.py` | EventStore append, read, snapshots |

## Run

```bash
python simple_producer.py
python simple_consumer.py
python event_sourcing.py
```

Cluster examples use the same APIs. Pass all bootstrap brokers, for example
`["localhost:9001", "localhost:9002", "localhost:9003"]`, instead of a single
`localhost:9000` address.
