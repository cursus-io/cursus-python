# Producer Guide

## Basic Usage

```python
from cursus import Producer, ProducerConfig, Acks

config = ProducerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    partitions=4,
    acks=Acks.ONE,
)

with Producer(config) as p:
    p.send("Hello!")
    p.flush()
```

## Partition Routing

- **No key**: round-robin across partitions
- **With key**: FNV-1a hash determines partition (same key always goes to same partition)

```python
p.send("order data", key="user-42")
```

```mermaid
flowchart TD
    Send["p.send(payload, key?)"]
    HasKey{key provided?}
    Hash["FNV-1a hash(key) % partitions\n→ fixed partition"]
    RR["round-robin counter % partitions\n→ next partition"]
    Buffer["PartitionBuffer[partition]"]
    Flush{flush trigger?}
    BatchSend["encode_batch() → write_frame()"]
    Broker([Cursus Broker])

    Send --> HasKey
    HasKey -->|yes| Hash
    HasKey -->|no| RR
    Hash --> Buffer
    RR --> Buffer
    Buffer --> Flush
    Flush -->|"batch_size reached\nor linger_ms fired\nor flush() called"| BatchSend
    Flush -->|no| Buffer
    BatchSend --> Broker
```

## Batching

Messages are buffered per-partition and flushed when:
- Buffer reaches `batch_size` (default: 500)
- `linger_ms` timer fires (default: 100ms)
- `flush()` is called explicitly

```mermaid
flowchart LR
    Msg["p.send(msg)"] --> Buf["PartitionBuffer"]
    Buf --> T1{"batch_size\nreached?"}
    Buf --> T2{"linger_ms\nfired?"}
    Buf --> T3{"flush()\ncalled?"}
    T1 -->|yes| Encode
    T2 -->|yes| Encode
    T3 -->|yes| Encode
    Encode["encode_batch()"] --> Wire["write_frame() → TCP"]
    Wire --> Broker([Broker])
    Broker -->|ACK| Ack["update ack_count"]
```

Tune for throughput:
```python
config = ProducerConfig(
    topic="high-throughput",
    batch_size=1000,
    linger_ms=200,
    buffer_size=50000,
)
```

Tune for latency:
```python
config = ProducerConfig(
    topic="low-latency",
    batch_size=1,
    linger_ms=0,
)
```

## Compression

```python
config = ProducerConfig(
    topic="compressed",
    compression_type="gzip",  # or "snappy", "lz4"
)
```

Snappy and LZ4 require extras: `pip install cursus-client[snappy,lz4]`

## Idempotency

```python
config = ProducerConfig(
    topic="exactly-once",
    idempotent=True,
    max_inflight_requests=1,
)
```

## Retry

Failed batches are retried up to `max_retries` (default: 3) with exponential backoff starting at 100ms, capped at `max_backoff_ms` (default: 10000ms). If all retries fail, the batch is re-queued to the partition buffer.

```mermaid
flowchart TD
    Send["send batch → Broker"]
    Send --> Result{ACK received?}
    Result -->|success| Done["update ack counters"]
    Result -->|error| Retry{retries < max_retries?}
    Retry -->|yes| Backoff["exponential backoff\n100ms × 2^attempt\n(max max_backoff_ms)"]
    Backoff --> Send
    Retry -->|no| Requeue["re-queue to PartitionBuffer"]
```

## Async

```python
from cursus import AsyncProducer, ProducerConfig

async with AsyncProducer(ProducerConfig(topic="my-topic")) as p:
    await p.send("Hello, async!")
    await p.flush()
```

## Shutdown

`close()` (or exiting the context manager) signals all sender threads to stop, waits for them to finish, and closes connections. Always close the producer to avoid message loss.
