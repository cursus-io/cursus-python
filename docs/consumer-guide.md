# Consumer Guide

## Basic Usage — Iterator (recommended)

```python
from cursus import Consumer, ConsumerConfig, ConsumerMode

config = ConsumerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    group_id="my-group",
    mode=ConsumerMode.STREAMING,
    auto_offset_reset="earliest",
)

with Consumer(config) as consumer:
    for msg in consumer:
        print(f"offset={msg.offset} payload={msg.payload}")
```

## Callback Style

```python
consumer = Consumer(config)
consumer.start(lambda msg: print(msg.payload))
```

`start()` blocks until `close()` is called (e.g., from a signal handler or another thread).

## Consumer Groups

Multiple consumers with the same `group_id` share partitions via the broker's group protocol:

1. `JOIN_GROUP` — register as a member
2. `SYNC_GROUP` — receive partition assignments
3. `HEARTBEAT` — keep session alive (every `heartbeat_interval_ms`)
4. `LEAVE_GROUP` — on close, triggers rebalance for remaining members

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant Broker as Cursus Broker

    App->>C: Consumer(config) / start()
    C->>Broker: JOIN_GROUP topic group member
    Broker-->>C: partition assignments
    C->>Broker: SYNC_GROUP generation
    Broker-->>C: confirmed assignment

    loop every heartbeat_interval_ms
        C->>Broker: HEARTBEAT
        Broker-->>C: OK
    end

    C->>Broker: STREAM / CONSUME (assigned partitions)
    loop messages
        Broker-->>C: batch frame
        C-->>App: yield Message
        C->>Broker: COMMIT_OFFSET / BATCH_COMMIT
    end

    App->>C: close()
    C->>Broker: LEAVE_GROUP
    Broker-->>C: rebalance triggered
```

## Modes

| Mode | Behavior |
|---|---|
| `STREAMING` | Broker pushes messages over persistent connection |
| `POLLING` | Client polls with `CONSUME` command each interval |

```mermaid
flowchart TD
    Start([Consumer started])
    Start --> ModeCheck{ConsumerMode?}

    ModeCheck -->|STREAMING| Stream["Send STREAM command\nBroker holds connection open\nMessages pushed as they arrive"]
    ModeCheck -->|POLLING| Poll["Send CONSUME command\nRead batch\nSleep poll_interval_ms\nRepeat"]

    Stream --> Decode["decode_batch()"]
    Poll --> Decode
    Decode --> Yield["yield Message to caller"]
    Yield --> Commit{commit trigger?}
    Commit -->|"immediate_commit\nor batch_size\nor interval"| CommitOp["COMMIT_OFFSET /\nBATCH_COMMIT"]
    Commit -->|no| Yield
    CommitOp --> Yield
```

## Offset Management

Offsets are tracked per `(topic, group_id, partition)` and the broker committed offset is the source of truth for resume. After `JOIN_GROUP` / `SYNC_GROUP`, the SDK calls `FETCH_OFFSET` for each assigned partition and starts `CONSUME` / `STREAM` from that broker-reported next offset.

Configure with:
- `auto_offset_reset` (default: `earliest`): `earliest`, `latest`, or `error` when the committed/requested offset is outside retention
- `auto_commit_interval_s` (default: 5.0)
- `immediate_commit` (default: False)
- `commit_batch_size` (default: 100)

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Consumer
    participant Broker as Cursus Broker

    Broker-->>C: Message (offset=N, partition=P)
    C->>C: nextFetchOffset[P] = N + 1
    C-->>App: yield Message
    App-->>C: handler/iterator body returns

    alt immediate_commit=True
        C->>Broker: COMMIT_OFFSET partition=P offset=N+1
        Broker-->>C: OK
    else commit_batch_size reached
        C->>Broker: BATCH_COMMIT P<partition>:N+1,...
        Broker-->>C: OK
    else auto_commit_interval_s fires
        C->>Broker: COMMIT_OFFSET lastProcessedOffset+1
        Broker-->>C: OK
    end
```

## Async

```python
from cursus import AsyncConsumer, ConsumerConfig

async with AsyncConsumer(config) as consumer:
    async for msg in consumer:
        print(msg.payload)
```

## Shutdown

`close()` sends `LEAVE_GROUP`, stops workers, and joins threads. Use signal handlers or context managers for clean shutdown.


### Delivery semantics

For at-least-once processing, process the message first and commit `lastProcessedOffset + 1` only after the handler succeeds. Committing before processing is possible for at-most-once workflows, but a crash after the commit can skip unprocessed records. Cursus does not yet provide Kafka transaction-level exactly-once semantics.

In iterator style, the SDK marks a yielded message as processed when the loop advances to the next item. If the application breaks immediately after handling a message, `close()` marks the last delivered message before flushing dirty offsets.

If the broker returns `ERROR: offset_regression ...`, the SDK treats the commit as failed and does not advance or rewind local committed state. Coordinator failures such as `GEN_MISMATCH`, `NOT_OWNER`, `member_not_found`, `group_not_found`, and `NOT_COORDINATOR` trigger rediscovery/rejoin behavior.

Streaming consumers recognize UTF-8 `STREAM_CONTROL` frames before binary batch decoding. `STREAM_CONTROL type=CLOSE reason=offset_out_of_range ...` applies the same `auto_offset_reset` policy as pull `ERROR: OFFSET_OUT_OF_RANGE ...`; zero-length stream frames are keepalives.

External DB offset stores should be treated as legacy fallback or migration aids; broker committed offsets are the default source of truth.
