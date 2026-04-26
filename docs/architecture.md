# Architecture

## Module Structure

```
cursus-python
|
+-- src/cursus/                       Core library (PyPI: cursus-client)
|   +-- compression/                  CursusCompressor protocol, GzipCompressor, CompressionRegistry
|   +-- config.py                     ProducerConfig, ConsumerConfig
|   +-- connection/                   SyncConnection, AsyncConnection, framing
|   +-- protocol/                     ProtocolEncoder, ProtocolDecoder, CommandBuilder
|   +-- errors.py                     CursusError hierarchy
|   +-- types.py                      Message, AckResponse, Event, Snapshot, enums
|   +-- producer.py                   Producer (sync, threaded partition senders)
|   +-- consumer.py                   Consumer (sync, group protocol, iterator)
|   +-- eventstore.py                 EventStore (sync, optimistic concurrency)
|   +-- async_producer.py             AsyncProducer (asyncio tasks)
|   +-- async_consumer.py             AsyncConsumer (async iterator)
|   +-- async_eventstore.py           AsyncEventStore
|
+-- examples
    +-- standalone/                   6 runnable examples
    +-- fastapi/                      FastAPI REST app with async producer
```

## Layer Diagram

```mermaid
flowchart TB
    A["Public API\nProducer · Consumer · EventStore\nAsyncProducer · AsyncConsumer · AsyncEventStore"]
    B["Configuration\nProducerConfig · ConsumerConfig (dataclass)"]
    C["Protocol\nencode_message · encode_batch · decode_batch\ndecode_ack · CommandBuilder"]
    D["Connection\nSyncConnection (socket) · AsyncConnection (asyncio)"]
    E["Compression\ngzip (builtin) · snappy/lz4 (extras)"]
    F["Transport: TCP · port 9000"]
    G["Cursus Broker (Go)"]

    A --> B --> C --> D --> E --> F --> G
```

## Producer Data Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant P as Producer
    participant B as PartitionBuffer
    participant S as SenderThread
    participant C as SyncConnection
    participant Broker as Cursus Broker

    App->>P: send(payload, key?)
    P->>P: route partition (FNV-1a hash or round-robin)
    P->>B: enqueue message
    note over B: buffer fills or linger_ms fires
    B->>S: flush batch
    S->>C: write_frame(encode_batch)
    C->>Broker: TCP frame
    Broker-->>C: ACK (JSON)
    C-->>S: read_frame
    S->>P: update ack counters
    App->>P: flush() [optional explicit flush]
```

## Consumer Data Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant Con as Consumer
    participant W as WorkerThread
    participant C as SyncConnection
    participant Broker as Cursus Broker

    App->>Con: start() / iterate
    Con->>C: JOIN_GROUP
    C->>Broker: JOIN_GROUP command
    Broker-->>C: partition assignment
    C-->>Con: SYNC_GROUP response
    loop heartbeat
        Con->>Broker: HEARTBEAT
    end
    Con->>C: STREAM or CONSUME command
    C->>Broker: subscribe
    loop messages
        Broker-->>C: batch frame
        C-->>W: decode_batch
        W->>App: yield Message
        App->>Con: (auto) commit offset
        Con->>Broker: COMMIT_OFFSET / BATCH_COMMIT
    end
    App->>Con: close()
    Con->>Broker: LEAVE_GROUP
```

## Go SDK Mapping

| Go SDK | Python SDK |
|---|---|
| `PublisherConfig` | `ProducerConfig` |
| `ConsumerConfig` | `ConsumerConfig` |
| `Message` struct | `Message` dataclass |
| `AckResponse` struct | `AckResponse` dataclass |
| `EncodeMessage` / `EncodeBatchMessages` | `encode_message()` / `encode_batch()` |
| `DecodeBatchMessages` | `decode_batch()` |
| `WriteWithLength` / `ReadWithLength` | `SyncConnection.write_frame()` / `.read_frame()` |
| `CompressMessage` | `CompressionRegistry.compress()` |
| `hash/fnv` partition routing | `Producer._fnv1a_32()` |
| `BATCH_MAGIC = 0xBA7C` | `BATCH_MAGIC = 0xBA7C` |
| `EventStore.Append()` | `EventStore.append()` |
