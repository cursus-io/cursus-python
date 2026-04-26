# Configuration Reference

## ProducerConfig

| Property | Type | Default | Description |
|---|---|---|---|
| `brokers` | `list[str]` | `["localhost:9000"]` | Broker addresses |
| `topic` | `str` | **required** | Topic to publish to |
| `partitions` | `int` | `4` | Number of partitions |
| `acks` | `Acks` | `Acks.ONE` | `NONE`, `ONE`, `ALL` |
| `batch_size` | `int` | `500` | Max messages per batch |
| `buffer_size` | `int` | `10000` | Max buffered messages per partition |
| `linger_ms` | `int` | `100` | Batch flush delay (ms) |
| `max_inflight_requests` | `int` | `5` | Concurrent in-flight sends |
| `idempotent` | `bool` | `False` | Enable deduplication |
| `write_timeout_ms` | `int` | `5000` | Broker response timeout |
| `flush_timeout_ms` | `int` | `30000` | `flush()` timeout |
| `leader_staleness_ms` | `int` | `30000` | Leader cache TTL |
| `compression_type` | `str` | `"none"` | `"none"`, `"gzip"`, `"snappy"`, `"lz4"` |
| `tls_cert_path` | `str \| None` | `None` | TLS certificate path |
| `tls_key_path` | `str \| None` | `None` | TLS key path |
| `max_retries` | `int` | `3` | Retry attempts per batch |
| `max_backoff_ms` | `int` | `10000` | Max backoff between retries |

## ConsumerConfig

| Property | Type | Default | Description |
|---|---|---|---|
| `brokers` | `list[str]` | `["localhost:9000"]` | Broker addresses |
| `topic` | `str` | **required** | Topic to consume |
| `group_id` | `str \| None` | `None` | Consumer group ID |
| `consumer_id` | `str` | auto-generated | Consumer identifier |
| `mode` | `ConsumerMode` | `STREAMING` | `POLLING` or `STREAMING` |
| `auto_commit_interval_s` | `float` | `5.0` | Offset commit interval (seconds) |
| `session_timeout_ms` | `int` | `30000` | Session timeout |
| `heartbeat_interval_ms` | `int` | `3000` | Heartbeat interval |
| `max_poll_records` | `int` | `100` | Max records per poll |
| `batch_size` | `int` | `100` | Batch hint size |
| `immediate_commit` | `bool` | `False` | Commit after each message |
| `commit_batch_size` | `int` | `100` | Commit batch threshold |
| `compression_type` | `str` | `"none"` | Compression algorithm |
| `tls_cert_path` | `str \| None` | `None` | TLS certificate |
| `tls_key_path` | `str \| None` | `None` | TLS key |
| `max_retries` | `int` | `3` | Reconnect attempts |
| `max_backoff_ms` | `int` | `10000` | Max reconnect backoff |
