from dataclasses import dataclass, field
from enum import Enum


class Acks(str, Enum):
    NONE = "0"
    ONE = "1"
    ALL = "-1"


class ConsumerMode(str, Enum):
    POLLING = "polling"
    STREAMING = "streaming"


class AutoOffsetReset(str, Enum):
    EARLIEST = "earliest"
    LATEST = "latest"
    ERROR = "error"


class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"


@dataclass
class Message:
    offset: int
    seq_num: int
    payload: str
    key: str = ""
    producer_id: str = ""
    epoch: int = 0
    event_type: str = ""
    schema_version: int = 0
    aggregate_version: int = 0
    metadata: str = ""
    partition: int = 0


@dataclass
class AckResponse:
    status: str
    last_offset: int
    producer_epoch: int
    producer_id: str
    seq_start: int
    seq_end: int
    leader: str = ""
    error: str = ""


@dataclass(frozen=True)
class OffsetRange:
    requested: int
    earliest: int
    latest: int


@dataclass(frozen=True)
class PartitionOffsetRange:
    partition: int
    earliest: int
    latest: int
    leo: int
    hwm: int


@dataclass(frozen=True)
class ProducerSession:
    transactional_id: str
    producer_id: str
    epoch: int


@dataclass(frozen=True)
class TransactionStatus:
    transactional_id: str
    state: str
    messages: int = 0
    offsets: int = 0


@dataclass(frozen=True)
class StreamControl:
    type: str
    reason: str = ""
    offset: int | None = None
    requested: int | None = None
    earliest: int | None = None
    latest: int | None = None


@dataclass
class Event:
    type: str
    payload: str
    schema_version: int = 1
    metadata: str = ""


@dataclass
class StreamEvent:
    version: int
    offset: int
    payload: str
    type: str = ""
    schema_version: int = 0
    metadata: str = ""


@dataclass
class Snapshot:
    version: int
    payload: str


@dataclass
class StreamData:
    snapshot: Snapshot | None
    events: list[StreamEvent] = field(default_factory=list)


@dataclass
class AppendResult:
    version: int
    offset: int
    partition: int
