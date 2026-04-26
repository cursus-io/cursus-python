from dataclasses import dataclass, field
from enum import Enum


class Acks(str, Enum):
    NONE = "0"
    ONE = "1"
    ALL = "-1"


class ConsumerMode(str, Enum):
    POLLING = "polling"
    STREAMING = "streaming"


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
