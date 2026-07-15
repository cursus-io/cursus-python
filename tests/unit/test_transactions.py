import pytest

from cursus.broker_client import BrokerCommandClient
from cursus.errors import AuthorizationDeniedError, ProducerFencedError
from cursus.offsets import OffsetClient
from cursus.transaction import TransactionalProducer


class FakeBrokerClient(BrokerCommandClient):
    def __init__(self, responses: dict[tuple[str, str], list[str]]) -> None:
        super().__init__(["bootstrap:9000"], backoff_ms=0)
        self.responses = {key: list(value) for key, value in responses.items()}
        self.commands: list[tuple[str, str]] = []

    def _send_to_addr(self, addr: str, cmd: str) -> str:
        self.commands.append((addr, cmd))
        key = (addr, cmd)
        if key not in self.responses or not self.responses[key]:
            raise AssertionError(f"unexpected command {key}")
        return self.responses[key].pop(0)


def make_txn(client: FakeBrokerClient) -> TransactionalProducer:
    producer = TransactionalProducer(
        "tx-1", ["bootstrap:9000"], principal="alice", auth_token="secret"
    )
    producer._client = client
    return producer


def test_offset_client_lists_offsets_with_auth():
    client = FakeBrokerClient(
        {
            (
                "bootstrap:9000",
                "LIST_OFFSETS topic=orders partition=0 principal=alice auth_token=secret",
            ): ["OK topic=orders partitions=1 offsets=P0:earliest=0:latest=3:leo=3:hwm=3"],
        }
    )
    offsets = OffsetClient(["bootstrap:9000"], principal="alice", auth_token="secret")
    offsets._client = client

    result = offsets.list_offsets("orders", 0)

    assert result[0].latest == 3


def test_transaction_redirect_and_publish_partition_minus_one():
    init = "INIT_PRODUCER_ID transactional_id=tx-1"
    begin = "BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=4"
    publish = (
        "TXN_PUBLISH transactional_id=tx-1 topic=out partition=-1 producerId=p1 seqNum=1 "
        "epoch=4 message=processed principal=alice auth_token=secret"
    )
    client = FakeBrokerClient(
        {
            ("bootstrap:9000", init): ["ERROR: NOT_COORDINATOR host=broker-b port=9002"],
            ("broker-b:9002", init): ["OK transactional_id=tx-1 producerId=p1 epoch=4"],
            ("broker-b:9002", begin): ["OK transactional_id=tx-1 state=open producerId=p1 epoch=4"],
            ("broker-b:9002", publish): [
                "OK transactional_id=tx-1 staged_messages=1 topic=out partition=2"
            ],
        }
    )
    producer = make_txn(client)

    producer.begin_transaction()
    producer.publish("out", "processed", partition=-1)

    assert producer.producer_id == "p1"
    assert producer.epoch == 4
    assert client.commands == [
        ("bootstrap:9000", init),
        ("broker-b:9002", init),
        ("broker-b:9002", begin),
        ("broker-b:9002", publish),
    ]


def test_send_offsets_sorted_and_commit_abort_idempotent_success():
    producer = TransactionalProducer("tx-1", ["bootstrap:9000"])
    producer.producer_id = "p1"
    producer.epoch = 2
    client = FakeBrokerClient(
        {
            (
                "bootstrap:9000",
                "FIND_COORDINATOR transactional_id=tx-1",
            ): ["OK coordinator_id=1 coordinator_type=transaction host=broker-a port=9001"],
            (
                "bootstrap:9000",
                "SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=2 topic=input "
                "group=grp member=m1 generation=7 P0:11,P2:21",
            ): ["OK transactional_id=tx-1 staged_offsets=2"],
            (
                "bootstrap:9000",
                "END_TXN transactional_id=tx-1 producerId=p1 epoch=2 result=commit",
            ): ["OK transactional_id=tx-1 state=committed messages=0 offsets=2"],
            (
                "bootstrap:9000",
                "END_TXN transactional_id=tx-1 producerId=p1 epoch=2 result=abort",
            ): ["OK transactional_id=tx-1 state=aborted"],
        }
    )
    producer._client = client

    producer.send_offsets_to_transaction("input", "grp", "m1", 7, {2: 21, 0: 11})
    producer.commit_transaction()
    producer.abort_transaction()


def test_transaction_context_aborts_on_exception():
    producer = TransactionalProducer("tx-1", ["bootstrap:9000"])
    producer.producer_id = "p1"
    producer.epoch = 2
    client = FakeBrokerClient(
        {
            ("bootstrap:9000", "BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=2"): [
                "OK transactional_id=tx-1 state=open producerId=p1 epoch=2"
            ],
            (
                "bootstrap:9000",
                "END_TXN transactional_id=tx-1 producerId=p1 epoch=2 result=abort",
            ): ["OK transactional_id=tx-1 state=aborted"],
        }
    )
    producer._client = client

    with pytest.raises(RuntimeError):
        with producer.transaction():
            raise RuntimeError("handler failed")


def test_transaction_fencing_and_authorization_errors_are_typed():
    producer = TransactionalProducer("tx-1", ["bootstrap:9000"])
    producer.producer_id = "p1"
    producer.epoch = 1
    client = FakeBrokerClient(
        {
            ("bootstrap:9000", "BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=1"): [
                "ERROR: producer_fenced transactional_id=tx-1 current_epoch=2 requested_epoch=1"
            ],
        }
    )
    producer._client = client

    with pytest.raises(ProducerFencedError):
        producer.begin_transaction()

    client = FakeBrokerClient(
        {
            ("bootstrap:9000", "LIST_OFFSETS topic=orders"): [
                "ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=orders operation=read"
            ],
        }
    )
    offsets = OffsetClient(["bootstrap:9000"])
    offsets._client = client
    with pytest.raises(AuthorizationDeniedError):
        offsets.list_offsets("orders")
