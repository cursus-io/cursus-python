import uuid

from cursus import (
    Consumer,
    ConsumerConfig,
    ConsumerMode,
    Producer,
    ProducerConfig,
    TransactionalProducer,
)
from cursus.offsets import OffsetClient
from cursus.types import IsolationLevel


def test_list_offsets_and_transaction_commit_smoke(broker_addr):
    topic = f"txn-{uuid.uuid4().hex[:8]}"
    with Producer(ProducerConfig(brokers=[broker_addr], topic=topic, partitions=1, batch_size=1)):
        pass

    offsets = OffsetClient([broker_addr]).list_offsets(topic, 0)
    assert offsets[0].partition == 0

    tx = TransactionalProducer(f"tx-{uuid.uuid4().hex[:8]}", [broker_addr])
    tx.begin_transaction()
    tx.publish(topic, "committed", partition=0)
    tx.commit_transaction()

    consumer = Consumer(
        ConsumerConfig(
            brokers=[broker_addr],
            topic=topic,
            group_id=f"group-{uuid.uuid4().hex[:8]}",
            mode=ConsumerMode.POLLING,
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
    )
    try:
        for msg in consumer:
            assert msg.payload == "committed"
            break
    finally:
        consumer.close()


def test_transaction_abort_not_visible_to_read_committed(broker_addr):
    topic = f"txn-abort-{uuid.uuid4().hex[:8]}"
    with Producer(ProducerConfig(brokers=[broker_addr], topic=topic, partitions=1, batch_size=1)):
        pass

    tx = TransactionalProducer(f"tx-{uuid.uuid4().hex[:8]}", [broker_addr])
    tx.begin_transaction()
    tx.publish(topic, "aborted", partition=0)
    tx.abort_transaction()

    offsets = OffsetClient([broker_addr]).list_offsets(topic, 0)
    assert offsets[0].latest >= offsets[0].earliest
