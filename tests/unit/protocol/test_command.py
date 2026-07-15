from cursus.protocol.command import CommandBuilder


def test_create():
    assert CommandBuilder.create("orders", 4) == "CREATE topic=orders partitions=4"


def test_create_event_sourcing():
    result = CommandBuilder.create("events", 2, event_sourcing=True)
    assert result == "CREATE topic=events partitions=2 event_sourcing=true"


def test_consume():
    result = CommandBuilder.consume("orders", 0, 100, "m1", group="grp", generation=3)
    assert "CONSUME" in result
    assert "topic=orders" in result
    assert "partition=0" in result
    assert "offset=100" in result
    assert "member=m1" in result
    assert "group=grp" in result
    assert "generation=3" in result


def test_stream():
    result = CommandBuilder.stream("orders", 1, "grp", "m1", 3, offset=50)
    assert "STREAM" in result
    assert "topic=orders" in result
    assert "partition=1" in result
    assert "group=grp" in result
    assert "member=m1" in result
    assert "generation=3" in result
    assert "offset=50" in result


def test_join_group():
    result = CommandBuilder.join_group("orders", "grp", "m1")
    assert result == "JOIN_GROUP topic=orders group=grp member=m1"


def test_sync_group():
    result = CommandBuilder.sync_group("orders", "grp", "m1", 5)
    assert result == "SYNC_GROUP topic=orders group=grp member=m1 generation=5"


def test_leave_group():
    result = CommandBuilder.leave_group("orders", "grp", "m1")
    assert result == "LEAVE_GROUP topic=orders group=grp member=m1"


def test_heartbeat():
    result = CommandBuilder.heartbeat("orders", "grp", "m1", 3)
    assert result == "HEARTBEAT topic=orders group=grp member=m1 generation=3"


def test_commit_offset():
    result = CommandBuilder.commit_offset("orders", "grp", 0, 42, 3, "m1")
    expected = "COMMIT_OFFSET topic=orders partition=0 group=grp offset=42 generation=3 member=m1"
    assert result == expected


def test_batch_commit():
    offsets = {0: 100, 1: 21}
    result = CommandBuilder.batch_commit("orders", "grp", "m1", 3, offsets)
    assert result == "BATCH_COMMIT topic=orders group=grp member=m1 generation=3 P0:100,P1:21"


def test_fetch_offset():
    result = CommandBuilder.fetch_offset("orders", 0, "grp")
    assert result == "FETCH_OFFSET topic=orders partition=0 group=grp"


def test_append_stream():
    result = CommandBuilder.append_stream(
        topic="events",
        key="order-1",
        version=0,
        event_type="Created",
        schema_version=1,
        producer_id="p1",
        payload='{"a":1}',
    )
    assert "APPEND_STREAM" in result
    assert "topic=events" in result
    assert "key=order-1" in result
    assert 'message={"a":1}' in result


def test_read_stream():
    assert CommandBuilder.read_stream("events", "order-1") == "READ_STREAM topic=events key=order-1"


def test_read_stream_from_version():
    result = CommandBuilder.read_stream("events", "order-1", from_version=5)
    assert result == "READ_STREAM topic=events key=order-1 from_version=5"


def test_save_snapshot():
    result = CommandBuilder.save_snapshot("events", "order-1", 5, '{"state":"x"}')
    assert result == 'SAVE_SNAPSHOT topic=events key=order-1 version=5 message={"state":"x"}'


def test_read_snapshot():
    assert CommandBuilder.read_snapshot("events", "order-1") == (
        "READ_SNAPSHOT topic=events key=order-1"
    )


def test_stream_version():
    assert CommandBuilder.stream_version("events", "order-1") == (
        "STREAM_VERSION topic=events key=order-1"
    )


def test_consume_with_isolation_and_auth():
    result = CommandBuilder.consume(
        "orders",
        0,
        100,
        "m1",
        group="grp",
        generation=3,
        isolation_level="read_committed",
        principal="alice",
        auth_token="secret",
    )
    assert result == (
        "CONSUME topic=orders partition=0 offset=100 member=m1 group=grp generation=3 "
        "isolation_level=read_committed principal=alice auth_token=secret"
    )


def test_list_offsets_commands():
    assert CommandBuilder.list_offsets("orders") == "LIST_OFFSETS topic=orders"
    assert CommandBuilder.list_offsets("orders", 1) == "LIST_OFFSETS topic=orders partition=1"
    assert CommandBuilder.list_offsets("orders", principal="alice", auth_token="secret") == (
        "LIST_OFFSETS topic=orders principal=alice auth_token=secret"
    )


def test_transaction_commands_exact_wire():
    assert CommandBuilder.find_coordinator(transactional_id="tx-1") == (
        "FIND_COORDINATOR transactional_id=tx-1"
    )
    assert CommandBuilder.init_producer_id("tx-1") == "INIT_PRODUCER_ID transactional_id=tx-1"
    assert CommandBuilder.begin_txn("tx-1", "p1", 2) == (
        "BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=2"
    )
    assert CommandBuilder.txn_publish(
        "tx-1",
        "out",
        -1,
        "p1",
        3,
        2,
        "processed",
        key="k1",
        principal="alice",
        auth_token="secret",
    ) == (
        "TXN_PUBLISH transactional_id=tx-1 topic=out partition=-1 producerId=p1 "
        "seqNum=3 epoch=2 key=k1 message=processed principal=alice auth_token=secret"
    )
    assert CommandBuilder.send_offsets_to_txn(
        "tx-1", "p1", 2, "input", "grp", "m1", 4, {2: 202, 0: 101}
    ) == (
        "SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=2 topic=input "
        "group=grp member=m1 generation=4 P0:101,P2:202"
    )
    assert CommandBuilder.end_txn("tx-1", "p1", 2, commit=False) == (
        "END_TXN transactional_id=tx-1 producerId=p1 epoch=2 result=abort"
    )
    assert CommandBuilder.txn_status("tx-1") == "TXN_STATUS transactional_id=tx-1"
