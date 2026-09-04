import pytest

from cursus.admin import (
    AdminClient,
    AdminConfig,
    DeleteTopicOptions,
    TopicCleanupPolicy,
    TopicDefinitionPatch,
    TruncateTopicOptions,
)


class FakeCommandClient:
    def __init__(self, responses: list[str]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, bool]] = []

    def send_any(self, command: str, *, operation: str, retry_ambiguous: bool = True) -> str:
        self.calls.append((command, operation, retry_ambiguous))
        return self.responses.pop(0)


def test_admin_client_builds_complete_patch_and_parses_authoritative_definition() -> None:
    client = AdminClient(AdminConfig())
    fake = FakeCommandClient(
        [
            "OK topic=orders revision=2 lifecycle_epoch=1 partitions=6 "
            "replication_factor=3 idempotent=true event_sourcing=false "
            "cleanup_policy=compact retention_hours=24 retention_bytes=4096 "
            "partitioner=hash_key auth_policy=acl read_acl=reader write_acl=writer"
        ]
    )
    client._client = fake  # type: ignore[assignment]

    result = client.update_topic(
        "orders",
        TopicDefinitionPatch(
            partitions=6,
            replication_factor=3,
            idempotent=True,
            event_sourcing=False,
            cleanup_policy=TopicCleanupPolicy.COMPACT,
            retention_hours=24,
            retention_bytes=4096,
            partitioner="hash_key",
            auth_policy="acl",
            read_acl=["reader"],
            write_acl=["writer"],
        ),
    )

    assert result.revision == 2
    assert result.read_acl == ["reader"]
    assert fake.calls[0][0].startswith("CREATE topic=orders partitions=6")
    assert fake.calls[0][2] is True


def test_destructive_admin_retries_only_with_explicit_idempotency_contract() -> None:
    client = AdminClient(AdminConfig())
    fake = FakeCommandClient(
        [
            "OK topic=orders deleted=true cleanup_pending=false",
            "OK topic=orders truncated=true revision=3 lifecycle_epoch=2 leo=0 hwm=0",
        ]
    )
    client._client = fake  # type: ignore[assignment]

    client.delete_topic("orders", DeleteTopicOptions(if_exists=True))
    client.truncate_topic("orders", TruncateTopicOptions(expected_revision=2))

    assert fake.calls[0][2] is True
    assert fake.calls[1][2] is False


def test_admin_rejects_command_injection_and_partial_credentials() -> None:
    with pytest.raises(ValueError, match="together"):
        AdminClient(AdminConfig(principal="admin"))
    client = AdminClient(AdminConfig())
    with pytest.raises(ValueError, match="invalid topic"):
        client.create_topic("orders injected=true", TopicDefinitionPatch())
