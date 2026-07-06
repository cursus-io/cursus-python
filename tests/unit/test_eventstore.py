import pytest

from cursus.eventstore import EventStore


def test_parse_append_response_strict_contract():
    es = EventStore("localhost:9000", "orders", "producer-1")

    result = es._parse_append_response("OK version=3 offset=42 partition=1")

    assert result.version == 3
    assert result.offset == 42
    assert result.partition == 1


@pytest.mark.parametrize("resp", ["OK version=3", "3", "", "ERROR: version_conflict"])
def test_parse_append_response_rejects_non_contract_responses(resp):
    es = EventStore("localhost:9000", "orders", "producer-1")

    with pytest.raises(ValueError):
        es._parse_append_response(resp)
