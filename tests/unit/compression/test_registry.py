import pytest

from cursus.compression.registry import CompressionRegistry


def test_registry_has_gzip_by_default():
    registry = CompressionRegistry()
    assert registry.get("gzip") is not None


def test_registry_none_returns_none():
    registry = CompressionRegistry()
    assert registry.get("none") is None


def test_registry_compress_decompress():
    registry = CompressionRegistry()
    original = b"test data"
    compressed = registry.compress(original, "gzip")
    assert registry.decompress(compressed, "gzip") == original


def test_registry_compress_none_passthrough():
    registry = CompressionRegistry()
    data = b"passthrough"
    assert registry.compress(data, "none") is data
    assert registry.decompress(data, "none") is data


def test_registry_unknown_raises():
    registry = CompressionRegistry()
    with pytest.raises(ValueError, match="unknown compression"):
        registry.compress(b"x", "zstd")


def test_registry_register_custom():
    class FakeCompressor:
        @property
        def algorithm_name(self) -> str:
            return "fake"

        def compress(self, data: bytes) -> bytes:
            return b"compressed"

        def decompress(self, data: bytes) -> bytes:
            return b"decompressed"

    registry = CompressionRegistry()
    registry.register(FakeCompressor())
    assert registry.compress(b"x", "fake") == b"compressed"
    assert registry.decompress(b"x", "fake") == b"decompressed"
