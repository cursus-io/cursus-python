from cursus.compression.gzip_comp import GzipCompressor


def test_gzip_roundtrip():
    comp = GzipCompressor()
    original = b"hello world " * 100
    compressed = comp.compress(original)
    assert compressed != original
    assert comp.decompress(compressed) == original


def test_gzip_algorithm_name():
    assert GzipCompressor().algorithm_name == "gzip"


def test_gzip_empty_data():
    comp = GzipCompressor()
    compressed = comp.compress(b"")
    assert comp.decompress(compressed) == b""
