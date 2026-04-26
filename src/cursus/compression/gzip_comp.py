import gzip


class GzipCompressor:
    @property
    def algorithm_name(self) -> str:
        return "gzip"

    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)
