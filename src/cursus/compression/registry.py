from cursus.compression.base import CursusCompressor
from cursus.compression.gzip_comp import GzipCompressor


class CompressionRegistry:
    def __init__(self) -> None:
        self._compressors: dict[str, CursusCompressor] = {}
        self.register(GzipCompressor())

    def register(self, compressor: CursusCompressor) -> None:
        self._compressors[compressor.algorithm_name] = compressor

    def get(self, name: str) -> CursusCompressor | None:
        if name == "none" or name == "":
            return None
        return self._compressors.get(name)

    def compress(self, data: bytes, algorithm: str) -> bytes:
        if algorithm in ("none", ""):
            return data
        comp = self._compressors.get(algorithm)
        if comp is None:
            raise ValueError(f"unknown compression algorithm: {algorithm}")
        return comp.compress(data)

    def decompress(self, data: bytes, algorithm: str) -> bytes:
        if algorithm in ("none", ""):
            return data
        comp = self._compressors.get(algorithm)
        if comp is None:
            raise ValueError(f"unknown compression algorithm: {algorithm}")
        return comp.decompress(data)
