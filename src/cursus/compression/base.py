from typing import Protocol, runtime_checkable


@runtime_checkable
class CursusCompressor(Protocol):
    @property
    def algorithm_name(self) -> str: ...

    def compress(self, data: bytes) -> bytes: ...

    def decompress(self, data: bytes) -> bytes: ...
