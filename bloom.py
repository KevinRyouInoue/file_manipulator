import math
import hashlib
from typing import Iterable


class BloomFilter:
    """
    Space-efficient probabilistic set with false positives (no false negatives).
    Uses double hashing: h(i) = (h1 + i*h2) % m

    m (bits) and k (hashes) chosen from expected n and false positive rate p:
      m = - (n * ln p) / (ln 2)^2
      k = (m / n) * ln 2
    """

    def __init__(self, expected_items: int, false_positive_rate: float = 0.001):
        if expected_items <= 0:
            raise ValueError("expected_items must be > 0")
        if not (0 < false_positive_rate < 1):
            raise ValueError("false_positive_rate must be in (0,1)")
        self.m_bits = max(8, int(-expected_items * math.log(false_positive_rate) / (math.log(2) ** 2)))
        self.k_hashes = max(1, int(round((self.m_bits / expected_items) * math.log(2))))
        self._bytes = bytearray((self.m_bits + 7) // 8)

    def _hashes(self, item: bytes):
        # Use blake2b to derive two 64-bit hashes; supports bytes
        h = hashlib.blake2b(item, digest_size=16).digest()
        h1 = int.from_bytes(h[:8], "big")
        h2 = int.from_bytes(h[8:], "big")
        if h2 == 0:
            h2 = 0x9e3779b97f4a7c15  # golden ratio constant
        for i in range(self.k_hashes):
            yield (h1 + i * h2) % self.m_bits

    def _set_bit(self, idx: int):
        b = idx // 8
        bit = idx % 8
        self._bytes[b] |= (1 << bit)

    def _get_bit(self, idx: int) -> bool:
        b = idx // 8
        bit = idx % 8
        return (self._bytes[b] >> bit) & 1 == 1

    def add(self, item: bytes):
        for idx in self._hashes(item):
            self._set_bit(idx)

    def __contains__(self, item: bytes) -> bool:
        return all(self._get_bit(idx) for idx in self._hashes(item))

    def add_str(self, s: str, encoding: str = "utf-8"):
        self.add(s.encode(encoding))

    def contains_str(self, s: str, encoding: str = "utf-8") -> bool:
        return self.__contains__(s.encode(encoding))

    @classmethod
    def from_iterable(cls, items: Iterable[bytes], expected_items: int, false_positive_rate: float = 0.001):
        bf = cls(expected_items, false_positive_rate)
        for it in items:
            bf.add(it)
        return bf