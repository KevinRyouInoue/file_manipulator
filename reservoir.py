import random
from collections import deque
from typing import Iterable, Iterator


def reservoir_sample(iterable: Iterable[str], k: int, rng: random.Random | None = None) -> list[str]:
    """
    Uniform random sample of size k from an iterable of unknown/large size.
    Algorithm R (Vitter): O(n) time, O(k) memory.
    """
    if k <= 0:
        return []
    r = rng or random
    reservoir: list[str] = []
    for i, item in enumerate(iterable):
        if i < k:
            reservoir.append(item)
        else:
            j = r.randint(0, i)
            if j < k:
                reservoir[j] = item
    return reservoir


def reservoir_sample_file(path: str, k: int, rng: random.Random | None = None) -> list[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return reservoir_sample(f, k, rng=rng)