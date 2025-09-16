from typing import Iterable, Iterator


def iter_lines(path: str) -> Iterator[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line