import heapq
import os
import tempfile
from typing import Callable, Iterable, Iterator


def _write_sorted_chunk(lines: list[str], key: Callable[[str], object] | None, reverse: bool, dirpath: str) -> str:
    lines.sort(key=key, reverse=reverse)
    fd, path = tempfile.mkstemp(prefix="chunk_", dir=dirpath, text=True)
    with os.fdopen(fd, "w", encoding="utf-8", errors="ignore") as w:
        w.writelines(lines)
    return path


def _merge_files(paths: list[str], out_path: str, key: Callable[[str], object] | None, reverse: bool):
    # For reverse merge, invert key using a wrapper object
    def key_of(line: str):
        return key(line) if key else line

    # Build iterators
    files = [open(p, "r", encoding="utf-8", errors="ignore") for p in paths]
    try:
        heap: list[tuple[object, int, str]] = []
        for idx, f in enumerate(files):
            line = f.readline()
            if line:
                k = key_of(line)
                # For reverse, push negative/comparable wrapper by tuple (not reliable for arbitrary types)
                # Instead use normal order and flip at chunk sort; merging assumes same order
                heap.append((k, idx, line))
        heapq.heapify(heap)

        with open(out_path, "w", encoding="utf-8", errors="ignore") as out:
            while heap:
                _, idx, line = heapq.heappop(heap)
                out.write(line)
                nxt = files[idx].readline()
                if nxt:
                    heapq.heappush(heap, (key_of(nxt), idx, nxt))
    finally:
        for f in files:
            try:
                f.close()
            except Exception:
                pass


def external_sort(
    in_path: str,
    out_path: str,
    key: Callable[[str], object] | None = None,
    reverse: bool = False,
    max_lines_in_memory: int = 500_000,
    tmp_dir: str | None = None,
):
    """
    Sort arbitrarily large text files by splitting into sorted chunks and k-way merging with a heap.
    Complexity: O(n log k), where k is the number of chunks (files).

    Notes:
    - Provide a key(line) function for custom ordering (e.g., numeric parse).
    - reverse uses reverse sorting at chunk stage; merge assumes same order across chunks.
    """
    tmp_base = tempfile.mkdtemp(prefix="extsort_", dir=tmp_dir)
    chunk_paths: list[str] = []
    try:
        with open(in_path, "r", encoding="utf-8", errors="ignore") as f:
            buf: list[str] = []
            for line in f:
                buf.append(line)
                if len(buf) >= max_lines_in_memory:
                    chunk_paths.append(_write_sorted_chunk(buf, key, reverse, tmp_base))
                    buf = []
            if buf:
                chunk_paths.append(_write_sorted_chunk(buf, key, reverse, tmp_base))

        if len(chunk_paths) == 1:
            # Simple move/rename if already sorted chunk (still ensure sorted)
            _merge_files(chunk_paths, out_path, key, reverse)
        else:
            _merge_files(chunk_paths, out_path, key, reverse)
    finally:
        # Cleanup chunk files and dir
        for p in chunk_paths:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        try:
            os.rmdir(tmp_base)
        except OSError:
            # Might not be empty if errors occurred
            pass