import csv
import math
from dataclasses import dataclass


@dataclass
class OnlineStats:
    """Welford's online algorithm for streaming mean/variance."""
    count: int = 0
    mean: float = 0.0
    M2: float = 0.0
    min_val: float | None = None
    max_val: float | None = None

    def update(self, x: float):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta2 * delta2
        if self.min_val is None or x < self.min_val:
            self.min_val = x
        if self.max_val is None or x > self.max_val:
            self.max_val = x

    @property
    def variance(self) -> float | None:
        if self.count < 2:
            return None
        return self.M2 / (self.count - 1)

    @property
    def stddev(self) -> float | None:
        v = self.variance
        return math.sqrt(v) if v is not None else None


def iter_numeric_column(file_path: str, col: int = 0):
    """
    Yields numeric values from column `col` (0-based).
    Autodetects CSV vs whitespace-delimited.
    Ignores non-parsable rows.
    """
    # Peek first line to detect delimiter
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        first = f.readline()
        if "," in first:
            # CSV mode
            f.seek(0)
            reader = csv.reader(f)
            for row in reader:
                if col < 0 or col >= len(row):
                    continue
                try:
                    yield float(row[col])
                except (ValueError, TypeError):
                    continue
        else:
            # Whitespace mode
            if first:
                parts = first.split()
                if len(parts) > col:
                    try:
                        yield float(parts[col])
                    except (ValueError, TypeError):
                        pass
            for line in f:
                parts = line.split()
                if len(parts) <= col:
                    continue
                try:
                    yield float(parts[col])
                except (ValueError, TypeError):
                    continue


def compute_file_stats(file_path: str, col: int = 0) -> OnlineStats:
    s = OnlineStats()
    for val in iter_numeric_column(file_path, col):
        s.update(val)
    return s