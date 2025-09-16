import argparse
import sys
from typing import Optional

from .bloom import BloomFilter
from .reservoir import reservoir_sample_file
from .stats import compute_file_stats
from .external_sort import external_sort


def cmd_dedup_approx(args: argparse.Namespace) -> int:
    bf = BloomFilter(expected_items=args.expected, false_positive_rate=args.fpr)
    seen = bf
    written = 0
    with open(args.input, "r", encoding="utf-8", errors="ignore") as r, open(
        args.output, "w", encoding="utf-8", errors="ignore"
    ) as w:
        for line in r:
            # Use bytes to avoid cost of re-encoding repeatedly; but here we can use str
            if not seen.contains_str(line):
                w.write(line)
                seen.add_str(line)
                written += 1
    print(f"Wrote {written} unique lines (approx; FPR~{args.fpr}, k={bf.k_hashes}, m_bits={bf.m_bits})", file=sys.stderr)
    return 0


def cmd_dedup_exact(args: argparse.Namespace) -> int:
    seen: set[str] = set()
    written = 0
    with open(args.input, "r", encoding="utf-8", errors="ignore") as r, open(
        args.output, "w", encoding="utf-8", errors="ignore"
    ) as w:
        for line in r:
            if line not in seen:
                seen.add(line)
                w.write(line)
                written += 1
    print(f"Wrote {written} unique lines (exact). Memory used grows with number of unique lines.", file=sys.stderr)
    return 0


def cmd_sample(args: argparse.Namespace) -> int:
    sample = reservoir_sample_file(args.input, args.k)
    sys.stdout.writelines(sample)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    s = compute_file_stats(args.input, col=args.col)
    if s.count == 0:
        print("No numeric values found for the selected column.", file=sys.stderr)
        return 1
    print("count:", s.count)
    print("mean:", s.mean)
    print("variance:", s.variance)
    print("stddev:", s.stddev)
    print("min:", s.min_val)
    print("max:", s.max_val)
    return 0


def cmd_sort(args: argparse.Namespace) -> int:
    def key_func(line: str):
        if args.numeric:
            # Strip newline and parse float for numeric sort
            try:
                return float(line.strip())
            except ValueError:
                # Non-numeric lines sort last
                return float("inf") if not args.reverse else float("-inf")
        return line

    external_sort(
        args.input,
        args.output,
        key=key_func,
        reverse=args.reverse,
        max_lines_in_memory=args.chunk_lines,
        tmp_dir=args.tmpdir,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="file-manipulator", description="File utilities using DS&A for large-scale data.")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("dedup-approx", help="Approximate deduplication using a Bloom filter (low memory, small FPR).")
    sp.add_argument("input", help="Input text file")
    sp.add_argument("output", help="Output file for unique lines")
    sp.add_argument("--fpr", type=float, default=0.001, help="Target false positive rate (default: 0.001)")
    sp.add_argument("--expected", type=int, required=True, help="Expected number of unique lines")
    sp.set_defaults(func=cmd_dedup_approx)

    sp = sub.add_parser("dedup-exact", help="Exact deduplication using a hash set (higher memory).")
    sp.add_argument("input", help="Input text file")
    sp.add_argument("output", help="Output file for unique lines")
    sp.set_defaults(func=cmd_dedup_exact)

    sp = sub.add_parser("sample", help="Reservoir sample k lines uniformly at random.")
    sp.add_argument("input", help="Input text file")
    sp.add_argument("k", type=int, help="Sample size")
    sp.set_defaults(func=cmd_sample)

    sp = sub.add_parser("stats", help="Streaming stats (mean/variance/stddev) of numeric column using Welford.")
    sp.add_argument("input", help="Input CSV or whitespace-delimited text file")
    sp.add_argument("--col", type=int, default=0, help="0-based column index to analyze (default: 0)")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("sort", help="External sort large file by chunking + k-way heap merge.")
    sp.add_argument("input", help="Input text file")
    sp.add_argument("output", help="Output sorted file")
    sp.add_argument("--reverse", action="store_true", help="Reverse sort order")
    sp.add_argument("--numeric", action="store_true", help="Sort numerically (parse float)")
    sp.add_argument("--chunk-lines", type=int, default=500_000, help="Lines per in-memory chunk (default: 500k)")
    sp.add_argument("--tmpdir", default=None, help="Optional temp directory for sort chunks")
    sp.set_defaults(func=cmd_sort)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())