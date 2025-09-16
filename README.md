# file_manipulator (Python)

Stream-friendly file utilities that demonstrate real-world Data Structures & Algorithms (DS&A):

- Approximate deduplication using a Bloom filter (probabilistic set): O(1) add/check, tiny memory, low false positive rate.
- Reservoir sampling: uniform random sample of size k from very large files in one pass.
- Streaming statistics with Welford's algorithm: mean/variance/stddev in one pass without loading entire file.
- External sort with k-way heap merge: sort files larger than memory.

## Install (local)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

## CLI usage
```bash
# 1) Approximate deduplication (Bloom filter)
file-manipulator dedup-approx input.txt output.txt --fpr 0.001 --expected 2000000

# 2) Exact deduplication (uses a Python set; memory heavy for huge files)
file-manipulator dedup-exact input.txt output.txt

# 3) Reservoir sampling k lines uniformly at random
file-manipulator sample input.txt 1000 > sample.txt

# 4) Streaming stats (mean/variance/stddev) of a numeric column (0-based)
# autodetects CSV vs whitespace; use --col for column index
file-manipulator stats data.csv --col 2

# 5) External sort (chunked + k-way heap merge)
file-manipulator sort big.txt sorted.txt --reverse
```

## DS&A highlights
- Bloom filter: bytearray bitset + double hashing to keep memory O(m) with false positive rate p.
- Reservoir sampling: single-pass O(n) with O(k) memory.
- Welford’s algorithm: numerically stable one-pass mean/variance.
- External sort: chunk sort + heap-based k-way merge O(n log k) where k = number of chunks.

## Notes
- Bloom-filter dedup can drop a very small number of unique lines (false positives). Use `dedup-exact` when exactness is required and memory allows.