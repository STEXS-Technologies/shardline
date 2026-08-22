# Benchmark evidence

This directory contains selected, reproducible benchmark snapshots. Each snapshot
names the exact source commit, UTC measurement time, deployment target, fixture,
host, and Rust toolchain. The scheduled `Benchmarks` workflow retains the complete
machine-readable JSON and `/usr/bin/time -v` reports for every run.

The numbers are engineering evidence for comparing Shardline revisions on equivalent
hardware. They are not product claims across different machines, object stores, or
network topologies. In particular, `isolated-local` exercises the real chunking,
indexing, reconstruction, deduplication, and cache paths with local adapters; it does
not measure Postgres, S3, Redis, or network overhead.

Available snapshots:

- [2026-08-22 local concurrency smoke](2026-08-22-local-concurrency-smoke.md)

Reproduce the default matrix from a clean checkout with:

```bash
scripts/benchmark-matrix.sh /tmp/shardline-benchmark-results
```
