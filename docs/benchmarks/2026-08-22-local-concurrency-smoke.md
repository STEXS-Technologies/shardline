# Local concurrency smoke — 2026-08-22

- source: `dea62384d0d9`
- measured at: `20260822T154041Z`
- target: `isolated-local`
- host: Linux 7.1.3 x86_64, glibc 2.42, 32 logical CPUs
- toolchain: `rustc 1.97.1 (8bab26f4f 2026-07-14)`
- fixture: 1,048,576 bytes, 65,536 mutated bytes, 65,536-byte chunks, 3 iterations

| clients | initial upload MiB/s | sparse upload MiB/s | latest read MiB/s | concurrent read MiB/s | concurrent upload MiB/s | upload reuse | cache cold µs | cache hot µs | CPU cores |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 285.2 | 347.1 | 631.6 | 504.4 | 385.7 | 87.5% | 335 | 7 | 1.12 |
| 32 | 289.8 | 323.6 | 553.6 | 931.8 | 1445.0 | 87.5% | 346 | 8 | 5.60 |
| 128 | 340.3 | 369.2 | 649.4 | 650.1 | 1455.4 | 87.5% | 360 | 8 | 6.69 |

The constant 87.5% concurrent-upload reuse is a fixture property: each worker writes
one distinct chunk and reuses seven existing chunks. The 32-to-128 client result is
already CPU-saturated enough that read throughput falls rather than scaling linearly;
that is useful regression context, not a horizontal deployment claim.

Command:

```bash
scripts/benchmark-matrix.sh /tmp/shardline-published-benchmark
```
