# Remaining maintenance cleanup

Inventory source: `89d8801f5a9a778ca9b558ab670cced2346686c3` (2026-11-17).
This is a review queue, not evidence that the listed code is redundant.

## Measured position

Tracked Rust source contains 566,513 lines, including comments, blank lines and
tests: 37,136 fewer lines (6.15%) than the 603,649-line baseline. Reproduce with
`git ls-files '*.rs'` and count each tracked file's lines at the source commit.
These totals exclude untracked files and unrelated working-tree changes.

| Area | Rust lines | Next review |
| --- | ---: | --- |
| Server | 208,722 | Protocol fixtures, configuration parsing and lifecycle orchestration |
| Index | 68,304 | Repeated adapter preparation and validation; retain adapter-owned transactions |
| Standalone E2E | 45,790 | Repeated setup and scenario coverage across real-client suites |
| Hub API | 40,883 | Route fixtures and repeated response construction |
| CLI | 36,578 | Remaining orchestration after shared report formatting cleanup |
| SDK | 24,185 | Remaining allocation and dependency costs after shared CDC and reconstruction cleanup |

There is no evidence-backed percentage of cleanup remaining. A further 3–7%
reduction is not an established forecast. File size and test count cannot prove
redundancy, and stronger tests can increase total lines while simplifying production
code.

## Prioritized work

1. **Close qualification gaps.** The latest full contributor checkpoint passed
   8,102 selected tests at `89d8801f`. Required PostgreSQL index qualification
   passed 813 tests at `4a037718`; PostgreSQL/MinIO qualification passed all
   157 integration cases and three additional copy-integrity cases at `711de39c`.
   See `BENCHMARKS.md` for commands, frozen sources and limits. These checkpoints
   do not qualify the final branch or close the historical hosted SDK memory
   failure. Local matching experiments have not reproduced it. Obtain current hosted evidence without relaxing the
   memory bound or opening a pull request. Repeat integrated PostgreSQL/object
   storage qualification after changes affecting those paths; preserve concurrent
   upload, GC, source deletion, restart and byte-identity coverage.
2. **Review large protocol fixtures for overlap.** Start with server
   `app/e2e_tests.rs` (11,574 lines), S3 route `tests.rs` (5,994), Hub route
   `tests.rs` (4,883), and standalone `data_integrity.rs` (12,769). For each
   consolidation, record which assertions and failure boundaries survive. Keep
   real-client, wire-level, adapter and concurrency tests when they exercise
   different boundaries. Splitting a file alone is not a reduction in complexity.
3. **Simplify metadata adapters locally.** SQLite and PostgreSQL `index_store.rs`
   contain 4,645 and 4,801 lines respectively. Compare repeated preparation and
   decoding with existing shared helpers before introducing abstractions. Preserve
   snapshot semantics, error propagation, locking and atomic publication. Similar
   SQL operations across engines are not automatically interchangeable. Shared
   SQLite row decoding and batch verification, plus PostgreSQL listing, lookup
   and create-retry decoding, are already consolidated; review other operations
   rather than reopening those completed changes.
4. **Finish measured build-cost work.** Ordinary build targets already avoid
   unconditional clean, Nextest is pinned without forced reinstall, and SDK tests
   no longer require the server graph. A controlled empty-target release-build
   pair measured 243.923 s at the baseline versus 234.337 s at `b367232d`
   (3.93% shorter in that sample). This is not a statistically established
   improvement or a cold-machine/container measurement. Three release-mode CLI
   edit samples per revision measured medians of 129.033 s baseline versus
   127.761 s at `d49752c3` (0.99% shorter; not a proven speedup). Development-profile
   iteration and container builds still need measurement before claiming the
   RFC's 30% contributor critical-path target. Review the sequential 150-minute
   E2E job for reusable compatible artifacts and measured scheduling opportunities;
   retain every required qualification gate.
5. **Review configuration and lifecycle branching.** Server `config/env.rs`
   contains 3,125 lines. Identify repeated conversion/validation and unnecessary
   intermediate state using existing owners. Preserve precedence, defaults,
   diagnostics, cancellation, admission and durability boundaries. Only remove
   branches after showing why supported behavior cannot require them.

## Completion criteria

For each accepted code change: retire the predecessor implementation, preserve
observable contracts, run focused tests and required checks, and commit/push the
existing cleanup branch. Periodically freeze a source commit for integrated
qualification. Record production and test changes separately when reporting size.

The cleanup milestone needs reviewed disposition of the queues above, reproducible
build comparisons and passing qualification for the final source. It does not
require reaching an arbitrary line count. New recovery architecture and a future
API-framework migration are separate projects, not prerequisites for this cleanup.
