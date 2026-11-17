# Remaining maintenance cleanup

Inventory source: `99bde68ea3eadf0f37c37b3ff9b1029da109f25d` (2026-11-17).
This is a review queue, not evidence that the listed code is redundant.

## Measured position

Tracked Rust source contains 566,249 lines, including comments, blank lines and
tests: 37,400 fewer lines (6.20%) than the 603,649-line baseline. Reproduce with
`git ls-files '*.rs'` and count each tracked file's lines at the source commit.
These totals exclude untracked files and unrelated working-tree changes.

| Area | Rust lines | Next review |
| --- | ---: | --- |
| Server | 208,722 | Protocol fixtures, configuration parsing and lifecycle orchestration |
| Index | 68,264 | Repeated adapter preparation and validation; retain adapter-owned transactions |
| Standalone E2E | 45,590 | Repeated setup and scenario coverage across real-client suites |
| Hub API | 40,854 | Route fixtures and repeated response construction |
| CLI | 36,578 | Remaining orchestration after shared report formatting cleanup |
| SDK | 24,190 | Remaining allocation and dependency costs after shared CDC and reconstruction cleanup |

There is no evidence-backed percentage of cleanup remaining. A further 3–7%
reduction is not an established forecast. File size and test count cannot prove
redundancy, and stronger tests can increase total lines while simplifying production
code.

## Prioritized work

1. **Close qualification gaps.** The latest full contributor checkpoint passed
   8,098 selected tests at `99bde68e`. Required PostgreSQL index qualification
   passed 813 tests at `e5d4f3af`; PostgreSQL/MinIO qualification passed all
   157 integration cases and three additional copy-integrity cases at `b080738d`.
   See `BENCHMARKS.md` for commands, frozen sources and limits. These checkpoints
   do not qualify the final branch or close the historical hosted SDK memory
   failure. Local matching experiments have not reproduced it. Obtain current hosted evidence without relaxing the
   memory bound or opening a pull request. Repeat integrated PostgreSQL/object
   storage qualification after changes affecting those paths; preserve concurrent
   upload, GC, source deletion, restart and byte-identity coverage.
2. **Review large protocol fixtures for overlap.** Start with server
   `app/e2e_tests.rs` (11,574 lines), S3 route `tests.rs` (5,994), Hub route
   `tests.rs` (4,854), and standalone `data_integrity.rs` (12,569). For each
   consolidation, record which assertions and failure boundaries survive. Keep
   real-client, wire-level, adapter and concurrency tests when they exercise
   different boundaries. Splitting a file alone is not a reduction in complexity.
   A shared route-registration patch for the three server HTTP fixtures was
   verified separately: 201 fewer test-fixture lines, all test bodies unchanged,
   238 selected HTTP tests passing, and strict server Clippy passing. Integration
   is deferred because `app/e2e_tests.rs` contains unrelated uncommitted work;
   that file was left byte-for-byte unchanged in the main workspace. The patch
   is preserved outside the repository at
   `.shardline-cleanup-state/http-shared-routes-deferred.patch` (parent projects
   directory). It is not part of the branch's reduction or qualification claims.
3. **Simplify metadata adapters locally.** SQLite and PostgreSQL `index_store.rs`
   contain 4,597 and 4,801 lines respectively. Compare repeated preparation and
   decoding with existing shared helpers before introducing abstractions. Preserve
   snapshot semantics, error propagation, locking and atomic publication. Similar
   SQL operations across engines are not automatically interchangeable. Shared
   SQLite row decoding and batch verification, plus PostgreSQL listing, lookup
   and create-retry decoding, are already consolidated; review other operations
   rather than reopening those completed changes. Shared SQLite appended-evidence
   persistence now also owns the baseline-versus-head rule for lifecycle and OCI
   tag mutations; quarantine empty-log validation and OCI object database-clock
   behavior remain explicit.
4. **Finish measured build-cost work.** Ordinary build targets already avoid
   unconditional clean, Nextest is pinned without forced reinstall, and SDK tests
   no longer require the server graph. A controlled empty-target release-build
   pair measured 243.923 s at the baseline versus 234.337 s at `b367232d`
   (3.93% shorter in that sample). This is not a statistically established
   improvement or a cold-machine/container measurement. Three release-mode CLI
   edit samples per revision measured medians of 129.033 s baseline versus
   127.761 s at `d49752c3` (0.99% shorter; not a proven speedup). A normal-development-profile CLI edit comparison at `5433f032` measured
   medians of 7.811 s baseline versus 3.735 s current (52.2% shorter for that
   narrow workload, with full debug information and checked-in incremental
   settings). Server-library edits and cold/edited-Rust container builds still
   need measurement before claiming the RFC's 30% contributor critical-path target. Review the sequential 150-minute
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
