# Remaining maintenance cleanup

Inventory source: `3d23a44653cfdfb11e1afba16a7ba66afc70154f` (2026-11-17).
This is a review queue, not evidence that the listed code is redundant.

## Measured position

Tracked Rust source contains 566,172 lines, including comments, blank lines and
tests: 37,477 fewer lines (6.21%) than the 603,649-line baseline. Reproduce with
`git ls-files '*.rs'` and count each tracked file's lines at the source commit.
These totals exclude untracked files and unrelated working-tree changes.

| Area | Rust lines | Next review |
| --- | ---: | --- |
| Server | 208,722 | Protocol fixtures, configuration parsing and lifecycle orchestration |
| Index | 68,264 | Repeated adapter preparation and validation; retain adapter-owned transactions |
| Standalone E2E | 45,547 | Repeated setup and scenario coverage across real-client suites |
| Hub API | 40,854 | Route fixtures and repeated response construction |
| CLI | 36,544 | Remaining orchestration after shared report formatting cleanup |
| SDK | 24,190 | Remaining allocation and dependency costs after shared CDC and reconstruction cleanup |

There is no evidence-backed percentage of cleanup remaining. A further 3–7%
reduction is not an established forecast. File size and test count cannot prove
redundancy, and stronger tests can increase total lines while simplifying production
code.

## Prioritized work

1. **Close qualification gaps.** The latest full contributor checkpoint passed
   8,098 selected tests at `99bde68e`. Required PostgreSQL index qualification
   passed 813 tests at `df0d63fd`; PostgreSQL/MinIO qualification passed all
   157 integration cases and three additional copy-integrity cases at `df0d63fd`.
   See `BENCHMARKS.md` for commands, frozen sources and limits. These checkpoints
   do not qualify the final branch or close the historical hosted SDK memory
   failure. Local matching experiments have not reproduced it. Manual hosted
   qualification at `39c86a9e` has passed the owned PostgreSQL process-cut job and
   all 59 distributed-integrity/mixed-version tests. The contributor job failed
   `cargo deny check` on the `chrono` and `humantime` unmaintained advisories;
   no exception was added. Configured PostgreSQL subsequently passed. Hosted E2E
   failed: the 256 MiB SDK case passed byte identity with a 61,584 KiB streaming
   RSS delta, but the 1 GiB case timed out at 300 seconds before reporting an
   isolated-client measurement. Later workflow steps were skipped. Investigate
   that timeout and resolve the dependency-policy failure without relaxing the
   memory bound or opening a pull request. Local profiling identified repeated
   record-level JSON parsing in SQLite membership triggers. An additive migration
   reduced a local 1 GiB case from 210.20 s to 56.812 s while preserving byte
   identity and the memory bound; hosted requalification is still required.
   Repeat integrated PostgreSQL/object
   storage qualification after changes affecting those paths; preserve concurrent
   upload, GC, source deletion, restart and byte-identity coverage.
2. **Review large protocol fixtures for overlap.** Start with server
   `app/e2e_tests.rs` (11,574 lines), S3 route `tests.rs` (5,994), Hub route
   `tests.rs` (4,854), and standalone `data_integrity.rs` (12,569). For each
   consolidation, record which assertions and failure boundaries survive. Keep
   real-client, wire-level, adapter and concurrency tests when they exercise
   different boundaries. Splitting a file alone is not a reduction in complexity.
   A shared route-registration cleanup for the three server HTTP fixtures is
   integrated: 201 fewer test-fixture lines, with all test bodies unchanged.
   All 238 selected HTTP tests pass; strict server Clippy and formatting checks
   also pass.
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
   settings). A server-library error-message edit at `1b02136b` measured
   medians of 21.733 s versus 7.442 s (65.8% shorter for that narrow workload).
   A container CLI-edit pair at `b6780921` took 146.197 s baseline versus
   135.355 s current (7.4% shorter in one pair; not a proven speedup). Preparation
   image builds were 266.364 s versus 273.602 s with warm registries/base images.
   Cold-machine container builds and broader contributor workloads still need
   measurement before claiming the RFC's 30% contributor critical-path target. Review the sequential 150-minute
   E2E job for reusable compatible artifacts and measured scheduling opportunities;
   retain every required qualification gate.

## Reviewed configuration disposition

Reviewed at `1c2c219d9c5f5265898e9d8fd2e6ec76bfd8c4d0`. The environment
loader is retained after inspecting its numeric parsing, frontend selection,
secret-source loading and TOML-default application. No code change is required
for those reviewed responsibilities:

- `load_non_zero_usize_env` and `load_non_zero_u64_env` share the same short
  structure but preserve different integer widths and field-specific parse/zero
  diagnostics. A generic conversion layer would add machinery to two small
  functions. In-flight limits additionally clamp accepted values; bounded pool
  sizes warn and fall back instead of failing startup. Those policies differ.
- The shared secret loader already owns source conflicts, empty values and size
  checks. File reads additionally enforce bounded reads, file identity and
  permissions; newline handling deliberately differs for fixed-length keys and
  variable-length secrets. Retain those checks and their error ordering.
- Frontend parsing preserves input order, ignores empty comma-separated tokens
  and removes duplicates. The small supported frontend set does not justify an
  extra hash set or a new parser abstraction.
- TOML defaults use quoted dotenv assignments and interpolation before the
  existing environment loader. The environment precedence, admin-token source
  exception and thread-local test environment are observable contracts. Do not
  replace this with raw string assignment or remove quoting as cleanup.

The interpolation implementation and its repeated tests were already simplified
in `d49752c3` and `ff414cd1`. The latest code-equivalent full contributor gate at
`99bde68e` passed its configuration tests with the rest of the 8,018 library
cases. This disposition closes the environment-loader review, not all server
configuration or lifecycle work. Further changes need a concrete redundant path
or unnecessary allocation rather than file size alone.

## Reviewed lifecycle-repair disposition

Reviewed at `418eba45602f02f312481d105ab2ec7577806966`. The repair orchestrator,
action classification, reachability traversal and interruption hooks are retained
after the earlier shared mutation-path and clock-wrapper cleanup. This closes
the lifecycle-repair orchestration review; it does not certify every GC caller.

- Quarantine and webhook actions each select a counter before one shared
  compare-and-delete path. Deletions must remain conditional so a concurrent
  replacement survives. Counters and interruption hooks run only after a
  successful mutation; moving them before deletion would misreport durable work.
- Active retention holds protect future objects as well as existing objects.
  Missing object bytes do not authorize hold removal. Expiry, classification
  precedence, overflow checks and PostgreSQL's database clock remain explicit.
- Local write barriers and PostgreSQL work-pool ownership have different
  lifetimes and cleanup contracts. The local-root entry point also deliberately
  selects Xet compatibility behavior. Combining these entry points would hide
  those differences behind another abstraction.
- Reachability must include latest and version records, referenced protocol
  objects, verified Xorb members and live dedupe-shard mappings. Its 4,096-entry
  verified-source cache and separate per-record attempt set have different
  purposes: reuse successful validation across records, but keep missing or
  failed observations from becoming trusted cache entries. Clearing a full cache
  trades rereads for bounded cache memory; removing it would increase I/O.
- Typed mutation interruption hooks exercise durable boundaries. They compile
  to no-op calls outside tests and remain useful for recovery qualification.

The unchanged implementation is covered by the full contributor checkpoint at
`99bde68e`, including 46 selected lifecycle-repair tests, and the required
PostgreSQL/index and PostgreSQL/MinIO checkpoints at `df0d63fd`. No additional
production abstraction or branch removal was justified by this review.

## Completion criteria

For each accepted code change: retire the predecessor implementation, preserve
observable contracts, run focused tests and required checks, and commit/push the
existing cleanup branch. Periodically freeze a source commit for integrated
qualification. Record production and test changes separately when reporting size.

The cleanup milestone needs reviewed disposition of the queues above, reproducible
build comparisons and passing qualification for the final source. It does not
require reaching an arbitrary line count. New recovery architecture and a future
API-framework migration are separate projects, not prerequisites for this cleanup.
