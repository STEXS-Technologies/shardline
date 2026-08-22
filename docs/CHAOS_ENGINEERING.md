# Chaos Engineering and Reliability Evidence

Shardline treats chaos testing as evidence, not proof. The trust strategy combines:

```text
validated types + property tests + fuzzing + Loom + deterministic chaos
                + deployment fault injection + independent invariants
```

Every failure report must distinguish an attempted operation from an acknowledged one.
An acknowledged write must remain byte-exact. An unacknowledged write may be absent or may
have committed completely, but it must never become partially visible.

## Replay contract

`crates/shardline-server/tests/chaos_runner.rs` owns the deterministic in-process
orchestrator. All workload and injection choices use one `SplitMix64` stream. Set
`SHARDLINE_CHAOS_SEED` to replay a schedule; the runner prints the seed and operation
transcript on failure. `SHARDLINE_CHAOS_SCALE=1` is a smaller local schedule and is not
the CI default.

The runner's acknowledged-write ledger is independent of server metadata. It verifies:

- every acknowledged live object reconstructs to an acknowledged coherent version
- content hashes and lengths match the returned bytes
- listings and point reads agree
- interrupted writes do not expose torn bytes
- restart and cache loss do not change logical state
- destructive GC reaches a fixed point without deleting reachable data

Every discovered seed must become either a checked-in regression test or a permanently
scheduled replay seed.

## Fault coverage audit

Status meanings:

- **Covered**: a deterministic or deployment test injects the fault and checks durable invariants.
- **Partial**: related evidence exists, but an important ambiguity or provider class is missing.
- **Open**: no representative automated injection exists yet. Documentation is not evidence.

| Layer | Status | Current evidence | Remaining trust work |
| --- | --- | --- | --- |
| Process | Covered | `chaos_runner`, `fault_drills`, `fault_drills_extreme` kill in-flight servers and run repeated restart cycles | Extend typed failpoints beyond local object publication to metadata commit/index publication boundaries |
| Filesystem | Covered | Anchored-path race tests, atomic temp publication, parent-swap fuzzing, read-only/permission failures and durable file/directory `fsync`; typed test-only I/O faults inject `ENOSPC`, `EIO`, prefix-only writes and sync failure at seven explicit publication boundaries, while proptests and `shardline_local_publication_faults` require complete-old or complete-new visibility | Kernel/FUSE fault mounts remain useful supplemental platform evidence, not the only regression oracle |
| Object storage | Covered | `deployment_chaos` kills and partitions MinIO during writes; a typed one-shot HTTP proxy proves exact-byte behavior for accepted PUT response loss, a retryable provider `503`, delayed successful range bodies and a stale-but-well-formed xorb response against real MinIO; the stale response is driven through the Xet HTTP frontend and rejected by end-to-end CAS validation before transfer bytes are exposed; multipart/integrity tests and OCI tombstone DELETE replay cover the other normal and ambiguous paths | Extend the provider/version matrix as external deployments contribute evidence |
| Database | Partial | Postgres kill/restart, mid-transaction loss, stale fencing, serialization/deadlock behavior and atomic migrations are exercised; a protocol-aware TCP proxy drops `CommandComplete(COMMIT)` only after PostgreSQL commits and proves idempotent recovery; lock-induced statement timeout and bounded pool exhaustion both prove recovery after the transient condition clears | Add a real primary/standby promotion campaign |
| Network | Partial | API/transfer and MinIO/Postgres partitions, connection stalls and reconnect recovery | Add packet duplication/reordering and asymmetric long-lived cluster partitions |
| Cluster | Covered | `multinode_chaos` kills either role, partitions split roles and replaces roles during traffic | True N-1/N mixed-binary tests remain upgrade evidence, not basic cluster-failure evidence |
| Concurrency | Covered | Upload/overwrite/delete/GC/reconstruction/cache/provider/OCI races plus Loom models | Extend models whenever a new shared mutable state machine is introduced |
| Resource pressure | Partial | Request/body/list bounds, admission and concurrency stress exist; an isolated `RLIMIT_NOFILE` regression exhausts descriptors, requires atomic local-publication failure, releases pressure and verifies an exact-byte retry; deterministic execution-pool saturation rejects without queueing and successfully admits the same valid upload after capacity returns | Inject memory pressure and Tokio runtime-worker starvation below the application execution pools |
| Time | Partial | Retention boundaries, expiry and corrupted/future timestamps are tested | Inject wall-clock jumps and multi-node skew around leases, tokens and retention |
| Data corruption | Covered | Corrupt chunks, hashes, ranges, metadata, cache entries and protocol inputs are rejected or repaired; Xet range transfers validate the complete addressed xorb before exposing bytes; fsck is an independent oracle | Add sampled corruption campaigns over restored production-scale inventories |
| Upgrade | Partial | Same-binary role replacement, migration up/down checks and documented rolling procedure | Run actual N-1 and N binaries together, test rollback limits, and kill during every migration boundary |
| Operator actions | Partial | Repeated restart, destructive restore rehearsal, repair/fsck/rebuild dry runs and malformed configuration tests; the full rebuild → lifecycle repair → fsck operator sequence is rerun against the same durable root and required to converge identically | Add interrupted repair and migration resume campaigns |
| Security | Partial | Cross-tenant route matrices, capability binding, provider revocation/visibility and webhook replay tests | Race authorization/revocation against every active write/read frontend and measure existence/timing leakage |
| Long soak | Partial | A weekly two-hour deterministic chaos campaign archives every seed transcript, elapsed time, peak RSS, sampled process-tree FD/task counts and invariant result; a separate scheduled job archives the real Postgres/MinIO/Redis kill-and-partition campaign; deterministic FD and application execution-pool exhaustion run in normal test suites | Add scheduled memory-pressure and runtime-worker-starvation campaigns |

“Partial” and “Open” rows are release-plan work. They must not be described as proven or
silently relabeled Stable based only on line coverage.

## Required failure-boundary model

`LocalPublishBoundary` and `LocalPublishFault` are the first explicit typed fault vocabulary
shared by production code and deterministic tests. They cover local write, file-sync,
installation and directory-sync transitions without string matching. The target is to extend
the same pattern through the complete write path so it distinguishes:

```text
validated
chunked
temporary object durable
immutable object installed
metadata committed
index published
visible
```

For each boundary, deterministic injection must demonstrate exactly one valid outcome:

1. the operation completed and all acknowledged bytes are reconstructable, or
2. the operation failed and recovery/fsck can return the system to a valid state.

External sleeps or lucky scheduler timing are not a substitute for these named failpoints.
Failpoint code must be disabled in production builds and must emit the seed, operation ID,
boundary, node and fencing epoch needed for replay.

## Commands

Run the fast deterministic schedule:

```bash
SHARDLINE_CHAOS_SCALE=1 SHARDLINE_CHAOS_SEED=2436552524 \
  cargo test -p shardline-server --test chaos_runner -- --nocapture
```

Run Loom models:

```bash
cargo test -p shardline-loom-tests
```

Run the bounded fuzz campaign configured by the repository:

```bash
scripts/shardline/fuzz.sh
```

Deployment drills require the Docker fault stack described by the test and CI workflow:

```bash
cargo test -p shardline-server --test deployment_chaos -- --nocapture
```

Run a bounded local soak (the scheduled workflow uses 7200 seconds):

```bash
SHARDLINE_SOAK_DURATION_SECONDS=600 \
SHARDLINE_SOAK_INITIAL_SEED=2436552524 \
  scripts/reliability-soak.sh reliability-soak-results
```

## Evidence accounting

Published reliability counters must come from archived CI/soak reports, not estimates.
Useful counters include injected failures by class, crash/restart cycles, invariant
evaluations, replayed regression seeds, maximum recovery time and any unrecoverable state.
The report must include the commit, binary versions, seed set, topology and fault-provider
versions so another operator can reproduce it.
