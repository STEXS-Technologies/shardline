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
| Filesystem | Partial | Anchored-path race tests, atomic temp publication, parent-swap fuzzing, read-only/permission failures, durable file and directory `fsync`, and typed failpoints before temp write/after temp durability/after install/after directory durability | Inject ENOSPC, EIO, partial write and real `fsync` failure with a faulting filesystem in CI |
| Object storage | Partial | `deployment_chaos` kills and partitions MinIO during writes; S3 retry, multipart and integrity tests cover normal failures; a one-shot HTTP fault proxy lets MinIO commit a conditional PUT and then drops the successful response, proving retry and exact-byte idempotence against a real provider; OCI tombstone tests replay the equivalent DELETE ambiguity | Inject slow response bodies, stale responses and provider-specific 5xx behavior |
| Database | Partial | Postgres kill/restart, mid-transaction loss, stale fencing, serialization/deadlock behavior and atomic migrations are exercised; request errors now preserve durable upload intents for retry and reconciliation instead of declaring ambiguous outcomes terminal | Add primary failover, response loss after COMMIT, statement timeout and pool-exhaustion campaigns |
| Network | Partial | API/transfer and MinIO/Postgres partitions, connection stalls and reconnect recovery | Add packet duplication/reordering and asymmetric long-lived cluster partitions |
| Cluster | Covered | `multinode_chaos` kills either role, partitions split roles and replaces roles during traffic | True N-1/N mixed-binary tests remain upgrade evidence, not basic cluster-failure evidence |
| Concurrency | Covered | Upload/overwrite/delete/GC/reconstruction/cache/provider/OCI races plus Loom models | Extend models whenever a new shared mutable state machine is introduced |
| Resource pressure | Partial | Request/body/list bounds, admission and concurrency stress exist | Inject memory pressure, file-descriptor exhaustion and executor/task starvation |
| Time | Partial | Retention boundaries, expiry and corrupted/future timestamps are tested | Inject wall-clock jumps and multi-node skew around leases, tokens and retention |
| Data corruption | Covered | Corrupt chunks, hashes, ranges, metadata, cache entries and protocol inputs are rejected or repaired; fsck is an independent oracle | Add sampled corruption campaigns over restored production-scale inventories |
| Upgrade | Partial | Same-binary role replacement, migration up/down checks and documented rolling procedure | Run actual N-1 and N binaries together, test rollback limits, and kill during every migration boundary |
| Operator actions | Partial | Repeated restart, destructive restore rehearsal, repair/fsck/rebuild dry runs and malformed configuration tests | Add interrupted repair/migration resume campaigns and operator-command idempotency transcripts |
| Security | Partial | Cross-tenant route matrices, capability binding, provider revocation/visibility and webhook replay tests | Race authorization/revocation against every active write/read frontend and measure existence/timing leakage |
| Long soak | Partial | A weekly two-hour deterministic chaos campaign archives every seed transcript, elapsed time, peak RSS, sampled process-tree FD/task counts and invariant result; first-to-last peaks expose cross-seed growth | Extend the scheduled campaign to deployment partitions and explicit resource pressure |

“Partial” and “Open” rows are release-plan work. They must not be described as proven or
silently relabeled Stable based only on line coverage.

## Required failure-boundary model

`LocalPublishBoundary` is the first explicit typed boundary enum shared by production code
and deterministic tests. It covers the local object publication transitions. The target is
to extend the same pattern through the complete write path so it distinguishes:

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
