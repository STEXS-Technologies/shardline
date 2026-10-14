# Maintenance integration: merge checklist

**The integration and UX acceptance review is complete within the contracts
below.** A subsequent [release upgrade audit](RELEASE_UPGRADE_AUDIT.md) found and
addressed a downstream Rust type compatibility gap with a public API bridge.
The branch has not been merged or deployed.
The version remains 1.11.3; release numbering belongs in a separate PR.

For operators, read [Maintenance without another service](MAINTENANCE_INTEGRATION_UX.md).
For source reviews, executed checks and reproduction commands, read the
[evidence appendix](MAINTENANCE_INTEGRATION_EVIDENCE.md). The appendix preserves
historical findings, including gaps subsequently resolved; this checklist is
the integration acceptance assessment. The release audit records later findings.

## Experience that must remain simple

Keep the existing server commands, maintenance commands and GC timer. Tracking
runs inside their existing process. It adds no service, port, mount, schedule or
operator-selected task ID. Server startup does not start destructive GC. Every
scheduled invocation performs fresh work; saved history never replaces a scan.

Ordinary commands keep live output, diagnostic destinations and their operation
result. Optional recording failure warns without falsely confirming completion
or repeating work. The optional Linux `shardline maintenance` child supplies
explicit deadlines and CPU limits; ordinary execution has no tracker deadline.

## Eight merge gates

The original eight requirements remain the completion criteria:

1. Unmodified server commands and manifests initialize tracking automatically.
   Cover local metadata, Postgres/S3, server roles and library embedding without
   an operator-managed daemon or new mount.
2. Preserve stdout/stderr, exit codes, configuration handling, diagnostics exports
   and safety barriers. Run the released GC service with its existing restrictions.
3. Consecutive schedules execute fresh scans. Concurrent invocations have defined
   ownership; completed history is never reused as a fresh reachability snapshot.
4. Large, long-running operations retain supported behavior. Budgets, report
   handling, retention and cleanup cause no undocumented limits or unbounded growth.
5. Interrupt controllers and workers at admission, checkpoint, external mutation,
   report proposal and completion acknowledgement. Recovery cannot falsely report
   completion, repeat an unknown mutation or permit an ownerless stale writer.
6. Test shutdown, reboot, container replacement, cross-host replacement and
   competing shared owners. Include coordination-connection loss while the old
   process remains alive. Specify what may continue and what must reconcile.
7. Mixed old/new nodes and rollback preserve history, domain formats and metadata
   arbitration. New records must not make old binaries misinterpret domain state.
8. Operators distinguish server readiness, failed optional jobs and maintenance
   needing reconciliation without understanding dependency internals.

## Current gate audit

All eight pass for the reviewed integration contracts. Passing does not certify
arbitrary external deployments or remove the explicit limits below.

| Gate | Acceptance evidence |
| --- | --- |
| 1: automatic startup | SQLite/local, SQLite/S3, PostgreSQL/local and PostgreSQL/S3 cover All, API and Transfer roles. Public embedding preserves application logging. Ordinary nodes share PostgreSQL history across separate roots, record server PIDs, perform fresh sweeps and need no tracker child, mount or S3 task-write permission. |
| 2: existing command behavior | Released/current comparisons cover twenty invalid-configuration cases, five restricted-root command forms, read-only SQLite fsck, diagnostics larger than 1 MiB, S3 copying/rollback and removed working directories. The unchanged released GC unit passes under its original systemd restrictions. Original output/result are preserved; optional recording warnings remain visible. |
| 3: fresh work and ownership | Later orphans, expired holds, versions and retained shards are discovered by fresh invocations. Concurrent ordinary rebuilds serialize through maintenance ownership. Completed history is never execution authority. High-level upload and metadata-only ownership boundaries have a source-backed review. |
| 4: capacity and cleanup | Current optimized SQLite/PostgreSQL drills fill all 4,096 slots and retire 4,094 finished runs in 97 bounded calls, preserving unresolved evidence and payloads. Legacy tests cover 1,026 runs and 10,000 artifacts. Workload comparisons at concurrency 1, 32 and 128 preserve stable counters/settings. Source review verifies fallback, live-output handling and documented limits. |
| 5: interruption safety | Native admission/checkpoint and completion-before-shared-ack cuts cover tracked command forms. PostgreSQL adds 25 receipt cuts, partial repair, latest-pointer and three derived-index mutation cuts. Local mutations, accepted remote copy PUTs, source inventory and reports have process cuts. Delete acknowledgement loss, delayed takeover and saved-reply recovery preserve uncertainty without replay. Optional controller death terminates its child and retains unresolved effects. |
| 6: replacement and shutdown | Current process cuts, container replacement, SIGTERM, real VM reboot and backend faults pass. Four earlier optimized two-machine drills cover partitions, node loss and rebuild/repair/GC connection loss while the source lives. Current source review verifies nonreplaceable ownership and cooperative history shutdown; active calls may finish. |
| 7: mixed versions and rollback | Actual released/current overlap, owner setup and rollback preserve payloads, compatible writers, migration records and runtime grants. Released startup preserves all four unfinished file/native/xorb checkpoints across scoped/default identities and present/missing bytes. Predecessor archives verify read-only without rewriting. The destructive-GC pause remains mandatory. |
| 8: operator distinctions | Readiness, failed optional work, corrupt/full/unavailable history and pending deletes have distinct responses/actions. The current walkthrough runs startup, fsck, repair, GC and history, verifies state/next_step fields and direct SIGTERM. Inspection/recovery need no admin API or worker setup. |

All 23 named current deployment drills pass across two complementary runs of the
same production binary. The current CLI harness passes 34 ordinary tests; four
owned PostgreSQL tests run separately without selected skips. The maintenance
unit selection passes 42 tests. Counts supplement operation-specific evidence.
Historical two-machine runs are identified explicitly, rather than described as
latest-binary executions.

## Completed focused reviews

| Review | Evidence |
| --- | --- |
| PostgreSQL derived-index interruption | [Three actual mutation cuts](MAINTENANCE_INTEGRATION_EVIDENCE.md#postgresql-derived-index-interruption). |
| Public write boundaries | [Entry-point inventory](MAINTENANCE_INTEGRATION_EVIDENCE.md#public-write-boundary-inventory) and [library guidance](RELIABILITY_ARCHITECTURE.md#library-upload-guarantees). |
| Ordinary-command overhead | [Fallback, output and resource review](MAINTENANCE_INTEGRATION_EVIDENCE.md#ordinary-command-overhead-review). |
| Shutdown and ownership loss | [Failure/replacement evidence map](MAINTENANCE_INTEGRATION_EVIDENCE.md#shutdown-evidence-map). |
| Upgrade and rollback | [Backend, grants and rollout map](MAINTENANCE_INTEGRATION_EVIDENCE.md#rollout-evidence-map-and-operator-walkthrough). |
| Operator walkthrough | [Executed commands and response checks](MAINTENANCE_INTEGRATION_EVIDENCE.md#rollout-evidence-map-and-operator-walkthrough). |

## Ownership and recovery rules

| Operation | Owner and recovery |
| --- | --- |
| Fsck | Diagnostics; inspection does not resume domain work. |
| Index rebuild | Exclusive maintenance owner. Transactions may commit partial progress; fresh rebuild rediscovers current inputs. |
| Lifecycle repair | Shared protection and existing row conditions; not every repair is serialized. Fresh repair handles current eligible rows and trusted saved replies. |
| Mutating GC | Exclusive owner, durable remote-delete attempts and reference protection. Unknown outcomes block affected writes. Trusted matching replies can settle attempts without another user-object delete. |
| Storage copy | Existing cutover contract: callers coordinate writers and GC. History adds neither a cluster cutover lock nor automatic replay. |
| Server startup sweeps | Existing server owns fresh cleanup. Replacement records its own run and retains uncertain foreign history. |

High-level local and PostgreSQL file/xorb uploads acquire protection automatically.
HTTP uploads retain their request owner through publication. Metadata-only library
APIs require caller-held ownership and storage validation; they have no object-store
input and cannot certify backing bytes or complete references.

## Limits and deployment requirements

- Pause destructive GC during upgrade and rollback, finish old jobs, run the
  usual PostgreSQL owner upgrade and replace maintenance binaries. Older GC
  does not create new delete markers. Retain protection state during rollback.
- History establishes neither cluster-wide exactly-once execution nor current
  deployment health. Check readiness separately. A foreign PID is not a lease.
- Process loss does not cancel arbitrary active database/storage calls. A backend
  may retain ownership until it actually ends. Unknown remote outcomes require
  reconciliation; object absence alone cannot settle a delete.
- Automatic history admission and incremental cleanup have logical limits.
  Unfinished/corrupt evidence stays retained. Limits do not truncate live output
  or impose a disk quota. Inventory and active I/O can add latency; fixture
  timings establish no production latency or throughput guarantee.
- Restricted-root Linux fixtures do not certify network filesystems with different
  locking semantics. Tenant policies can make inspection unavailable; keep those
  restrictions and use original logs.
- Moving live optional-runner state between hosts/PID namespaces is unsupported.
  A changed executable cannot run an older pinned definition; inspect effects
  before intentionally starting a new run.

These explicit boundaries remain part of the reviewed contract. Follow the
[rolling-upgrade procedure](ROLLING_UPGRADE.md) and concise
[operator guide](MAINTENANCE_INTEGRATION_UX.md).
