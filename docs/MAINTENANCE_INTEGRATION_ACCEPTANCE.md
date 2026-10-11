# Maintenance integration: maintainer acceptance checklist

Status: automatic integration implemented, deployment acceptance in progress.
The embedded server tracker, ordinary Linux CLI tracking with live output,
durable local journals, signed shared evidence, export recovery and retention
are present. The requirements and acceptance gates below remain the completion
criteria; passing the optional runner tests alone does not satisfy them.

## Validation so far

The repository CI passed, including 9,325 workspace tests, 35 reliability
contract tests and 13 release CLI integration tests. Focused tests cover the
embedded tracker and storage migration, including pre-existing objects under
the new task-evidence prefix.

Normal CLI tests check its original PID, fresh consecutive runs, live command
results, read-only dry runs, export failures and later evidence export without
repeating completed work. A real GC invocation also passed in an unprivileged
container with a read-only filesystem, no network, no new privileges, writable
temporary storage and only the existing deployment root writable.

The exact systemd sandbox could not be exercised on the validation host: its
user manager rejected namespace setup before executing Shardline. Container
validation covers similar filesystem restrictions, but does not establish that
systemd gate. Production Postgres/S3 rolling deployment and multi-node fault
testing also remain outstanding. The checklist below records the full scope;
these checks do not justify a blanket rollout or exactly-once guarantee.

## Default experience

Operators must continue to start Shardline with `shardline serve` and run the
existing maintenance commands and installed GC schedules. They must not have
to launch a tracker, manage another service, select task IDs, provision a second
state mount, or replace their deployment manifests to obtain task supervision.

The supervisor belongs to the Shardline runtime. It does not require a separate
tracker PID or network service. A process worker, when an operation requires
isolation and reliable fencing, is a child managed by Shardline. Worker creation,
authorization, recovery and shutdown are internal responsibilities. An idle
sidecar would not establish supervision of the work that actually runs.

Starting the server must not introduce a new GC schedule, trigger destructive
maintenance, or replay unknown mutations. Existing systemd GC timers remain the
schedule owner. Server startup owns only the work already associated with that
startup and the lifecycle of its background services.

## Compatibility constraints

The advanced `maintenance` command requires `--state-dir` and `--task-id`.
Normal commands allocate those internally; `serve` uses an embedded observer for
existing S3/LFS startup sweeps and the Hub webhook-secret upgrade sweep. Existing
upload/session journals, webhook delivery ownership and provider refresh loops
retain their own authority. Task observation does not automatically make all
Tokio tasks durable, restartable jobs.

These concrete constraints prevent promoting the optional controller unchanged:

- Generated GC units use `ProtectSystem=strict` and grant `ReadWritePaths` only
  for the existing working directory. The optional controller requires state
  outside the deployment root. Existing timers must work without reinstalling
  units or expanding filesystem permissions.
- Its 30-minute deadline, 15-minute timeout, 600 CPU seconds per generation and
  64 MiB report ceiling are new limits. Existing maintenance must not silently
  inherit them when processing a large deployment.
- It binds live runtime state to a machine and PID namespace. Container restart
  and pod replacement need an explicit ownership/reconciliation path; copying
  that SQLite state to a replacement pod is not supported recovery.
- Reusing a completed task ID returns the recorded result. A scheduled run must
  perform fresh work rather than return yesterday's successful scan.
- Optional reports differ from existing command output. Normal stdout, stderr,
  exit codes and GC diagnostic export destinations are existing interfaces.
- Library users may run an executable with no hidden maintenance-worker entry
  point. A server library must not assume its host is the Shardline CLI binary.

## Work ownership

Before default activation, every relevant entry point needs a specified owner,
completion rule and recovery rule:

| Work | Existing owner | Required integration |
| --- | --- | --- |
| Fsck, rebuild, GC, lifecycle repair and storage migration | CLI invocation; systemd timer for scheduled GC | Automatic run identity and durable supervision through the existing entry point; preserve command interfaces and application barriers. |
| Hub webhook-secret upgrade sweep | Server startup blocking task | Track actual scan outcomes and lifecycle. The current sweep skips some failures and returns no result, so returning from it cannot certify complete success. Preserve lazy per-row upgrade. |
| Hub webhook delivery | Request-launched asynchronous delivery | Retain the durable delivery record as effect authority. Interrupted delivery can have succeeded remotely; tracking must not imply exactly-once delivery or authorize a duplicate automatically. |
| JWKS/OIDC refresh | Provider-owned asynchronous loop | Preserve provider ownership, refresh policy and cached-key availability. Distinguish service liveness from a durable maintenance job; a network refresh failure is not a reason to restart data mutations. |
| Request-bound blocking work and cache loader heartbeats | Request/cache owner | Preserve request cancellation, resource limits and cache leases. Do not create a durable restartable job for every Tokio task. |

Task records must describe completed, failed, running and needs-reconciliation
outcomes truthfully. A signed result authenticates that result, not current
deployment health or cluster-wide completion.

## Persistence and recovery

Automatic persistence must fit the deployment's existing writable resources.
Its placement must be validated against object inventory, GC, backup/export,
storage migration and report output paths. It must not make a writable local
filesystem a new requirement for a Postgres-backed deployment that previously
did not need one. The existing optional private runner store can remain an
advanced interface, but is not a deployment default by itself.

Run identities are allocated internally. Separate intentional invocations get
fresh runs; continuation of interrupted work attaches to the appropriate
unfinished run under exclusive ownership. Changes to configuration, executable
or task definition cannot silently replace the pinned request. History and
artifact retention must be bounded without deleting unresolved work or keys
needed to verify retained evidence.

Keep domain transactions, resource fences and local/Postgres maintenance barriers
authoritative. Local worker locks do not coordinate several nodes. For shared
metadata, claim/recovery decisions need ownership that competing nodes can
arbitrate. A replacement node may reconcile durable domain effects, but cannot
claim that it fenced a process on another host solely from a local journal.

Shutdown stops admission and follows the existing drain policy. Any work that
outlives its controller must remain fenced or explicitly unresolved. After a
crash, verify durable evidence and establish ownership before resuming. A fresh
fsck may be repeatable; unknown GC, rebuild, repair, migration or webhook effects
require their operation-specific reconciliation rules. Changing the task ID
must not be presented as a recovery mechanism that makes unknown effects safe.

Expose useful operation names, progress, outcomes and actionable recovery
messages through existing logs and operator surfaces. Internal digests, proof
paths and generation numbers belong in diagnostic detail. Tracking failures
must be visible; silently falling back to an untracked mutation must not produce
a supervised-success claim. Availability behavior must be specified separately
for essential serving, optional background acceleration and administrative work.

## Merge acceptance

Default integration remains unverified until tests establish all of these:

1. Unmodified server commands and deployment manifests initialize the intended
   supervision automatically, without an operator-managed daemon or new mount.
   Exercise local metadata, Postgres/S3, server roles and library embedding.
2. Existing maintenance commands preserve stdout/stderr, exit codes, global
   configuration handling, diagnostics exports and safety barriers. Run the
   previously generated GC service under its existing filesystem restrictions.
3. Consecutive scheduled runs execute fresh scans. Concurrent invocations have
   defined ownership and contention behavior; completed history is not reused
   as a fresh reachability snapshot.
4. Large, long-running operations retain supported behavior. Default budgets,
   report streaming, history retention and artifact cleanup do not impose
   undocumented regressions or unbounded disk growth.
5. Kill a controller and a worker at admission, checkpoint, external mutation,
   report proposal and completion acknowledgement. Recovery never falsely
   reports completion, silently repeats an unknown mutation or leaves an
   unowned worker authorized to continue.
6. Exercise graceful shutdown, reboot, same-host container replacement,
   replacement on another host and two nodes claiming shared work. Explain and
   test which work can continue, which must reconcile and which stays blocked.
7. Roll old and new nodes together against the same deployment. Historical
   evidence, existing deployment formats and metadata arbitration stay valid.
   New supervision records cannot make old binaries misinterpret domain state.
8. Operators can distinguish a healthy server, a failed optional background job
   and a maintenance operation awaiting reconciliation without understanding
   Penelope or StateChronicle internals.

The dependency compatibility fixtures and optional runner tests remain valuable
prerequisites. They do not establish these default-operation guarantees.
