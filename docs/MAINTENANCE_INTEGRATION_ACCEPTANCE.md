# Maintenance integration: merge checklist

**Not ready to merge.** The normal process model is implemented, but the full
rollback audit remains open. Readiness-based testing found that released 1.11.3
could falsely complete a new scoped file intent from a same-name default file.
The compatibility guard now preserves all four unfinished states in both scoped
and default namespaces during actual released-server startup on PostgreSQL and
SQLite. Legacy control records still recover, and current recovery remains
functional. The earlier listener-based pass is superseded: it stopped too early.
Remaining deployment combinations and overall startup cost still need review.
Normal router startup now defers eligible file-byte verification to its own
process lifetime. Held-read fixtures cover 65 MiB files across multiple containers
on SQLite and PostgreSQL for all three server roles. Startup still checks saved
evidence and candidate metadata; these fixtures do not bound all startup work.
Native shard reuse across repositories is now fixed and tested on both metadata
backends, including retries alongside older unfinished history. Actual released
startup preserves all four new native checkpoints in default and scoped
namespaces, with both present and missing physical request objects.
Canonical xorb retry identity now uses the stored length rather than footerless
wire length. Both backends cover retry and recovery at every unfinished
checkpoint; actual released startup also preserves the new xorb histories with
physical bytes present or missing.

The full compatibility and failure audit is still open. The version remains 1.11.3;
the version bump belongs in a separate PR.

For operators, read [Maintenance without another service](MAINTENANCE_INTEGRATION_UX.md).
For test details and commands, read the [evidence appendix](MAINTENANCE_INTEGRATION_EVIDENCE.md).

## Experience that must remain simple

Keep using the existing server command, maintenance commands and GC timer.
Normal tracking runs inside their existing process. It adds no service, port,
mount, schedule or operator-selected task ID. Every scheduled invocation starts
fresh work; saved history never replaces a new scan.

The optional `shardline maintenance ...` runner can use a child for deadlines
and CPU limits. Those controls do not apply to ordinary commands.

Tracking must preserve command output, exit codes, configuration handling,
diagnostic exports and existing safety barriers. Failure to save optional
history produces a warning, not a verified completion or an automatic retry.

## What has been tested

These results cover named fixtures. They do not certify every deployment.

| Area | Verified behavior | Limit |
| --- | --- | --- |
| Startup | Normal commands initialize tracking in the existing process. Local, SQLite/S3 and PostgreSQL router fixtures cover all three roles, application-owned logging and fresh startup sweeps. SQLite/S3 stores history in SQLite rather than payload objects. | Remaining deployment combinations still need review. |
| Existing commands | Released/current comparisons preserve output, status, configuration errors, diagnostic exports and restricted-root behavior. Both ordinary repair forms pass. The released systemd GC service runs with its original restrictions. | Comparisons cover the named commands and configurations. |
| Fresh work | Consecutive invocations discover new work and produce distinct results. Completed history does not replace a scan. | An explicit retry still requires investigation of uncertain effects. |
| Optional recording failures | Admission and final-recording faults preserve tested CLI work, startup LFS/S3 cleanup and Hub secret upgrades. Destination-history failure preserves a local storage copy. | These are specific fault points, not every external-mutation phase. |
| Shared ownership | Configured PostgreSQL and process drills reject stale owners on covered file, dedupe, reconstruction, stored-object, OCI and S3 paths. | Metadata-only library APIs require caller-held ownership and storage validation. |
| Uncertain deletes | Covered writes return retryable errors. Saved successful replies settle matching attempts without another user-object delete. Missing replies remain blocked. | Older GC does not create the new markers; pause it during upgrade and rollback. |
| Backing data | Staged and native PostgreSQL publication fixtures reject missing physical data and roll back metadata. Stale Repeatable Read/Serializable snapshots require fresh transactions. | Local-CAS interleavings do not establish every remote-S3 fault or legacy reference layout. |
| Interruption and replacement | Named process, reboot, container, cross-host and connection-loss drills preserve unfinished evidence and fresh takeover. The optional Linux child stops with its controller. | Phase-by-phase coverage and remaining deployment layouts stay open. |
| Shutdown | Optional cleanup stops between storage calls when draining begins or the router is dropped. Partial cleanup resumes later. | Active storage I/O may finish; ordinary application work is not cancelled by this flag. |
| History capacity | Separate optimized SQLite and PostgreSQL tests fill all 4,096 slots. Automatic cleanup retires 4,094 completed runs in 97 calls, within the per-call artifact limit, preserving unresolved evidence and user data. Fresh recording succeeds. | Logical limits do not bound physical disk use, inventory latency or production cost. |
| Upgrade and rollback | Named mixed-node fixtures preserve domain data, migration records and existing runtime grants before and after owner setup. Historical signed evidence verifies without rewriting. | Remaining legacy and deployment combinations require review. |
| Operator guidance | HTTP tests distinguish readiness, failed jobs, corrupt or unavailable history, full recording capacity and pending deletes. The read-only `history` CLI includes pending-delete inspection without enabling the admin API or configuring the optional runner. The guide gives actions for each. | Historical completion does not certify current deployment health. |

The optimized deployment evidence includes all 23 fault scenarios across two
complementary runs of the same binary. After the native recovery correction,
the optimized CLI suite passes all 22 scenarios, including destination-history
failure after successful copying and process termination during copying or
after its report is saved.
These counts supplement the behavior checks above and do not close a gate on
their own.

The existing local benchmark comparison covers concurrency 1, 32 and 128 with
matching report schema and non-performance counters. A 4,096-entry GC mark-only
comparison preserves diagnostics larger than 1 MiB. Neither establishes
distributed performance or safe reclamation for every legacy layout.
The configured PostgreSQL/local-storage benchmark also preserves workload
counters and settings at concurrency 1, 32 and 128 after automatic public upload
ownership. Embedded cancellation releases the GC barrier in its configured
fixture. These results do not establish remote-storage cost or a production SLO.

See the appendix's [test table](MAINTENANCE_INTEGRATION_EVIDENCE.md#what-has-been-tested)
and [reproduction commands](MAINTENANCE_INTEGRATION_EVIDENCE.md#reproducing-the-machine-tests)
for exact fixtures, historical measurements and their boundaries.

## Known gaps

The following still need implementation or stronger evidence before merge:

- **Remaining write paths and older GC.** Some library callers and references
  hidden inside legacy payloads do not use the covered ownership checks.
  Direct Postgres file, dedupe, reconstruction and stored-object reference
  publication is covered; this gap concerns paths outside those boundaries
  and references not visible in the protected metadata.
  In particular, lower-level `PostgresRecordStore` methods publish metadata
  without inspecting object storage. The plain `commit_native_shard_metadata`
  entry point also supplies no caller-derived physical reference keys; the
  server uses the connection-bound variant with those keys and checks backing
  bytes in its publication transaction. Database reference protection alone
  does not prove storage presence for library callers.
  The metadata-only API also exists in released 1.11.3: its native commit writes
  records and mappings in a database transaction without an object-store input.
  This is an existing guarantee boundary, not evidence that the integration
  introduced missing-content publication. It still limits the stronger guarantee
  this branch can promise to library users.
  Raw-xorb, native-shard and digest-checked file request uploads now create,
  read and advance their intents on the original request-owned database
  connection. A lost connection
  cannot be replaced to finish that request's bookkeeping. Detected owner loss
  returns HTTP 503 with `Retry-After: 1`; the client must make a fresh request.
  Public `PostgresBackend::upload_file` and `upload_xorb` calls now establish
  their request-owned session automatically. Nested calls reuse the existing
  session and reject its loss rather than acquiring a replacement. Lower-level
  metadata APIs and startup reconciliation retain their existing pool-based
  APIs and require the caller's applicable ownership checks.
  Public local file and xorb uploads also acquire the existing shared filesystem
  GC barrier automatically, before payload writes. Local embedding therefore
  needs no caller-managed lock for those upload methods. Lower-level metadata
  methods remain outside this high-level upload boundary.
  Both public upload methods cancel while waiting for GC without leaving a
  waiter or writing payloads. A subsequent exclusive GC and file upload succeed.
  With a pre-existing writable barrier and live SQLite WAL/SHM files, both
  methods also succeed under a 0555 metadata root without changing its mode.
  This does not cover a restricted root lacking the existing barrier file.
  Plain file requests without upload intents also check their original session
  before body ingestion and after ingestion. Publication failures check that
  same session to return retryable ownership loss without reacquiring it.
  Upload progress is not proof that bytes still exist. Native file publication
  checks backing data in its publication transaction; raw xorbs store immutable
  bytes without publishing file metadata. GC uses file records and dedupe
  mappings as reachability authority, and reconciliation never publishes files
  solely from an intent. Already-active remote storage calls may still finish
  after connection loss. Broader mutation coverage remains part of this audit.
  File-intent recovery now preserves attempts without a `Stored` checkpoint.
  For later attempts with a verifiable scope, it checks the published bytes against the
  saved SHA-256 before completing progress. New scoped file intents now preserve
  exact repository components and recover within that scope. Unrelated or
  missing file metadata cannot settle a logical file intent. Ambiguous legacy
  scoped identities remain preserved. Actual released-server startup preserves
  new scope evidence with the compatibility guard installed. Router-owned file
  verification now runs outside readiness startup; all-role SQLite/PostgreSQL
  fixtures cover multi-container 65 MiB files. Larger backlogs, remote-storage
  cost and remaining deployment combinations still need review.
  An older GC binary does not establish the current remote-delete markers.
- **Uncertain remote outcomes.** Missing or invalid delete replies leave an
  object blocked. Object absence alone cannot safely clear it. Recovery and
  remote effects outside the covered paths need further checks.
- **Scale and retained history.** Bounded pages are tested; whole-operation cost,
  larger reports, broad deployment performance and total unresolved-history
  growth are not fully established. Automatic private-workspace admission is
  limited to 4,096 slots, preserving evidence and falling back to shared history;
  active private workspaces and retired scratch now share that limit. Shared
  metadata has its own 4,096-run automatic-admission budget, with new artifacts
  limited to 64 KiB. Existing evidence and explicit legacy imports remain
  unrestricted. These are logical admission limits, not physical disk limits;
  whole-operation cost and broader scaling remain open.
  Metadata fixtures with 132, 1,026 and 4,096 runs
  drain completed history in batches while retaining unfinished and corrupt history, but
  retention and legacy inspection still walk the inventory.
  Automatic retention now retires at most 256 run-artifact keys per invocation,
  leaving a marker so larger histories continue later. This limits deletion
  work, not inventory latency or active storage-call duration.
  A separate optimized SQLite fixture now exercises this automatic API at all
  4,096 occupied slots: 97 cleanup calls retire 4,094 verified completed runs,
  with per-call artifact limits, unresolved histories, payload preservation
  and fresh admission checked. The corresponding optimized PostgreSQL automatic
  cleanup fixture also passes at all 4,096 slots, retiring 4,094 completed runs
  in 97 calls with the same preservation and per-call budget checks. Earlier
  compatibility cleanup fixtures remain separate.
  Retention now stops both metadata and legacy traversal when its artifact
  budget is reached. History pagination keeps its full unordered legacy scan
  to preserve page ordering. This removes work after a retention budget break;
  selecting eligible histories and reaching that break can still require
  inventory scans, so it does not establish a whole-operation latency bound.
  A separate legacy object-store fixture retires one completed run with over
  10,000 artifacts across 40 calls, reopening the history store each time.
  It checks the 256-key budget, resumable markers and unchanged unfinished
  evidence, corrupt history and user payload. A separate legacy archive fixture
  paginates all 1,026 distinct histories exactly once, then incrementally retires
  1,024 finished runs while preserving unfinished and corrupt history and user
  payload. Neither fixture establishes production latency or broader cost.
  Local prefix traversal now skips unrelated directories. The optimized
  1,026-history fixture preserves the same pagination and cleanup assertions,
  with 24 cleanup calls before and after the change; measured cleanup falls
  from 186.46 to 5.39 seconds in this local fixture. Eligible history scans,
  larger inventories and remote deployment cost remain open.
  An optimized ordinary CLI fixture verifies exact
  256-key progress over two invocations, released output/status and unchanged
  payload bytes. Metadata inspection
  stops after its eligible page is settled, but filtering unrelated history can
  still require a longer scan; bounded output does not bound latency.
  A real HTTP fixture now checks the legacy fixed/compressed chunk read budget:
  four requests for two chunks, and two for a selected file range. GET response
  metadata replaces a redundant HEAD while retaining full stored-length checks.
  These request counts do not establish cloud latency or a production SLO.
- **Remaining deployment layouts.** Work-specific database/schema routing,
  row policies, narrower accounts, library embedding and legacy history need
  coverage beyond the passing fixtures. Corrupt, unfinished or unrelated legacy
  objects can still enter an older raw storage copy.

## Eight merge gates

All eight remain the completion criteria. No gate is closed by a dependency
upgrade, a test count or the optional runner alone.

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

SQLite and PostgreSQL receipt faults now cover each admission record, report
materialization and completion acknowledgement. The PostgreSQL fixture also
checks exact database row counts after each rejected write. This closes that
specific evidence gap; external-mutation and process-interruption coverage is
still required by gate 5.

Ordinary local storage-copy process termination is now covered after an actual
destination write and after report materialization, before completion
acknowledgement. Both cuts preserve copied data and unresolved history, and
inspection does not resume copying. Remote-copy interruption still needs its
own evidence; this local fixture does not close the phase audit.

| Gate | Finding | Evidence and remaining work |
| --- | --- | --- |
| 1: automatic startup | Process model and default initialization verified; deployment coverage remains open. | Current public-router startup covers all three roles locally and with PostgreSQL, including fresh sweeps, readiness and same-PID shared history. Local embedding preserves application-owned logging. Restricted schema routing and actual-process drills extend the shared-backend evidence. Remaining deployment layouts in the known gaps still need review. |
| 2: existing command behavior | Named released/current comparisons pass; full compatibility audit remains open. | Restricted commands, the released GC service, large diagnostic exports and configuration isolation have passing fixtures. Sixteen failure comparisons now also preserve released output/status for malformed TOML, missing config/environment files and unusable roots. Remaining work concerns supported command/deployment combinations beyond those comparisons. |
| 3: fresh work and ownership | Fresh discovery verified; concurrency evidence covers named workflows. | The optimized ordinary GC fixture discovers an orphan added after its previous completion, marks it and records a distinct result. Current configuration tests preserve admitted targets and isolate parallel invocations. PostgreSQL rebuild and takeover drills establish ownership for their named paths; the remaining write-path audit still limits broader claims. |
| 4: capacity and cleanup | Full SQLite/PostgreSQL capacity and bounded atomic cleanup pass; broader cost remains open. | Both current 4,096-slot release drills preserve unresolved evidence and payload bytes, including PostgreSQL after publication locking. SQLite and PostgreSQL second-batch failures preserve resumable retirement. Larger legacy archives, long operations and deployment cost still need assessment. |
| 5: interruption safety | Named interruption and remote-delete faults pass; the phase-by-phase audit remains open. | Lost acknowledgements, denied saved replies, interrupted history retirement and process termination have passing fixtures. Remaining work must account for admission, checkpoint, mutation, report proposal and completion acknowledgement across supported operations, including the remaining library write paths. |
| 6: replacement and shutdown | Named node-loss, ownership-loss and replacement drills pass; coverage remains open. | Container replacement, SIGTERM, native reboot and cross-host/connection-loss fixtures distinguish unfinished work from completion. Cooperative history shutdown and fresh cleanup resumption pass. Remaining layouts and active storage-call shutdown behavior still need review. |
| 7: mixed versions and rollback | Named rollout and rollback fixtures pass; the remaining compatibility audit stays open. | Existing domain formats, migration records and runtime grants survive the tested upgrades and rollback. Older GC must remain paused until job binaries are replaced; it does not create the new delete markers. Remaining legacy histories and deployment layouts still need review. |
| 8: operator distinctions | Response contract and documented recovery guidance verified. | Current HTTP tests distinguish degraded readiness, failed optional work, corrupt history, full recording capacity and unavailable delete inspection. A configured PostgreSQL test checks pending-delete pagination, guidance and preservation of markers. The operator guide maps each case to an action without dependency internals. This does not certify cluster health or close the safety and scale gates. |

The [gate-audit evidence](MAINTENANCE_INTEGRATION_EVIDENCE.md#startup-and-operator-gate-audit)
records the executed checks. Gates 2–7 retain their requirements above and remain
open until their remaining compatibility, failure and cost evidence is complete.

## Ownership and recovery rules

Task history records an outcome. Transactions, locks and operation-specific
fencing authorize changes. A separate PID does not provide exactly-once effects.
A foreign PID does not prove that a remote job is alive or dead.

An idempotency key protects a retry when the duplicate check and effect commit
together. A fence protects takeover when the effect atomically rejects an old
owner. Unknown effects require investigation before retry; a new task ID does
not make a retry safe. Recorded completion does not certify current cluster health.

Keep existing request, upload, cache, OIDC and webhook owners. Starting Shardline
must not turn these into automatically retried maintenance jobs. Preserve lazy
Hub secret upgrades when a startup sweep fails.

The normal owner-run database upgrade installs the optional metadata and
protection functions. See [rolling upgrades](ROLLING_UPGRADE.md) for deployment
steps and limitations; the tracker itself requires no new service or runtime grant.
