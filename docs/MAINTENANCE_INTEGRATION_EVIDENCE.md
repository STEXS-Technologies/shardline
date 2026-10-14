# Maintenance integration: detailed evidence

## PostgreSQL derived-index interruption

The owned PostgreSQL process-cut verifier now also passes three actual SIGTERM
cuts against the current optimized CLI: reconstruction pruning, dedupe pruning
and restoration from real retained Xet shards. A trigger pauses the second
mutation after the first commits. Terminating the CLI preserves the committed
change; the paused PostgreSQL backend retains its advisory owner until the
fixture explicitly cancels that query. The second transaction rolls back.

Read-only history inspection leaves the index unchanged. A later stale entry
or retained shard is added before a fresh ordinary rebuild. That rebuild
discovers the new work, preserves immutable file versions and protected payload
bytes, and records a distinct completed run with its actual fresh PID. The old
run remains unresolved; its native SQLite journal and WAL bytes stay unchanged.
Restored mappings are checked against all three actual retained shard hashes
and paths, and the earlier shard bytes remain unchanged.

The final rebuilt harness passes the three cuts in 7.86 seconds. The same owned
fixture also reruns all 25 receipt cuts (35.49 seconds), partial repair (2.81
seconds) and latest-pointer rebuild (2.47 seconds), with no selected test skipped.
All 34 ordinary CLI tests pass in 5.07 seconds; four owned PostgreSQL tests are
excluded from that ordinary run and executed separately by the verifier.
Strict CLI Clippy, formatting and shell syntax checks pass.
Log: `.codex/validation/postgres-derived-owned-final.log`; detailed results:
`.codex/validation/shardline-postgres-process-cuts.x3PJSm/`.
Reproduce with `bash scripts/shardline/verify_maintenance_postgres_process_cuts.sh
CURRENT RELEASED CLI_TEST_HARNESS`.

These fixtures establish the named PostgreSQL mutation boundaries. Explicit
cancellation of the injected query is fixture cleanup, not a claim that process
termination immediately cancels arbitrary database or storage calls. No
production source or version changes are needed for these checks.

This branch adds automatic task history to existing startup and maintenance.
It is **not ready to merge**. The version stays unchanged; a version bump belongs
in a separate PR.

Operators should start with the [maintenance guide](MAINTENANCE_INTEGRATION_UX.md).
Reviewers should start with the [merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md).
This appendix preserves development evidence, fixture limitations and reproduction
commands. Earlier observations describe their tested revision; later notes record
subsequent fixes. They are not a substitute for the current checklist or proof of
untested deployments.

## Staged upload publication

### Native shard backing content

The write-path audit reproduced the same disappearance window in native shard
registration. A configured PostgreSQL fixture uploads a real native xorb and
parses a real shard, retaining the validator's actual records, mappings and
member references. It verifies the xorb exists, physically deletes it, settles
its exact marker and attempts publication. Temporarily restoring the previous
index commit reproduces an incorrect `Ok(())`; the final server publication
method instead rolls back and returns a retryable error.

Both memory-backed and file-backed server callbacks now use the publication
module on their existing write connection. An outer transaction retains the
native reference locks through final bounded presence checks for the physical
xorbs and normalized shard bodies. Packed member chunk keys are protected by
the existing index guard but are not incorrectly required to exist as separate
objects. Before owner setup installs the guard helpers, the previous path
remains available; the change adds no migration entry, runtime grant, process
or configuration. The lower-level public index API remains a metadata API and
cannot certify storage presence for arbitrary library callers.

The regression also checks pending-delete rejection, deletion of the actual
normalized shard body, rollback of file and dedupe rows, 503 with `Retry-After:
1`, and guidance to restore backing content before retrying the shard. Restoring
the xorb and retrying through the normal upload handler succeeds, and a normal
download returns the original bytes. This uses local CAS with a real database
and controlled phase interleaving; it is not a remote-S3 fault or performance
drill. The earlier optimized deployment binaries below do not include this
additional native publication check.

All five configured publication tests and 21 PostgreSQL delete-guard tests pass.
All 261 selected server error tests pass. Strict all-feature/all-target Clippy
passes for the CLI, server and S3 adapter. Public error enums and existing
unrelated IO error responses are unchanged.

### Full automatic SQLite history capacity

The explicit release-mode capacity fixture passes with 4,096 retained runs:
4,094 real completed histories, one unfinished run and one unreadable identity.
It verifies every paginated ID and each oldest-first cleanup batch, including
pagination after a deleted cursor. The unfinished run, corrupt bytes and user
payload remain intact; successful retirement leaves neither cleanup markers
nor task objects in payload storage.

On this local run, full paginated inspection took 7.114 seconds. The 65 cleanup
calls took 43.685 seconds in total; the slowest call took 0.802 seconds. The
complete fixture, including creation and additional assertions, passed in
84.15 seconds. These measurements use short native embedded histories in an
optimized build. They do not establish production latency, larger legacy
history cost, physical database disk limits or PostgreSQL/cloud performance.
An earlier unoptimized attempt was deliberately interrupted; it is not counted
as a passing check or used for these timings.

The new verifier runs the otherwise ignored scale test explicitly and requires
one passing test with none ignored. Formatting, shell syntax, strict all-feature
all-target server Clippy and the AST graph update pass. Runtime code and the
version are unchanged by this test-only addition.

### Transactions with older snapshots

A configured PostgreSQL 16 fixture reproduced unsafe publication when a
Repeatable Read transaction could not see a subsequently admitted delete.
An initial `FOR KEY SHARE` fix also failed the regression. The corrected guard
uses `FOR SHARE` on an internal coordination row before physical-key locks;
delete admission updates that row before taking physical-key locks. This makes
stale Repeatable Read and Serializable snapshots fail with SQLSTATE `40001`.
The normal Read Committed publication path does not lock this row. Delete
admission adds one row update, committed before the remote delete request.
Long transactions at stronger isolation can delay otherwise unrelated delete
admissions; this cost and broader concurrency still need assessment.

The regression verifies rejected publication leaves no visible metadata and a
fresh retry succeeds after the marker settles, preserving the configured
isolation level. Server responses map serialization failures to retryable 503
errors rather than calling them pending deletes. The separate coordination
domain preserves the existing first permit epoch and subsequent identities.
Schema identifiers remain outside dollar-quoted function bodies; the existing
adversarial schema-name test exercises this installation boundary.

Four configured publication tests pass in 2.27 seconds. They also exercise
actual serialization-error responses through HTTP, OCI and S3, checking 503,
`Retry-After: 1` and guidance to start a new transaction. A two-connection
fixture at both stronger isolation levels verifies waiting delete admission
does not hold a physical lock that prevents the publisher checking a second
key. All 21 configured PostgreSQL delete-guard tests pass in 14.58 seconds,
including restricted writers, adversarial schema names and permit identities.
Strict all-feature, all-target Clippy passes for the index, CLI, server and S3
adapter; formatting and the AST graph update pass. These are PostgreSQL 16
fixtures bootstrapped with the released 1.11.3 migration command, not a new
optimized deployment or throughput validation.

The optimized all-feature binary built from `ed2d5aff` still reports 1.11.3.
Its SHA-256 is
`8af66771e1dc94cde6abc11a28b43a718cdc5b948b45056bf748052f4f6714dd`.
An immutable copy passed all 11 selected deployment drills in 145.351 seconds,
including real PostgreSQL/MinIO delete faults, existing restricted CLI accounts,
concurrent owners, rolling overlap and rollback, container replacement and
graceful shutdown. No selected drill skipped at runtime; 12 other tests were
excluded by the selection. The restricted released/current OCI and S3 process
drill also passed, preserving objects and upload state after database owner
loss, publication failure and SIGKILL. Existing table grants and the released
migration ledger remained unchanged, the artifact table remained private,
and ordinary servers had no children at readiness or completion.

A broader configured PostgreSQL index run passed all 184 selected tests in
56.68 seconds with no runtime skips. This includes the other PostgreSQL and Hub
metadata paths matched by `postgres::`; the count is not a claim of 184 separate
deployment scenarios. The broader merge gates, stronger-isolation cost and
uncovered legacy writers remain open. Earlier optimized results below describe
their original revisions and do not include this snapshot protection.

New publication logic lives in `postgres_backend::publication`, with its tests
in `publication::tests`. Shared PostgreSQL fixtures live in `test_support`.
These use ordinary Rust module declarations; there are no `#[path]` overrides.

A real-Postgres fixture stages an actual packed xorb, verifies it exists, deletes
it, and settles the matching delete attempt before publication. Replacing the new
S3 publication call with the previous call reproduces an incorrect successful
publication. With the fix, both staged-file and native S3 publication roll back
their metadata and return a retryable error. A fresh upload retry restores the
bytes and succeeds. Pending deletes retain their reconciliation error; failed
conditional publication retains its original result. A separate database
connection proves the backing-object lock lasts until the transaction ends.
Both configured publication tests pass in 0.65 seconds. Their payload store is
local CAS: this deterministic interleaving is not itself a remote-S3 fault drill.

The final check runs inside the metadata transaction on the actual write
connection. It locks the existing delete slots, supports direct chunks and
packed xorbs, and bounds concurrent presence checks with existing upload
parallelism. It applies to newly staged uploads, not arbitrary legacy records
or copy operations. Before the existing owner setup installs the guard helpers,
the previous publication behavior remains available. Blackhole benchmarking
does not require stored bytes. Physical deletion outside the covered guards
remains outside this guarantee.

HTTP, OCI and native S3 return 503 with `Retry-After: 1` and tell the caller to
retry an upload whose staged content disappeared. A settled delete is not
misreported as pending. The public server-error enums remain unchanged; an
internal typed storage error carries this case. The S3 classifier extension has
a default preserving existing implementations and error classes.

Validation also passes 261 selected server unit tests, all 178 S3 adapter tests,
15 configured PostgreSQL history tests in 22.41 seconds, and the actual CLI
work-session fixture in 34.53 seconds. Strict all-feature, all-target Clippy
passes for the CLI, server and S3 adapter. The optimized binary still reports
1.11.3; its SHA-256 is
`58cef9df53a2d7a97ed9b13486026c1a1ff2987505b3125518d5b10129d0eb21`.
An immutable copy of that binary passed all 11 selected deployment drills in
161.516 seconds, including real PostgreSQL/MinIO delete faults, restricted
commands, concurrent ownership, mixed-version rollout and rollback, and
container replacement with graceful shutdown. No selected drill skipped at
runtime; the 12 excluded tests were outside the selection.
The broader merge gates remain open, including upload presence-check cost and
uncovered legacy or externally managed writers.

## Recording capacity in the admin response

The authenticated full-history router fixture now also starts a fresh router
with expired LFS staging files. Normal startup removes those files even though
new history admission is refused. An exact before/after snapshot of all artifact
keys and bytes is unchanged: startup neither fabricates completion nor leaves
orphan evidence at capacity. All 15 configured PostgreSQL metadata tests passed
in 22.10 seconds; the actual CLI work-connection fixture passed in 34.18 seconds.
Strict all-feature, all-target CLI Clippy passed. Operator guidance now explains
that an alive recorded process cannot prove an embedded job is still running;
its outcome may have failed to save. Stored states and evidence formats are unchanged.

The optimized all-feature binary built from `3b1a0359` passed all 11 selected
deployment drills in 144.440 seconds with no runtime skips; 12 unrelated tests
were filtered out. This revalidates restricted commands, concurrent owners,
remote-delete faults, replacement, shutdown, mixed-node rollout and rollback
after the capacity notice and scope/copy fixes. The verifier used immutable
copies of both binaries. Current/released local copies also passed, including
byte-for-byte legacy relocation, released rollback and execute-only binaries.
Restricted real-MinIO copies passed without extra payload-bucket permissions.
The 4,096-entry mark-only diagnostic comparison preserved released output,
status, payload bytes and exports larger than 1 MiB. The binary reports 1.11.3.

A further actual rebuild-handler fixture passed in 19.84 seconds. It retained
the native records of a completed run, modeled that run as expired, filled the
remaining 4,095 slots with unresolved evidence and made private tracking
unavailable. Admission was refused but the command succeeded without claiming
a new verified outcome. Post-work verified retention freed the expired slot;
the next fresh command recorded completion. All unresolved bytes and the private
operator file remained unchanged. This proves capacity recovery through the
existing command path, not a distributed disk quota or current cluster health.

Automatic work-session scope bindings and storage-copy histories use the same
bounded admission policy. The actual work connection is retained; a legacy
manifest alone cannot authorize an orphan metadata association after its live
metadata manifest disappears. Explicit legacy publication remains unrestricted.
All 15 configured PostgreSQL metadata tests passed in 22.20 seconds, including
that legacy/live-manifest transition. The actual CLI work-session fixture passed
in 33.73 seconds across its ordinary tracking routes with one PostgreSQL login
connection. Four copy/evidence fixtures passed in 5.03 seconds, checking admitted
and rejected orphan records, metadata-only local/private archives, unrelated
database configuration, unavailable history and unchanged source bytes. Strict
all-feature, all-target CLI Clippy passed.

The optimized binary built from `67f482ab` passed all 11 selected deployment
drills in 163.561 seconds, with no runtime skips and 12 unrelated tests filtered
out. This covers admission before the later admin notice and scope/copy changes;
it does not prove those later edits. The deployment verifier now snapshots both
executables into its own fixture so a concurrent build cannot change drill bytes.

The maintenance collection now reports full automatic history through its
existing `history_notice` field. A bounded key count reads no artifact bodies
and reserves no admission slot; publication still performs the atomic check.
Capacity inspection uses the actual metadata target and existing read grants.
Failures return a recording-availability notice without replacing an otherwise
valid history response or implying that the server is unready.

All 14 configured PostgreSQL metadata tests passed in 22.64 seconds. The actual
authenticated router fixture filled history to 4,096 manifests, observed the
full-history notice, and kept `/api/v1/status` successful. Legacy, RLS, routing,
retention and concurrent-admission regressions also passed. Seven SQLite artifact
tests passed, including absent-table inspection without DDL and full/free capacity
observations. Strict all-feature, all-target CLI Clippy passed. The operator
guide now leads with unchanged commands and explains warnings, results, retryable
writes and the upgrade workflow without dependency internals.

## Automatic metadata admission

Automatic metadata publication now admits at most 4,096 retained histories per
SQLite database or PostgreSQL schema. Retiring markers conservatively consume
admission capacity. SQLite serializes admission in its immediate transaction;
PostgreSQL serializes it with a schema-scoped transaction advisory lock and
requires READ COMMITTED without changing caller isolation. Identical existing
publications and later artifacts for admitted runs remain available at capacity.
New artifacts require their manifest and are limited to 64 KiB. The original
unrestricted primitives remain available for explicit legacy imports.

Seven SQLite artifact tests passed, including competing publishers for one slot,
exact retries, conflicts, orphan rejection, body limits and legacy publication.
All 14 configured PostgreSQL metadata tests passed in 23.12 seconds, including
concurrent final-slot admission and rejection inside a caller-owned repeatable-read
transaction without changing its isolation. Five automatic-maintenance tests
passed in 38.60 seconds. The new actual rebuild-handler fixture filled metadata
with 4,096 unresolved manifests and made private tracking unavailable: the
command succeeded, retained every historical byte, added no orphan records and
did not claim verified completion. Shared cleanup now runs after application
work even when receipt admission failed, preventing that failure from starving
verified retention. Admission policy uses the conventional `task_tracking::admission`
module; no `#[path]` attributes occur under `crates/`.

These checks cover automatic logical admission, not physical database disk
usage, all explicit library callers, all legacy binaries or total inspection
cost. The integration remains subject to the full acceptance checklist.

## Private tracking admission

The optimized all-feature binary built from `86c807e1` passed all 11 selected
deployment drills in 147.946 seconds with no runtime skips; 12 unrelated tests
were filtered out. This revalidates restricted commands, concurrent ownership,
rollback, remote-delete faults, replacement and shutdown after combining private
and retired scratch admission. The binary remains version 1.11.3. Shared-history
growth, whole-operation cost and remaining write-path coverage are still open.

The 4,096-slot automatic-admission budget now includes retired scratch waiting
for deletion. Admission and retirement share a nonblocking archive-directory
lease, and verified export reuses its existing lease for retirement. Root identity
and ownership are checked; a replaced archive or foreign root cannot borrow the
old lease. Existing scratch validation never recreates directories removed by
another cleaner. The combined inspection budget remains 8,192 entries, with no
new file, process, service or configuration. Shared metadata remains outside this limit.

Five admission tests and five cleanup tests passed, covering queued capacity,
symlinks, lock contention, export-lease reuse, root replacement and missing
scratch. Both journal-export regressions and all three existing automatic tests
passed, including full-archive recovery. A further actual rebuild-handler fixture
passed with 4,096 queued symlinks that make cleanup fail safely even as root:
shared history records completion in the original PID, every queued link remains
intact and the outside operator file is unchanged. Strict all-feature, all-target
CLI Clippy and formatting passed. These are library/handler and filesystem tests;
they do not establish a distributed storage budget or deployment performance.

The optimized all-feature binary built from `e2e6f877` passed all 11 selected
deployment drills in 172.010 seconds with no runtime skips; 12 unrelated tests
were filtered out. This revalidates restricted commands, concurrent ownership,
mixed-version rollback, remote-delete faults, replacement and shutdown after
private admission and fallback-cleanup changes. The binary remains version 1.11.3.

Automatic CLI private-workspace admission now has a 4,096-slot limit and stops
inspection after at most 8,192 archive entries. Admissions serialize on the
existing directory inode with a nonblocking lock; no persistent lock file,
process, service or configuration is added. Existing workspaces remain usable,
and paths and ownership retain their normal checks. Capacity or lock contention
selects shared tracking before fresh application work instead of creating more
private workspaces. That initial limit excluded retired scratch; the later combined
budget above includes it. Shared metadata remains outside the admission limit.

Three focused tests passed for capacity, unrelated entries and concurrent
admissions. Actual rebuild-handler fixtures passed with 4,096 retained workspaces,
preserving every unfinished fixture file and producing shared completion in the
original PID. A second full-archive fixture includes a verified completed
predecessor with a modeled dead local PID and aligned private/public identity.
Post-work export removes that verified workspace and restores admission for the
next fresh run. Its native records and proofs stay unchanged; this is a modeled
owner transition in a library test, not an actual process-death drill.

Automatic, corrupt-private-journal and shared receipt regressions passed.
The database-only test in the local shared suite had no configured database;
the separate configured CLI test passed against disposable Postgres in 33.22
seconds, covering rebuild, GC mark and lifecycle repair through both observation
paths with a one-connection login. Strict all-feature, all-target CLI Clippy and
formatting passed. Shared fallback now continues private receipt export and
verified cleanup after work without repeating shared retention or application work.

## Shared history retention after configuration changes

The optimized all-feature binary built from `8078848c` passed all 11 selected
maintenance deployment drills in 128.121 seconds, with no runtime skips. Twelve
unrelated tests were filtered out. This covers restricted commands, concurrent
ownership, mixed-version rollback, remote-delete recovery, container replacement
and graceful shutdown after the shared-retention and atomic-finalization changes.
The binary still reports version 1.11.3. These results do not close the remaining
scale, unresolved-history growth or uncovered write-path requirements.

When its candidate batch is full, retention now excludes newer identities
before reading their scope bindings. Selected runs still require verified scope
authorization. A real-Postgres 65-run fixture injects a scope-read failure for
the newest run: cleanup removes the oldest 64 without reading that binding, the
next invocation fails and preserves the remaining history, and cleanup succeeds
after restoring reads. The exact test passed in 7.13 seconds, with formatting
and strict server Clippy passing. This avoids unnecessary scope reads; the
inventory scan and unresolved-history growth remain open.

Retention now lives in the conventional `task_tracking::retention` module,
separate from status inspection. The extracted method bodies match the prior
revision exactly; public methods and storage formats are unchanged. Formatting,
strict all-feature/all-target server Clippy and the oldest-first regression pass.

Retention previously selected only the legacy configuration fingerprint, even
when shared inspection accepted a verified database-scope binding. Completed
history could therefore remain after a database-login change. Retention now
accepts verified matching Postgres scope bindings for metadata-only history.
Foreign unbound runs, other scopes, corrupt bindings and legacy object copies
remain protected; existing maintenance-delete permissions still apply.

Retirement preserves the exact original manifest bytes. The signed binding
stays available through artifact cleanup, and its final deletion commits in the
same Postgres transaction as deletion of the retirement marker. A replacement
configuration can therefore resume interrupted cleanup without relying on the
former login's fingerprint. No migration, new function or runtime grant is added.

All nine server metadata tests passed with disposable Postgres 16 configured.
The new checks cover differing configuration fingerprints, unbound history,
another signed scope and a legacy copy. A forced marker-delete failure rolls
back binding deletion, preserving exact pretty-printed manifest bytes; a later
invocation finishes after the fault is removed. Local retention regressions also
pass, including the larger backlog fixture. Database tests in that local-only
run had no `DATABASE_URL`; the separate configured nine-test run is the database
evidence. Formatting and strict all-feature, all-target server Clippy passed.

This fixes retention grouping for verified shared metadata. It does not bound
full-scan latency or total unresolved-history growth, and does not extend
cross-configuration cleanup to legacy object-backed history.

A further real-account transition test passed against disposable Postgres 16.
The source login has schema usage and SELECT/INSERT/DELETE on the existing
reliability table, with no direct artifact-table grant. It writes two completed
runs and one interrupted run, then its pool closes. A different login resolves
the same history scope despite its different configuration fingerprint, removes
the two completed runs, and retains the interrupted run. The fixture removes
its temporary role and database container. The exact selected test passed in
1.25 seconds; formatting and strict server Clippy passed. This checks database
accounts and shared retention, not a cross-host failure or current cluster health.

## Retention batch reads

The optimized all-feature binary built from `5e62ac6b` passed all 11 selected
deployment drills in 136.788 seconds, with no runtime skips; 12 unrelated tests
were filtered out. This revalidates rollback, restricted commands, concurrent
ownership, remote-delete faults, replacement and shutdown after native-record
batching. The binary remains version 1.11.3. Total inventory cost and retained
unresolved-history growth are still open requirements.

Native history verification now batches record bodies through the conventional
`task_tracking::evidence_reader` module, shared with manifest inspection and
retention. It still validates every sequence, semantic transition, signature,
proof and materialized result. The reader enforces the original 16 KiB record
limit before decoding, including bodies prefetched by a 64 KiB metadata reader.
Each metadata page remains bounded to 64 bodies. Public interfaces, evidence
formats and legacy lookup behavior are unchanged.

The local task-tracking suite passed all 24 selected tests, including page
boundaries, exact-limit and oversized records, legacy conflicts and both backlog
fixtures. Its database-only routing check had no configured database. Separately,
all 13 metadata tests passed against disposable Postgres in 21.63 seconds. An
instrumented database read function confirms completed-history inspection and
report retrieval use bounded record pages; reports and verified completion remain
correct. A native SQLite regression confirms oversized and gapped histories stay
corrupt and survive retention while valid finished history retires. Formatting
and strict all-feature, all-target server Clippy passed. This reduces individual
record reads; it does not bound total inventory cost or unresolved-history growth.

The optimized all-feature binary built from `750cecbd` passed all 11 selected
deployment drills in 157.802 seconds with no runtime skips; 12 unrelated tests
were filtered out. This revalidates mixed-version rollback, restricted commands,
concurrent ownership, remote-delete faults, replacement and graceful shutdown
after the shared-reader and housekeeping-order changes. The binary remains
version 1.11.3. The separate scan-cost and unresolved-history requirements remain open.

Ordinary CLI housekeeping now runs after fresh work and outcome recording.
Both private-state and shared-only paths use the same evidence-retirement helper.
Configuration validation still precedes application mutation. Cleanup, history
relocation and pending receipt exports cannot replace the command's result or
rerun application work. This removes old-history scans from before work starts;
housekeeping can still delay final exit and remains subject to the scale audit.

An actual-command SQLite regression covers both paths with successful and denied
cleanup. Its trigger rejects retirement until the new command's report exists.
Fresh rebuilds complete in the original PID, preserve exit status and verified
history, and retire expired history after work. Denied cleanup preserves old
evidence; removing the fault permits later cleanup. The automatic fallback,
private corrupt-journal and shared receipt tests passed. The database-only test
in the local shared suite had no database configured; a separate disposable
Postgres run passed the exact CLI work-session test in 32.74 seconds. It covers
rebuild, GC mark and lifecycle repair through both paths with a login limited to
one connection. Formatting and strict all-feature, all-target CLI Clippy passed.

History inspection now shares retention's bounded manifest reader through the
conventional `task_tracking::evidence_reader` module. Both metadata backends
load at most 64 bodies of at most 64 KiB per page. Legacy-only and unordered
keys retain ordinary reads; conflicting copies and oversized identities retain
their existing error handling. Public interfaces and evidence formats are unchanged.

The focused reader regression passed across page boundaries, a deleted cursor,
an oversized body, a conflicting copy, backwards legacy enumeration and a nested
key. All 11 server metadata tests passed with disposable Postgres configured.
The 21 existing local task-tracking tests also passed, including both backlog
fixtures; their database-only checks had no configured database and are not
additional Postgres evidence. Formatting and strict all-feature, all-target
server Clippy passed. This reduces repeated manifest reads during inspection;
it does not bound the complete inventory scan or unresolved-history growth.

After both batching changes, the optimized all-feature binary built from
`ca627d23` passed all 11 selected maintenance deployment drills with no runtime
skips. The verifier ran the complete selected matrix in 128.677 seconds; 12
unrelated deployment tests were filtered out. This revalidates restricted
commands, mixed-version rollback, concurrent ownership, remote-delete faults,
container replacement and graceful shutdown on the current implementation.
The binary remains version 1.11.3. These results do not close the separate scale,
remaining write-path, reboot or cross-host audit requirements.

SQLite retention now uses bounded payload pages as well. The additive local
reader returns at most 64 bodies of at most 64 KiB, preserves literal prefixes
and deleted cursors, and leaves a missing optional table uncreated. It uses the
existing artifact table and makes no migration-ledger change. Both backends
share the retention payload cache and retain legacy-conflict checks.

All five SQLite artifact tests passed, as did the reversed-creation-time
regression and the 132-run pagination/cleanup fixture. The latter retained its
unfinished and unreadable evidence through four cleanup calls. The configured
real-Postgres multi-page retention regression also passed after this shared
cache change. Formatting and strict all-feature, all-target Clippy passed for
both crates. These results establish bounded payload reads and unchanged cleanup
selection; they do not establish a production performance target or remove the
full inventory scan.

Additional conflict regressions passed for both bounded readers. The SQLite
fixture preserves exact metadata and legacy identity bytes when they disagree.
The real-Postgres multi-page fixture now also includes a completed run with a
conflicting legacy identity: normal completed history drains, while both versions
of the disputed identity remain unchanged. These checks verify that batching
does not bypass the existing legacy-conflict rule. The Postgres run passed in
10.32 seconds; formatting and strict server Clippy passed as well.

Postgres retention now fetches bounded manifest payloads in pages instead of
reading each manifest separately. A page contains at most 64 bodies of at most
64 KiB each. Oversized records retain their length without transferring their
body. The query composes the existing read/page functions on the same caller
connection, preserving transaction, schema, timeout and permission checks.
It introduces no migration, function installation or runtime grant.

Retention still checks legacy byte conflicts and verifies eligible outcomes,
selects the oldest finished runs, and retains corrupt or unresolved history.
Seven index artifact tests and six server metadata tests passed with a disposable
Postgres 16 database configured, including prefix/cursor boundaries, oversize
handling, caller transaction preservation, row policies and legacy retention.
Strict all-feature, all-target Clippy passed for both crates. This reduces
per-manifest round trips; the full scan and unresolved-history growth remain open.

A further real-Postgres regression crosses the 64-entry payload-page boundary.
It creates 65 completed runs, reverses their creation timestamps relative to key
order, and adds an unfinished run and unreadable identity. Cleanup removes the
oldest 64, then the last completed run, and a third invocation removes nothing.
The unfinished and unreadable identity bytes remain unchanged. The configured
disposable Postgres 16 fixture passed in 8.85 seconds; this is a functional result,
not a production latency target. Formatting and strict server Clippy also passed.

## Retention ownership cleanup

The metadata retention chronology regression creates 65 completed runs, with
creation timestamps deliberately reversed relative to their ordered artifact
keys. The first cleanup removes the 64 oldest runs and retains the newest; the
second removes the remaining eligible run. This protects existing oldest-first
behavior against an incorrect early stop after 64 metadata keys. It does not
remove the full-inventory scan or establish a deployment-scale latency bound.

The optimized all-feature binary built from `fe40b096` passed the complete
11-drill maintenance deployment matrix after these cleanup changes. The verifier
confirmed 11 selected tests and 11 passes with no runtime skips; 12 unrelated
deployment tests were filtered out. The run took 158.216 seconds on this fixture.
This covers the checks listed in the reproduction command below, not the separate
reboot, cross-host or scale gates. The binary still reports version 1.11.3.

Retention hold creation and release now retain the same ownership guard used by
the public metadata barrier. It closes the bound Postgres pool on early errors,
unwind and cancellation, while normal completion still awaits connection cleanup.
There is no new process, setting or runtime grant.

A disposable Postgres 16 instance, bootstrapped by released 1.11.3, passed all
13 `retention_runtime::tests` with `DATABASE_URL` configured. The new factory
failure test preserves the original error and verifies that both an error and a
deliberate panic allow an exclusive maintenance owner to acquire within two
seconds. Existing contention, cancellation and lost-session tests also pass.
This is evidence for retention ownership cleanup, not every library write path.

Rebuild, lifecycle repair and GC also retain a shared `WorkPoolCleanup` guard
from acquisition until operation exit. Early storage errors and cancelled futures
close their work pools, including retained handles; GC's independent reply
recorder receives the same cleanup. Normal completion still awaits closure.
The implementation lives in the conventional `maintenance_barrier::work_pool`
module. A no-connection regression verifies that retained handles are closed
and reject acquisition after the operation guard drops. Disposable real-Postgres
fixtures passed 13 barrier tests, 13 retention tests and 17 rebuild tests.
The exact GC ownership-loss regression also passed with a configured database;
its original `gc_runtime::tests` filter selected zero tests and was discarded.
Strict all-feature, all-target Clippy and formatting checks passed.

A separate disposable Postgres 16 fixture passed both work-pool cleanup tests.
Cancellation now has direct evidence: an exclusive owner blocks a competitor,
then aborts while another caller retains its pool handle. The retained handle
immediately rejects acquisition and the competitor acquires within the test's
two-second deadline. This tests local future cancellation, not a network partition.
The embedded Hub secret sweep uses the same guard for unwind cleanup; cancelling
its async caller still leaves existing blocking work owned until that work exits.

## Known gaps

- **Ownership protection for other operations.** Mutating GC, rebuild, lifecycle
  repair, Hub tree repair and retention hold updates bind their locks and metadata work to non-replaceable Postgres
  connections. The two-machine
  `connection-loss` drill passes, and a database test rejects an old pool's
  attempted write after takeover. Retention fault tests cover setting a hold
  after losing its barrier and releasing a hold during a blocked database query.
  Repair fault tests preserve an expired hold after connection loss during its
  scan, including a two-machine CLI run with a competing rebuild.
  Hub tree repair now uses its lock-owning session for reference reads and
  publication. Its real Postgres regression reproduced publication after
  takeover on the old implementation; the bound implementation fails and
  preserves the original ref. Other library callers using a separate pool
  still need to adopt the bound session.
  All 13 Hub repair tests pass with an actual Postgres fixture, including caller
  cancellation while a competing GC owner waits. All 12 retention/barrier tests
  pass; eight exercise Postgres. Dropping the public barrier immediately closes
  retained pool handles and permits a new exclusive owner after connection cleanup.
  Native OCI request publication now also borrows its request's lock-owning
  session. The real server-process drill terminates that session during tag
  publication, preserves the prior manifest for released/current readers and
  verifies a normal retry succeeds without restarting the server.
  Partial-upload publication now uses that same connection too. Its native
  fault test first reproduced PATCH returning 202 after request ownership loss;
  the fixed path rejects the request without advancing session generation or
  adding a part, and a normal PATCH retry succeeds. Session creation, completion
  claims, cancellation transitions and reopening now use the same connection.
  The native creation regression first reproduced 202 after losing its separate
  request owner; the fixed path creates no session. A blocked completion claim
  now fails with the session still active and its generation unchanged; retry
  publishes the exact blob bytes for both released/current readers. All 13
  index session tests pass, including direct rejection of all four mutations
  after connection termination with unchanged session and journal state.
  Standard mutating Postgres requests now lend their GC-owning session to
  guarded resource publication across frontends. The S3 HTTP fault regression
  first reproduced PUT returning 200 after request ownership loss; it now
  rejects the overwrite, preserves the previous bytes and accepts a retry.
  S3 batch deletion explicitly releases each resource before moving on, while
  retaining GC ownership. A database test verifies another writer can acquire
  the released resource, exclusive GC remains excluded, the next resource
  uses the same session, and final pool reuse leaves no advisory locks behind.
  All 137 S3 tests, 279 OCI tests and nine provider functional tests pass.
  The combined native-process drill also starts released 1.11.3 with OCI and
  S3 enabled before the owner upgrade, then starts current Shardline on the
  same restricted account, Postgres database and local object storage. It
  verifies the S3 publication session itself owns the GC lock, terminates it,
  observes a server error with the server still alive, preserves the old bytes
  for both readers, then accepts a normal current retry and a later released
  overwrite. The OCI interruption/restart phases still pass. Binary checksums
  and both unchanged 1.11.3 version strings are captured with the fixture.
  S3 fixtures now isolate unrelated metadata/object-storage pairs, and the
  one-connection progress fixture uses the single production middleware layer.
  Provider push and access callbacks now commit the delivery claim, repository
  state and their evidence on the resource-lock-owning connection, as rename
  and delete callbacks already do. The push regression first reproduced a
  failed request leaving both state and delivery committed. The fixed push and
  access regressions terminate ownership during the state write, require both
  rows to roll back, retry the same delivery successfully, verify the saved
  state through its evidence, and deduplicate a later replay. Existing
  state regressions also require the failed request to preserve its entire
  previous snapshot. A successful push keeps access and reconciliation fields;
  a successful access change keeps revision and reconciliation fields. All
  six real-Postgres ownership-loss cases pass, alongside the other ten route
  tests and strict server lint. Rename/delete HTTP interruption tests include
  both latest and version file records. Terminating ownership during the state
  mutation rolls back the earlier record changes and leaves no retention holds
  or delivery claim. Retry moves both records and all state fields for rename;
  deletion removes both records and state and creates the expected chunk/xorb
  holds. Replaying the same delivery does not add holds or repeat changes.
  Opportunistic delivery-history cleanup is retained. All 584 server tests
  selected for provider behavior passed before these additional tests.
  Mutations outside guarded publication remain open;
  these tests do not prove deployment health or remote exactly-once effects.
  Ordinary file and native shard publication now also borrow the matching
  request's ownership connection for their final metadata transaction. The
  additive record-store methods preserve existing library signatures and
  reference checks. A backend regression terminates the real request session,
  rejects a file overwrite and native shard publication with the previous
  record snapshot unchanged, then successfully retries both. All 28 upload
  tests and all ten record-store tests pass against a migrated Postgres fixture,
  and strict index/server lint
  passes. The first broad run exposed fixture ordering: one xorb test ran before
  another test installed the public schema; rerunning against that schema passes.
  A two-node native shard HTTP regression now pauses the real handler's file
  record write, terminates its request-lock-owning connection and observes a
  server error. The record snapshot remains unchanged, both nodes can still
  read the previous reconstruction, and both return 404 for the unpublished
  reconstruction. A normal POST retry succeeds and both nodes then read it.
  This uses real Postgres and shared local object storage with the production
  handler, authorization extractor and request middleware; it does not cover
  process interruption or every frontend. Other upload HTTP paths,
  upload-intent phase ownership and library callers outside request sessions
  remain open.
  Other entry points that keep coordination
  separate from effects still require review and fault coverage. Polling or
  cancellation alone does not establish protection at their mutation boundaries.
- **Remote effects and acknowledgements.**
  OCI pending-delete responses preserve HTTP 503, their existing error envelope
  and `Retry-After: 1`. A real Postgres manifest request verifies the header and
  explanation; all 279 OCI tests and strict server lint pass. The delay tells
  clients when to retry, not when recovery will finish.
  Local GC crash boundaries and a real
  MinIO deletion with its response withheld have passed. The latter preserves
  unresolved history after killing the original CLI; fresh fsck does not repeat
  the delete. The previously failing hold/delete takeover regression now passes:
  terminating GC's database owner while its S3 delete is paused makes the later
  hold return a typed retryable error. A replacement ordinary GC also rejects
  the pending object without sending a second delete. The old request can still
  finish, but it cannot erase newly acknowledged hold protection. Its source
  records failure. An observed successful single-attempt reply now permits
  exact-epoch settlement through an independent recorder using the same database
  credentials; no GC work resumes. Missing replies remain unresolved.
  Additive signed-reply primitives now bind an observed reply to its registered
  attempt key and durable metadata namespace. PostgreSQL tests reject tampering,
  stale attempts and other targets, and verify settlement after the source
  connection closes using the existing GC role without fence-table grants.
  Normal GC now attempts to save signed replies in existing object storage
  before settlement. Denied evidence writes retain ordinary acknowledged
  settlement behavior. Sweep and ordinary `repair` process a bounded saved-reply
  page on their existing ownership session; a durable cursor advances past
  malformed evidence. Real PostgreSQL recovery tests cover a 64-record malformed
  backlog, source-session closure, missing-reply preservation and retirement of
  already-settled replies. This does not recover a delete whose reply was never
  received or lost before durable storage, and does not close the broader gate.
  Two real database-outage drills save a successful MinIO reply while PostgreSQL
  is stopped, keep later holds blocked after restart, then settle through either
  ordinary `repair` or a fresh sweep without another user-object DELETE. The
  original GC remains failed. A separate denied-receipt-write drill confirms
  ordinary GC still completes and clears its marker using existing settlement.
  Normal Postgres/S3 GC now admits quarantine, OCI tombstone and resumable-staging
  deletes durably, sends each through a client with mutation retries disabled,
  and settles only a successful response. Its work remains on the original
  ownership session; an independent recorder can only settle that acknowledged
  attempt after the work session fails, and the original failure still propagates.
  Completed markers are removed; one sequence prevents stale identity reuse.
  A normal-completion drill verifies marker removal and subsequent hold admission.
  The existing owner-run `db migrate up` installs functions and a retention
  trigger atomically, keeping the 35 released migration records and their
  checksums unchanged. The trigger also checks older retention writers.
  A dedupe-table trigger also rejects inserts and updates that reference a
  pending physical shard key or its named chunk key, including unchanged released SQL writers. It
  requires no new retention or fence-table privileges for mapping writers.
  File-record, reconstruction and stored-object triggers now protect their
  directly named chunks, containers, cache sidecars and mapped shards. A real
  file-version transaction preserves its old latest/version metadata on
  rejection; S3 publication adds no listing row. Restricted writers need no
  new retention/fence grants. A 20,000-chunk file and 20,000 dedupe mappings in
  one transaction use at most 256 publication locks rather than one lock
  per reference. Current native shard upload now reads validated xorb
  descriptors and checks member chunks and their existing mapped shards in
  the same metadata transaction. A real PostgreSQL/native upload regression
  rejects a pending member even with an empty chunk-cache sidecar, publishes
  no file metadata, and succeeds after proven settlement. Older mapping writers
  also reject pending canonical chunk identities, including updates that change
  the hash; malformed legacy hashes retain their existing behavior. Their
  references to other members hidden in payloads remain unprotected. Older SQL-only
  writers and other publication
  paths, a supported reconciliation path for uncertain markers, protection when
  an older GC binary issues the delete, and remote coverage for other operations
  remain required work. New native Postgres manifest uploads now record verified
  hidden state before storing previously absent bytes, then transition to
  published state only when metadata publication commits. This does not yet
  establish cleanup of every interrupted upload. A native-process drill now
  covers released/current manifest visibility and retry after publication failure.
  The owner upgrade now materializes current hidden manifests into the existing
  OCI deletion inventory, and a journal trigger keeps new schema-scoped stages
  there without extra runtime grants. Row-scoped writers retain journal-only
  staging. Ordinary GC now discovers up to 64 missing candidates per sweep on
  its exclusive work session, using its existing authorized maintenance account.
  Discovery does not bypass active row policies on that account; cleanup with
  a row-restricted GC account remains open.
  A missing completion record never authorizes replay.
- **Large deployments and history growth.** Long operations, large reports and
  total growth of unresolved history still need deployment-scale validation.
  Saved-reply publication and cleanup add object-storage requests. An initial
  release-profile PostgreSQL/HTTP drill covers 1, 64 and 1,024 deletions, verifies
  one user delete plus two receipt requests per object, and leaves no pending
  marker or saved reply. The 1,024-object local run took 2.12 seconds for a guarded
  baseline without saved replies and 3.11 seconds with them. These are local
  boundary measurements, not released-binary, cloud or whole-GC benchmarks.
  Provider latency, large inventories and end-to-end GC cost still need measurement.
  Native Postgres manifest staging adds one storage existence check; its
  deployment-scale latency and abandoned-upload cleanup still need measurement.
  Owner-run installation also scans current visibility heads to restore earlier
  hidden stages. A local synthetic-history drill now measures discovery and
  upgrade cost at 100,000 published heads; real deployment cost remains open.
  Discovery bounds new candidates per call, but not the cost of scanning heads.
  Unfinished evidence is retained, so completed-history cleanup alone does not
  establish a total storage bound.
- **Remaining compatibility coverage.** Passing deployment fixtures establish
  those configurations. They do not establish compatibility for every filesystem,
  permission layout or deployment manifest. History across different database
  logins remains unfinished for some maintenance entry points: the legacy deployment
  fingerprint includes the login name. A real PostgreSQL regression establishes why simply dropping it
  is unsafe: two roles on the same database resolve to distinct default schemas
  despite identical URL options. Shared history must use the resolved database,
  schema and object target while preserving legacy evidence and isolation of
  other deployments. The owner-run upgrade now installs a stable namespace
  identity and a schema-bound read function without new fence-table grants.
  An additive resolver agrees across roles using the same initialized target,
  separates schemas and object targets, and refuses row-level security or an
  absent schema. An additive tracker option now signs a companion binding to
  the original run manifest with that run's key. Tests cover two real database
  logins sharing one resolved target, tampering, retries and retention cleanup.
  Normal server startup now publishes these bindings automatically. Admin
  inspection admits verified matching records across logins and preserves
  unbound own-login history. Resolution uses the existing pool, performs no DDL,
  and falls back to legacy grouping with an explicit response field when
  unavailable. Real PostgreSQL tests exercise both normal router publication
  and shared admin inspection. Ordinary GC, rebuild and lifecycle repair now
  bind on their existing barrier-owning work session. Both CLI tracking paths
  preserve the original process and handlers; a real PostgreSQL CLI test runs
  all three handlers through both paths and verifies their signed bindings and
  completed outcomes. Database tests distinguish that
  session's schema from an inspection connection, preserve its PID and reject
  a changed scope or closed pool without replacing the session. Storage-copy
  execution uses its copy endpoints without acquiring a server PostgreSQL
  metadata connection. Its history
  cannot safely inherit a server's database scope without a verified association;
  opening a guessed database connection would also add a deployment dependency.
  Local deployments without PostgreSQL now publish new task artifacts in an
  optional table in their existing SQLite metadata database. Artifact encodings,
  signature verification and the domain migration ledger stay unchanged.
  Inspection merges this history with legacy object-backed evidence; retention
  removes both copies without exposing a partly retired run. The local
  current/released copy comparison now passes with PostgreSQL unavailable:
  clean copy, current recopy and released rollback match payload bytes and
  output while ten current task artifacts stay outside payload inventory.
  Verified finished legacy history can now move into metadata in bounded
  batches. Unfinished, corrupt and unrelated object-backed history remains in
  place and can still enter a released raw copy. Ordinary PostgreSQL maintenance
  uses metadata when its account supports the installed functions. Copies to
  S3-only endpoints use private host metadata. The native restricted-account
  copy and released rollback checks pass for that route. Deployment permissions and
  unresolved legacy history remain required work before the full isolation gate
  passes. No database service or mount was added for the local backend.
  The PostgreSQL owner upgrade now also installs a private artifact table and
  guarded immutable publication, bounded reads, 64-key pages and retention
  functions. Existing released table grants, migration rows and owner defaults
  are preserved; direct grants on the optional artifact table are removed so
  default privileges cannot bypass its functions. Reads require existing
  reliability-metadata access. Publishing and retention check their respective
  existing maintenance permissions. Real PostgreSQL tests cover writer, reader,
  GC and denied accounts, concurrent conflicts, paging, reinstallation and RLS
  rejection. New server-owned evidence now uses that table after the owner
  upgrade. Server startup, retention and admin readers reuse the existing
  backend pool and runtime. They select a verified metadata identity and schema;
  each operation verifies and uses the same checkout, with a five-second total
  timeout. No new pool or runtime is created. Legacy object history remains
  readable, and retention removes both formats. Unsupported owner installations
  and row-scoped schemas retain legacy behavior. Unexpected metadata failures
  return an inspection error, while startup preserves serving availability.
  Library embedders can reuse an existing backend through the public async
  `TaskEvidenceStore::from_backend` constructor. The returned scope works with
  `summaries_with_scope` and the existing report API; it is a history filter,
  not execution authority. No extra pool or runtime is created. Calls outside
  a Tokio runtime return a prompt error instead of panicking. The existing
  synchronous constructor and summary method preserve their behavior.
  All 23 tracker tests pass against owned PostgreSQL, including the runtime
  guard and public cross-login summaries; the external API example compiles
  as a Rust doctest and strict server Clippy passes.
  Ordinary PostgreSQL CLI tracking now selects metadata storage through the
  additive `from_postgres_pool` API. Its one-connection evidence pool lasts
  through command completion, independently of the closed lock-owning work
  pool. Evidence connections close after each operation, avoiding an idle
  session that could exhaust a restricted login's connection limit. The scope
  binding is saved on the actual work session. Neither path replaces a work
  session or authorizes effects. Both ordinary
  tracking paths and retained local-journal exports use this selection.
  Metadata initialization failures warn and retain legacy tracking; storage
  copies retain their existing database-independent behavior. Configuration-only
  library stores and the optional runner still use legacy PostgreSQL/S3 storage.
  Full connection/resource limits, source-specific routing, legacy-artifact
  relocation, copy isolation and supported row-scoped history remain unfinished.
  All nine CLI maintenance tests pass against owned PostgreSQL bootstrapped by
  released 1.11.3. A real non-superuser login with a one-connection limit runs
  GC mark, rebuild and repair through both ordinary tracking paths. All six
  results verify as completed with their actual work scope and original PID;
  payload task inventory and remaining sessions for that login are empty.
  All 23 server tracker regressions and strict CLI/server Clippy also pass.
  All 22 server task-tracking tests pass against an owned PostgreSQL fixture
  bootstrapped by released 1.11.3. New tests close the original backend pool,
  verify completed history and reports from a fresh backend, retain and retire
  legacy and metadata records together, reject conflicting and oversized reads,
  and reject a changed metadata identity without publishing payload artifacts.
  Normal router startup publishes its completed LFS sweep in PostgreSQL; the
  authenticated list/report endpoints verify it while payload inventory stays
  empty. Unauthenticated inspection remains rejected. Existing cross-login
  grouping survives restricted artifact permissions. Strict server Clippy passes.
  Runtime routing must follow the work session's resolved metadata target;
  another connection guessed from configuration does not establish that match.
  The artifact API now accepts that actual connection for publication, reads,
  paging and deletion. These methods preserve its schema and transaction;
  successful evidence writes inside a work transaction remain subject to its
  rollback. Evidence conflicts roll back their own savepoint, and the original
  statement timeout is restored. Existing pool-based methods remain available.
  Six real PostgreSQL artifact tests pass, including a borrowed session whose
  schema differs from its pool default, publication and deletion rollback,
  recovery after a conflicting publication, and lock contention under a
  100 ms work timeout. Strict index Clippy also passes.
  Server stores reject a signed work-scope binding that differs from their
  selected metadata target. Jobs whose work target differs from the server's
  inspection target still need source-specific storage routing outside the Hub
  secret sweep described below; this change
  does not establish that association or switch optional runner storage.
  The native mixed-version OCI/S3 drill passes with this owner extension: the
  released ledger and original table grants are unchanged, the existing writer
  has no direct artifact-table access, and prior publication/crash guarantees
  and the ordinary one-process startup checks still pass.
  The earlier SQLite prototype also verifies unchanged released fsck output.
  The verifier has since been replaced with the actual automatic-history check:
  `bash scripts/shardline/verify_maintenance_metadata_inventory.sh CURRENT RELEASED`,
  where `CURRENT` is the branch executable and `RELEASED` is the official
  1.11.3 executable. See the actual metadata inventory results below.
  Shared grouping for these jobs, the optional isolated runner, other entry points and binding every
  server background operation to its work connection remain unfinished.
  The Hub secret sweep now binds from its own non-replaceable work connection,
  rather than inheriting the startup inspection pool's scope. It remains on the
  existing server blocking task and stops on its first storage failure; ordinary
  Hub requests and lazy secret encryption retain their existing request pool.
  Its history storage now also follows that actual work connection. The run
  begins using the same session; final evidence opens an independent connection
  only after the work pool closes, verifies the pinned target again, and cannot
  resume work. A webhook-only writer without metadata-publication privileges
  retains legacy tracking. Unexpected target failures warn and leave the sweep
  running without verified history rather than publishing to the startup schema.
  The PostgreSQL regression passes with different startup/work schemas and a
  login limited to one connection, without direct artifact-table grants or
  fence-table access: completed and interrupted metadata history
  is readable at the work target, the startup schema receives none, the narrow
  writer retains completed legacy history, and terminated work is not replayed.
  All 29 tracker regressions and strict server Clippy also pass.
  The existing-role drill
  inspects history using that role's scope.

These are merge requirements. A passing dependency upgrade or optional worker
test does not close them.

## Experience that must remain simple

- Existing `shardline serve`, maintenance commands and installed GC timers work
  without a tracker service, extra port, operator-selected task ID or new mount.
- Server tracking is embedded. Ordinary maintenance stays in its original CLI
  process, with live stdout/stderr and the existing command interface.
- Each scheduled invocation performs fresh work. A completed historical task
  must not replace today's scan.
- Starting a server does not introduce a GC schedule or retry uncertain mutations.
- Postgres/S3 deployments retain their existing filesystem requirements.
- The optional `maintenance --state-dir ... --task-id ...` runner may create a
  child for cancellation and resource limits. Its extra setup and limits must
  not become requirements for ordinary commands.

## What has been tested

| Area | Evidence and limit |
| --- | --- |
| Existing command and Rust interfaces | Released/current output, exit status, umask and fresh-root comparisons; exhaustive public command matches; configuration pinned through execution. Broader permission layouts remain open. |
| Current native upgrade and process model | The full-feature current binary was rebuilt after the native upload and retry-guidance changes, then the released 1.11.3/current OCI+S3 drill passed again. Both version strings remain 1.11.3; the 35 released migration records and existing table grants remain unchanged. Ordinary servers have no child processes at readiness and completion, including after current-server SIGKILL/restart. Database-owner-loss and normal-retry checks preserve prior records and bytes for both versions. This covers the fixture's restricted Postgres account and shared local object storage, not every deployment or a continuous child-process trace. |
| Generated GC service | An isolated Debian 13 guest runs the unchanged service generated by released 1.11.3 before and after replacing the binary, with its original systemd restrictions. |
| Read-only Postgres/S3 containers | Ordinary fsck, rebuild and GC dry runs use existing shared storage without a writable local root or temporary mount. Read-only S3 fsck warns when tracking cannot publish. This does not promise mutating commands work with read-only credentials. |
| Local root permissions | With an existing writable SQLite database and open WAL/SHM sidecars beneath a mode-0555 root, fsck, rebuild, lifecycle repair and GC mark complete without private task storage. The regression checks four verified completions and unchanged root permissions; the released/current comparison matches stdout, stderr and exit status for all four commands. Fully read-only metadata is outside this proof. |
| Repeated and concurrent runs | Distinct scheduled runs complete. Concurrent ordinary rebuilds serialize through the existing Postgres barrier; one removes the fixture's stale record and the next removes zero. |
| Local crash and reboot | GC interrupted after admission, a committed quarantine change, or a printed report stays unresolved. Fresh diagnostics do not replay it. Actual VM resets also preserve the original records. |
| Remote delete acknowledgement | A proxy forwards the actual S3 deletion to MinIO and withholds only its successful response. The object and candidate are absent while the original ordinary CLI is still alive. Killing that process preserves its three admission records without a completion record. Fresh fsck completes without another delete. This proves unresolved reporting and no diagnostic replay at this boundary; it does not prove fencing of delayed requests during takeover. |
| Delayed delete during takeover | Real Postgres/MinIO regression passes: the paused old request has a durable marker; killing only GC's owner rejects the later hold with the typed retryable error. A replacement ordinary GC sends no duplicate delete. Releasing the old request records source failure; its successful reply settles the exact marker, and a subsequent hold succeeds. This covers current GC and retention writers, not every publication path or an older GC source. |
| Acknowledged settlement after ownership loss | PostgreSQL tests keep the work pool closed and propagate its error even when an independent recorder settles the attempt. Settlement uses the schema captured at admission; an identical key/epoch in another schema stays pending despite the recorder's different search path. A duplicated completion cannot clear a newer attempt. An unavailable recorder preserves the marker. This does not recover missing replies or authorize replay. |
| Acknowledged remote delete | Ordinary Postgres/S3 GC records verified completion after the single-attempt response and marker settlement. Exactly one delete is applied, no object marker remains, and a later hold succeeds. |
| Saved reply during database outage | Real PostgreSQL/MinIO drills stop the database before releasing the successful delete reply. The source saves evidence and fails. After restart, holds remain blocked until ordinary repair or a fresh sweep verifies the reply and settles the marker; neither recovery sends another user-object DELETE. Missing replies remain unresolved. |
| Bounded saved-reply recovery | Real PostgreSQL replay advances past 64 malformed records, settles the later valid reply after source-session closure, retains missing-reply markers and retires already-settled evidence. Scan counts and per-record reads are bounded; total unresolved storage growth and deployment-scale cost remain open. |
| Optional receipt writes | A native GC drill actually denies saved-reply writes, yet preserves successful ordinary settlement and completion. It establishes no new S3 write requirement for existing GC; without a saved reply or database settlement, recovery remains unavailable. |
| Saved-reply request cost | The release-profile `remote_delete_request_cost` drill uses actual guarded PostgreSQL transactions and a counted HTTP fixture for batches of 1, 64 and 1,024 objects. Healthy deletes send one single-attempt user DELETE, one conditional receipt PUT and one receipt cleanup request. No HEAD, duplicate user delete, leftover receipt or pending marker is observed. Admission pins the namespace in its transaction; normal GC no longer reads it separately before admission. Cloud latency and whole-GC performance remain open. |
| Concurrent webhook secret upgrades | SQLite and PostgreSQL use an atomic comparison against the observed secret. Concurrent upgrades allow one winner; stale upgrades preserve rotation, removal, deletion and repository isolation. A real PostgreSQL sweep waits at its conditional UPDATE while another transaction rotates the secret; the resumed upgrade preserves it and reports zero changes. A lazy delivery regression preserves a secret rotated after its snapshot. Existing custom store implementations compile with an explicit unsupported result instead of an unchecked fallback. |
| Hub background work session | A real PostgreSQL test uses a runtime role with a different default schema from the startup inspection connection and no fence-table access. Both successful and interrupted sweep history bind to the actual work target in the original PID. Terminating the source while its SELECT is blocked leaves the secret unchanged, records reconciliation required and opens no replacement work session. No global worker lock, service or configuration is added. |
| Container and machine replacement | New server instances publish their own startup results while lost foreign runs remain unresolved. Two distinct VMs share history without copied private state. |
| Machine loss during rebuild | A waiting rebuild acquires the barrier after the source VM dies, even while the fixture blocks the dead client's query. It removes one stale record and completes. The lost run remains unresolved. Connection-only loss is covered separately below. |
| Connection loss during rebuild | The lock and metadata operations share one non-replaceable connection. The two-machine fault drill terminates that connection, observes a verified failed source run and a completed waiter with one stale-record removal. A separate database test rejects a stale pool's attempted write after takeover. |
| Retention ownership | Hold setting and release use the same session for the shared barrier and metadata writes. Fault tests reject a late setting after exclusive takeover and preserve an active hold when a blocked release loses its connection. Normal hold/GC contention and physical OCI protection also pass. |
| Pending delete primitives | Nine actual Postgres tests cover durable markers, absent-row serialization, unique concurrent admission identities, stale completion rejection, overflow, unchanged retention metadata/evidence on rejection, refusal without an installed trigger, cross-schema rejection, and safe installation with quotes, backslashes and template tokens in schema names. |
| Single-attempt S3 client | A real HTTP fixture returns a retryable server error or drops the response after receiving the delete. Both cases return an error after exactly one request. Existing clients keep their retry policy for other operations. |
| Direct-reference publication during a pending delete | Real PostgreSQL tests reject dedupe, file-record, reconstruction and stored-object publication to pending direct keys. Dedupe inserts and hash updates also check their named physical chunk; rejection preserves the previous mapping. Rejection preserves the previous file version and rolls back a multi-row insert. Restricted writers need no retention or fence-table grants. Publication serializes delete admission before a marker exists. A 20,000-chunk file and 20,000 mappings with canonical hashes use at most 256 publication locks. References hidden inside older writers' xorb payloads and other mutation paths remain open. |
| Current native shard member references | Normal Postgres native shard uploads derive member keys from validated xorb descriptors and check them in the metadata transaction. A real PostgreSQL test empties the optional chunk cache, marks a member pending and verifies rejection with no file records; settlement permits the same upload. References now come from the existing validation read. A real S3 HTTP fixture verifies unchanged read counts and one payload GET for repeated references; byte/file callbacks preserve legacy metadata. Older SQL-only writers, other publication paths and broader deployment-scale cost remain open. |
| Direct OCI publication | Real PostgreSQL tests reject native blob publication, manifest tag changes and deletion-tombstone removal while their physical object is pending deletion. Manifest publication also checks its media-type sidecar. Rejection rolls back tags, tombstones and native evidence; proven settlement permits publication. Other repositories and namespaces remain writable. A legacy column-only tag writer needs no fence-table grants. Current delete admission requires these guards, and the normal owner-run upgrade restores a disabled guard. New native manifest uploads carry validated config, layer, child-manifest and subject identities into the same publication transaction, with all target/reference locks acquired in sorted order before mutation. PostgreSQL regression tests cover pending blob, manifest and sidecar dependencies. Payload references behind legacy tag-only writes, underlying chunk references and older GC binaries remain open. |
| New manifest byte visibility | A PostgreSQL-backed HTTP regression reproduced a digest GET returning 200 after rejected publication. Native uploads now record verified hidden state before writing previously absent manifest bytes. Two routers sharing Postgres and object storage return 404 for digest GET/HEAD after manually staged bytes encounter a later pending dependency; settlement and a normal PUT publish those same bytes. Existing published identities remain visible, and a restricted role without tombstone INSERT or fence-table access can stage and publish. The native-process `verify_maintenance_oci_upgrade.sh` drill starts official released 1.11.3 before the owner upgrade and current Shardline afterward, sharing Postgres and local storage. It forces metadata publication failure, verifies the manifest and media-type bytes exist, and checks both binaries return 404 for digest GET/HEAD while preserving the previous tag and bytes. Released and current retries succeed, both readers return the exact document, and released migration verification accepts every unchanged ledger row. Other failure boundaries and broader layouts remain open. |
| OCI process crash before publication | The same mixed-version, restricted-account drill pauses a real native PUT inside its tag transaction after both immutable objects exist, kills the current server with SIGKILL and verifies exit status 137. PostgreSQL closes that session naturally; the fixture never terminates its backend or forces rollback. Released GET/HEAD and the restarted current reader return 404 for the staged digest. A normal current PUT publishes the same bytes, both readers return the exact document, and the previous manifest remains readable. This covers that publication boundary with shared local storage and PostgreSQL, not every crash point or remote storage layout. |
| Existing OCI runtime account and mixed writers | The same native-process drill uses a non-owner publication account without schema CREATE, superuser, role administration, RLS bypass, fence DELETE or tombstone INSERT/UPDATE. It retains the repository-fence SELECT/INSERT/UPDATE permissions already required by released native uploads. All existing table grants and owners remain identical across owner-run installation. Both binaries serve and publish with that account; current staging materializes its hidden manifest without an extra tombstone grant. A fixture-only pending delete blocks both native writers and preserves the published document. Current returns HTTP 503 with the reconciliation message; released returns its generic HTTP 500. Row policies, other account layouts, dependency-bearing uploads and mixed-version GC remain outside this drill. |
| Owner installation during writes | Compatibility guard installation takes the existing exclusive maintenance barrier before taking trigger DDL locks. Released and current ordinary writes already take its shared side. A PostgreSQL regression identifies the installer's actual session, observes it waiting behind the writer and verifies installation completes after release. The native-process drill also pauses an actual released-server publication, observes the owner CLI waiting at that barrier, then verifies both complete with unchanged migration records and table grants. This prevents that DDL/publication lock-order conflict. Custom writers that bypass the maintenance barrier still require coordination. OCI fixtures now use private schemas and the actual write-barrier middleware. |
| Abandoned manifest cleanup | A private-schema PostgreSQL regression stages a manifest as an account with active tombstone row policies and no tombstone INSERT, leaving only its verified journal entry. Ordinary guarded GC discovers it on the existing exclusive non-replaceable work pool. An active hold preserves both staged manifest objects; after release, GC reclaims the manifest and sidecar and removes their candidate while a published manifest stays readable. An owner-upgrade regression restores journal-only hidden generations, preserves timestamps across repeated installation, leaves published generations alone and rejects tampered evidence during candidate inspection. Discovery requires the caller's own exclusive GC session and existing OCI read/delete grants, creates at most 64 candidates per sweep, excludes published heads and never bypasses active row policies on the GC account. PostgreSQL tests reject an unowned session and an oversized limit, process 65 stages over two pages, and preserve the row-scoped account's inventory. Row-restricted GC, mixed released GC binaries and large-history discovery/upgrade cost remain open. |
| Existing retention role and mixed writers | Released 1.11.3 and current native binaries use the same restricted role with no fence-table grant. Both work before and after the normal owner-run upgrade, reject a fixture-only pending delete, and work again after its settlement. The current error explains that retry must wait for reconciliation. Released migration verification accepts the upgraded schema. This checks hold commands, not every publication path or released-server startup. |
| OCI history scan cost | `measure_maintenance_oci_inventory.sh` uses an owned PostgreSQL 16 fixture with 100,000 synthetic published heads, each preceded by an older Deleted snapshot: 200,000 rows and 165 MB. Three warm trials measured median discovery cost falling from 6.867 seconds to 0.175 seconds, and median repeated owner upgrade from 7.117 seconds to 0.291 seconds. A query-plan comparison isolates the expensive operation-ID regex; discovery now uses a cheap type prefilter and validates complete candidate identities after selecting the latest head. No published head enters the deletion inventory. The synthetic rows are scan load without cryptographic commitments, not reclamation evidence. Native tests separately cover verified stages and malformed legacy identities. These measurements do not bound total scan work, cloud latency or end-to-end GC cost. |
| Existing GC role | Released 1.11.3 and current native binaries both mark and physically sweep an expired MinIO object using a role with no fence-table access. Current GC records verified completion, settles its marker, and admits a later hold. This regression failed before admission/settlement moved behind the scoped, authorized database functions. |
| Repair ownership | A database regression first demonstrated repair removing an expired hold after losing its separate lock connection during GC takeover. Repair now binds its scan and metadata changes to the lock-owning session. The database regression and two-machine `repair-connection-loss` drill pass: the source records failure, the waiting rebuild completes, and the expired hold remains unchanged. This covers metadata changes, not remote mutations by other operations. |
| GC database ownership | A real database regression first reproduced GC completing its blocked scan after losing the coordination connection during exclusive takeover. Mutating GC now binds its metadata work to that connection. The regression rejects continuation, creates no quarantine row or clock anchor, and leaves the fixture object intact. The two-machine `gc-connection-loss` drill records a failed source GC and a completed waiting rebuild with one stale-record removal. This does not establish atomic fencing of remote requests already issued or delayed during takeover. |
| Lock ordering | A queued exclusive owner blocks later shared readers. Cancelling an acquisition closes its session; completed metadata work releases it. |
| OCI request ownership | A real PostgreSQL HTTP fault test first reproduced a manifest PUT returning 201 after its request's GC barrier connection was terminated during tag publication. Current OCI requests now lend that same session to resource fencing, staging and guarded publication; a backend identity prevents borrowing across backends. The regression now rejects the upload and preserves the previous tag and exact manifest bytes. All 279 OCI route and helper tests pass against the owned PostgreSQL fixture. The native-process upgrade drill additionally verifies the sleeping publication session itself holds the GC lock, terminates it, observes a server error while the server stays alive, checks released/current GET and HEAD preserve the old tag and hide the new digest, then publishes successfully through a normal retry. Released migration records and existing table grants remain unchanged. This covers guarded native publication, not every partial-upload metadata write, other frontend or delayed external storage operation. |
| Mixed versions and rollback | Released/current nodes read and append the same object history in old+old, new+old, new+new and old+new combinations against the same Postgres/S3 deployment. No new metadata migration. |
| History inspection | Corrupt records remain visible without hiding unrelated runs. Cancelled requests retain their inspection permit until blocking work finishes. History does not certify server health. |
| Embedded failure diagnostics | Failed server jobs save their supplied counts as signed checkpoints and remain unresolved. The authenticated read-only per-run report endpoint verifies the report and deployment association, rejects tampering, and preserves disabled-API concealment. Legacy or interrupted jobs without a report return an explicit unavailable result; CLI diagnostics keep their existing interface. |
| Pending-delete inspection | Existing admin authentication lists bounded pages of affected keys without replaying or clearing markers. Long keys use compact attempt cursors. Restricted readers need no fence-table grant; insufficient existing read privileges and row-level security report unsupported. This is diagnostic evidence, not a recovery procedure or a health certificate. |
| Export and cleanup | Export visits at most 64 journals per invocation, makes progress past unfinished entries and does not replay work. Cleanup retains unresolved evidence and has interrupted-deletion coverage. Total unresolved growth remains open. |

The detailed test counts are intentionally omitted: they change as the branch
evolves and do not establish these deployment guarantees by themselves.
Historical evidence compatibility is protected by the frozen vectors in
`crates/shardline-reliability/tests/fixtures`.

## Ownership and recovery rules

Task history reports what happened. Storage transactions, locks and
operation-specific fencing decide who may change data.

An idempotency key protects a retry only when the effect and duplicate check
commit together. A fencing token protects takeover only when the effect rejects
an obsolete owner atomically. A separate worker PID supplies isolation and
termination; it supplies neither of these storage guarantees.

| Work | Owner and recovery rule |
| --- | --- |
| Fsck, rebuild, GC, repair, migration | Existing CLI invocation; existing timer owns scheduled GC. Preserve barriers and command results. Unknown mutations need operation-specific inspection before retry. |
| S3/LFS startup cleanup | Server startup owns the existing sweep. A replacement records its own run and leaves uncertain foreign history unresolved. |
| Hub webhook-secret upgrade | Server startup owns the sweep; retain lazy per-row upgrade. Skipped failures prevent certifying complete success. |
| Webhook delivery | The durable delivery record remains authoritative. A remote receiver may already have accepted a request whose acknowledgement was lost. |
| JWKS/OIDC refresh | The provider owns its refresh loop and cached keys. A refresh failure must not trigger data maintenance retries. |
| Request-bound work and cache heartbeats | Existing request and cache owners retain cancellation and lease rules. Every background task need not become a durable job. |

A recorded foreign PID is not a lease. A signed result authenticates the recorded
result, not current readiness, cluster-wide health or exactly-once execution.
Keep readiness, failed optional work and uncertain maintenance distinguishable.

Normal commands retain their original output, diagnostics export paths and
configuration handling. Existing upload/session formats remain readable by old
nodes. Changes to configuration or executable must not silently change an
admitted request.

Private task state must fit existing writable resources and stay separate from
object inventory, GC and storage migration. Local locks do not coordinate hosts.
Copying an advanced runner's private SQLite state to another pod is not supported
recovery. Advanced deadlines, CPU limits and report ceilings must not silently
limit ordinary maintenance. Library embedding must not require a hidden CLI
worker executable.

Tracking failures must be visible. A command that preserves its existing
behavior when observation fails must not claim a verified outcome. Unresolved
work and its verification keys must survive retention. Establish ownership and
verify durable effects before any continuation; a new task ID does not make an
unsafe retry safe.

## Eight merge gates

All eight remain the completion criteria:

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

## Reproducing the machine tests

The maintenance deployment matrix requires Docker Compose and cargo-nextest:

```bash
bash scripts/shardline/verify_maintenance_deployments.sh CURRENT RELEASED
```

It selects 11 drills covering restricted commands, concurrent rebuilds, container
replacement and shutdown, remote-delete acknowledgement faults, and mixed-version
rollout and rollback. Each drill owns its disposable Compose project. The verifier
rejects runtime skips and an unexpected selected test count, and retains logs.
It does not cover the separate KVM reboot and two-machine checks below.

The full automatic SQLite history-capacity scale fixture runs explicitly in
release mode, outside the ordinary unit suite:

```bash
bash scripts/shardline/verify_maintenance_history_capacity.sh
```

It creates 4,094 real completed histories, one unfinished run and one unreadable
identity. It checks every paginated ID, the oldest-first retirement order,
deleted cursors, preserved unfinished/corrupt evidence, user payload bytes and
absence of task payload objects. The verifier requires exactly one passing
test with none ignored and prints inspection, total cleanup and slowest-batch
times. This local fixture cannot establish PostgreSQL, cloud-storage or
production latency guarantees.

The storage-copy compatibility check covers new local history through released rollback:

```bash
bash scripts/shardline/verify_maintenance_storage_copy.sh CURRENT RELEASED
```

It verifies exact payload bytes and matching current/released output on a clean
source with PostgreSQL unavailable, then checks current recopy and released
rollback from a destination containing current task history. It retains its
diagnostics, requires actual task artifacts in SQLite and verifies that rollback
leaves their exact bytes intact. This does not cover historical object-backed
evidence or PostgreSQL/S3 task storage.

The local permissions comparison requires an unprivileged host user and SQLite:

```bash
bash scripts/shardline/verify_maintenance_local_permissions.sh CURRENT RELEASED
```

It compares released/current stdout and stderr for fsck, rebuild, lifecycle repair
and GC mark. All eight invocations must succeed. The database and its existing
WAL/SHM files remain writable beneath a mode-0555 root; four current manifests
must be saved in SQLite, with no task artifacts in payload inventory. This does
not cover a fully read-only metadata database.

For the saved-reply request-count drill, point `DATABASE_URL` at an isolated,
owner-accessible PostgreSQL fixture initialized by `shardline db migrate up`:

```bash
cargo test -p shardline-server --release --all-features \
  --test remote_delete_request_cost -- --ignored --nocapture
```

The drill creates and removes its own schema and HTTP fixture. It prints batch
timings and request counts and asserts exact requests and cleared markers.
It compares a guarded baseline without saved replies against the current
delete boundary; it does not benchmark the complete GC command or a cloud provider.

`CURRENT` and `RELEASED` are Linux binaries. `IMAGE` is a locally verified
Debian 13 generic cloud qcow2 image.

```bash
bash scripts/shardline/verify_maintenance_vm.sh CURRENT RELEASED IMAGE
bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE
bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE connection-loss
bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE repair-connection-loss
bash scripts/shardline/verify_maintenance_cross_host.sh CURRENT IMAGE gc-connection-loss
```

These scripts own disposable KVM guests and isolated fixtures, retain diagnostic
artifacts and clean up their guests on exit. They do not reboot the workstation.
The connection-loss drills pass for rebuild, lifecycle repair and GC. They do not
establish equivalent protection for other operations or remote object-store
mutations.

The local permission comparison requires Linux, `sqlite3` and a normal user
account. Root bypasses directory permissions and cannot establish this case:

```bash
bash scripts/shardline/verify_maintenance_local_permissions.sh CURRENT RELEASED
```

It keeps the fixture database open, prevents new root entries, compares all four
commands and restores permissions before closing its database process.

The remote acknowledgement drill owns an isolated Docker Compose project and a
local fault proxy. Set the executable override to the current binary:

```bash
CARGO_BIN_EXE_shardline=/absolute/path/to/CURRENT cargo test -p shardline-server --test deployment_chaos gc_acknowledgement::drill_gc_applied_remote_delete_with_lost_acknowledgement_is_not_replayed -- --nocapture
```

Confirm the log reports a resolved stack and the final MinIO-deletion assertion;
the drill explicitly reports a skip when its deployment prerequisites are absent.

The delayed-delete regression now passes against real Postgres/MinIO and is
included in normal test runs. Run it directly with:

```bash
CARGO_BIN_EXE_shardline=/absolute/path/to/CURRENT cargo test -p shardline-server --test deployment_chaos gc_acknowledgement::drill_gc_delayed_delete_rejects_hold_after_takeover -- --nocapture
```

Run all remote-delete drills, including both saved-reply outage cases and the
existing-role comparison, with nextest. Each drill needs its own OS process
because the deployment fixture sets process-wide environment variables:

```bash
CARGO_BIN_EXE_shardline=/absolute/path/to/CURRENT SHARDLINE_N_MINUS_ONE_BINARY=/absolute/path/to/RELEASED cargo nextest run -p shardline-server --all-features --test deployment_chaos -E 'test(gc_acknowledgement::)' --no-capture -j 1
```

Check that no drill was skipped. These passing boundaries do not close the other
publication, reconciliation and deployment-scale requirements above.

### Finished legacy history relocation

Normal startup and maintenance now relocate verified completed or failed history
to available metadata storage, with limits of 64 artifacts and 16 MiB per pass.
Each artifact is copied and read back byte-for-byte before its original is
removed; the manifest moves last. A later pass resumes partial relocation
without replaying work. Invalid, unfinished, oversized and unrelated history
remains in place. PostgreSQL relocation checks the signed work scope when present.
Raw storage copy does not modify its source or require source write permission.

Validation passed: 29 tracker tests against SQLite and PostgreSQL 16, including
partial relocation, conflicting metadata, byte limits and different work scopes;
strict CLI/server Clippy; and the native storage-copy script against released
1.11.3. The native check recreated finished object-backed history, verified every
relocated byte, then confirmed released 1.11.3 copied the original payload and
retained its expected output. This covers the supported finished-history case,
not every legacy deployment or S3-only storage-copy endpoint.

### Storage-copy history without payload changes

Normal copies run in their original CLI process and keep scratch outside the
source. Copies to local storage publish history in destination SQLite metadata.
Copies to S3-only endpoints publish history in private host temporary metadata;
they do not guess a PostgreSQL association or write control objects to S3.
That temporary history is not shared across nodes and can be removed by host
cleanup. If temporary storage is read-only, the original copy still runs without
claiming a recorded outcome.

The three new routing tests pass, alongside eight other exercised maintenance
tests; the PostgreSQL-dependent test was skipped in this run. Native local-copy
and S3-copy checks also pass against released 1.11.3. The S3 fixture uses an
account allowed to list its bucket and read/write only the exact payload key.
Its source is read-only, an unrelated PostgreSQL URL is unreachable, and the
recorded owner PID equals the ordinary CLI PID. Current and released recopy
preserve payload bytes, counts and stdout; the bucket contains only its original
payload object. A second copy succeeds with read-only temporary storage and
adds no source files or control objects. These checks cover this fixture, not
every storage provider, deployment permission profile or unresolved legacy run.

```bash
bash scripts/shardline/verify_maintenance_s3_copy.sh CURRENT RELEASED
```

The script creates and removes its own MinIO fixture. `CONTAINER_ENGINE`,
`MINIO_IMAGE` and `MC_IMAGE` can select the test environment; these are test-only
settings, not deployment requirements.

### Automatic tracking before metadata is available

Automatic server and ordinary CLI tracking now require metadata for new history
publication. If the owner extension is absent, metadata access is denied or its
database is unavailable, tracking warns and preserves existing work without
adding control objects to payload storage or claiming a verified receipt.
Legacy history remains readable and eligible for its existing retention rules.
Explicit library constructors retain their legacy publication behavior; callers
can opt into `TaskEvidenceStore::require_metadata_publication()`.

The 30 tracker tests, Hub work-session regression and all 13 CLI maintenance
tests pass against owned PostgreSQL bootstrapped by released 1.11.3. Coverage
includes preserving explicit legacy publication, reading old history while
rejecting new payload history, unavailable metadata, schema-only history readers
and the webhook writer lacking metadata-publication permission. The latter
still completes its sweep without extra grants or payload history. Healthy
metadata history and its signed work bindings remain available.

The native OCI/S3 upgrade drill now also starts current Shardline before the
optional owner upgrade. Both versions read current writes, the migration ledger
is unchanged, the optional artifact-read function remains absent, payload storage
contains no tracker prefix and the ordinary server has no child process. The
remaining owner-upgrade, publication-failure, ownership-loss, process-crash and
retry stages also pass. Strict CLI/server Clippy passes. These results cover the
fixture's existing writer and storage layout, not every deployment permission
profile.

### Optional tracking failures preserve command results

Private directory, identity and journal failures can fall back only before
application work starts. The fallback uses available metadata history; if its
own preparation fails, it runs the original handler without a verified receipt.
After work, report or journal publication failures preserve the original exit
status, retain unconfirmed evidence and never rerun the handler. Confirmed
scratch cleanup is also best-effort and cannot replace the command's result.

Three new regressions exercise the actual rebuild handler with an operator
file at the private-state path, a corrupt private journal and a conflicting
completion artifact. Successful fallback preserves the original files; rejected
completion preserves success without publishing a completed outcome. All 16
maintenance tests pass with owned PostgreSQL bootstrapped by released 1.11.3,
and strict CLI/server Clippy passes.

The CLI's logging writer sends task diagnostics to stderr while retaining the
existing stream for other application logs. Existing embedding subscribers
remain untouched. Native local-copy and S3-copy rollback checks pass. The local
check additionally runs an execute-only binary: the failed executable
fingerprint leaves copy stdout, payload bytes and exit status unchanged, with
no verified receipt. Four released/current commands also preserve stdout,
stderr and exit status beneath a mode-0555 root with writable SQLite sidecars.
The native cleanup drill independently injects an I/O error into removal of the
completed task's top-level private directory. Its marker confirms that the
fault occurred. The command succeeds with unchanged stdout, warns on stderr,
retains its stored completion record and permits a released rebuild with the
same output. Payload inventory receives no control objects. This closes that
specific cleanup boundary; broader filesystem failure coverage remains open.

```bash
bash scripts/shardline/verify_maintenance_cleanup_failure.sh CURRENT RELEASED
```

This Linux-only drill compiles an owned test library that intercepts directory
removal only beneath its fixture's private task root. It adds no production
library, runtime setting or deployment dependency.

## Cleanup after confirmed completion

Confirmed task scratch now moves into a private retirement directory before
recursive deletion. A partially failed deletion therefore cannot leave an
unrecognizable journal in the unfinished-task backlog. Later ordinary maintenance
removes up to 64 retired directories per invocation, using the existing fair
cursor. It does not rerun work or change saved history.

The native cleanup-failure drill now verifies that the next current invocation
clears the leftover directory, preserves every previously saved artifact byte
and records only its own new completion. Released 1.11.3 still runs against the
same fixture with identical output. Unit tests cover bounded cleanup, untouched
unfinished state and refusal to replace an existing retirement entry. This is
an internal cleanup change, with no additional process or operator configuration.
All 18 focused maintenance tests pass with owned PostgreSQL bootstrapped by
released 1.11.3. Strict CLI/server Clippy, native local and restricted-S3 copies,
and all four read-only-root command comparisons also pass.

## Public-library local startup

`embedded_task_history` constructs the public router directly with the existing
local configuration and an application-owned logging subscriber. For All, Api
and Transfer roles, it starts the router twice against the same metadata. Each
startup removes a newly introduced expired legacy LFS staging session and saves
a distinct verified completion, retaining the previous run. Recorded ownership
uses the application PID. Startup creates no CLI maintenance directory or task
objects in payload storage, and the application's logging subscriber still
receives its events afterward.
The integration test and strict Clippy check for its target pass.

```bash
cargo test -p shardline-server --test embedded_task_history
```

This proves the tested local library configuration, not arbitrary embedded
Postgres routing, row policies or remote storage layouts. Those checks remain
part of the deployment gates.

## Row policies and unavailable history

A PostgreSQL fixture installs optional history, saves an artifact, then enables
forced row-level security with a deny policy on the history table. The already
opened store rejects reads and writes rather than bypassing the new policy.
The normal router still starts and removes an expired legacy LFS staging session.
Server status remains available; no new completion or payload history is written.
After the policy is disabled, the original artifact is readable with unchanged
bytes, and the rejected artifact is absent.

Legacy history inspection remains available. Its response now includes a
`history_notice` when metadata history was not selected, explaining that an empty
list does not prove maintenance never ran. Selected metadata leaves this field
`null`; that is not a guarantee of future write permissions or database health.

This checks the explicit refusal of schema-wide history under row policies. It
does not implement tenant-scoped history or certify arbitrary runtime policies.

```bash
cargo test -p shardline-server --all-features --lib task_tracking::postgres_metadata::tests:: -- --test-threads=1
```

The six PostgreSQL history tests pass with an owned database bootstrapped by
released 1.11.3. All 32 admin API tests and strict server Clippy also pass.

## Metadata history across several pages

A local metadata fixture stores 130 verified completed runs, one unfinished
run and one unreadable manifest. Paginated inspection visits all 132 IDs once.
Successive retention calls remove 64, 64, 2 and 0 completed runs. The unfinished
run, unreadable bytes and existing payload bytes remain intact. Cleanup leaves
no retirement markers and publishes no task objects into payload storage.

The debug build on this shared host measured about 2.0 seconds for full paginated
inspection and 5.8 seconds for four retention calls. These are fixture timings,
not production performance guarantees. Both operations still walk the inventory;
large-scale latency and unresolved-history growth remain open checks.

```bash
cargo test -p shardline-server --all-features --lib metadata_backlog_paginates -- --nocapture
```

All 32 task-tracking tests pass with an owned PostgreSQL database bootstrapped
by released 1.11.3. Strict server Clippy also passes.

### Avoiding reads outside the selected page

Once inspection has selected a full page, a higher-sorting ID cannot replace
one of those entries. Inspection now avoids reading and resolving its identity.
Retention similarly avoids native-state verification for newer candidates once
its eligible oldest-first batch is full. Lower candidates still receive the
usual checks, including deployment binding and completion verification.
These checks also work with unordered enumeration and preserve the existing
selection rules; they do not introduce a new task limit or cursor format.

The 132-run fixture additionally verifies the exact oldest-first deletion order.
It measured about 2.0 seconds for inspection and 4.9 seconds for retention after
the change. The shared-host debug timings are indicative rather than a controlled
benchmark. Inventory enumeration remains O(N), so deployment-scale cost is still
open. All 32 tracking tests, 32 admin API tests and strict server Clippy pass.

### Seeking past the history cursor

Later history pages now pass their existing cursor to SQLite and PostgreSQL
metadata enumeration, avoiding rows at or before that key. A cursor need not
still exist: cleanup between requests does not reset inspection to the start.
Legacy object-backed enumeration remains available and keeps its inventory walk.
The existing visitor API retains its behavior; SQLite adds an optional-cursor
variant using the same literal-prefix queries and 64-row storage pages.

A SQLite fixture deletes the cursor, then seeks and deletes 66 later artifacts
across storage pages without touching earlier keys or a wildcard-lookalike prefix.
The 132-run server fixture also inspects after each newly deleted retirement
cursor and verifies the exact surviving IDs. Four SQLite artifact tests, all 32
tracking tests with owned PostgreSQL, all 32 admin API tests and strict index/server
Clippy pass. This reduces repeated enumeration on later pages; first-page scans,
legacy inventory walks and deployment-scale cost still need further work.

### Stopping metadata enumeration when the page is settled

SQLite and PostgreSQL history inspection now stop their ordered metadata walk
once enough eligible records establish the requested page. Deployment and scope
filters still run before a record counts toward that page. Unordered legacy
inventory remains fully considered so its earlier IDs can replace later metadata
IDs; the mixed-storage regression checks this with one-record pages.

The SQLite visitor test checks stopping across a 64-row storage boundary, then
resuming after a deleted cursor without changing unvisited artifacts. The server
fixture introduces a malformed key beyond an earlier valid page: that page remains
inspectable, while reaching the malformed key still reports failure and preserves
its bytes. Four SQLite tests, all 32 tracking tests with owned PostgreSQL, all 32
admin API tests and strict index/server Clippy pass.

This limits unnecessary metadata enumeration, not all history-related work.
Filtering large foreign backlogs, legacy walks, retention eligibility scans and
deployment-scale latency remain open. No process, configuration, cursor format
or database migration is added.

## Current native binary after history-query changes

The full-feature CLI was rebuilt after cursor seeking and early metadata-page
termination. Six native released/current drills pass: OCI/S3 upgrade and owner
loss, local copy, restricted S3 copy, read-only-root commands, injected scratch
cleanup failure and the existing retention-only role. CLI/server strict Clippy
also passes. The copy checks retain payload bytes, output and released rollback;
the cleanup fault retains receipt bytes and never replays completed work.

The OCI/S3 drill now uses the existing optional admin read token to inspect
the current native server before the owner upgrade, after upgrade and after a
SIGKILL/restart. All three responses report server state `ready`. Before upgrade,
history uses legacy grouping and explains that new history is unavailable.
After upgrade and restart, it selects shared metadata history with no notice.
The original role's grants, released migration records and payload state remain
unchanged, and server child-process snapshots remain empty.

These checks cover the owned fixture's roles and storage, not every deployment.
An absent history notice selects a metadata route; it does not certify future
publication, current cluster health or exactly-once effects.

## Large native diagnostic exports

The released/current mark-only GC comparison passes with 4,096 synthetic orphan
files. Both retention and orphan exports exceed 1 MiB and retain every entry.
Standard output, standard error, exit status, payload bytes and the post-baseline
filename inventory match. Retention comparison excludes only the naturally
time-dependent `seconds_until_delete` field; persisted timestamps still match.
Current GC also records its own completed run without payload control objects.

This checks report compatibility, not verified chunk identities, reclamation,
production latency or unlimited workload size. The reproducible fixture is
`scripts/shardline/verify_maintenance_large_diagnostics.sh`.

## Saved-reply recovery preserves user objects

The PostgreSQL `remote_delete_reply_recovery` integration test passes against
an owned fixture bootstrapped with released 1.11.3 migrations. It now places
user-object bytes at both the acknowledged-delete key and the unresolved key.
After advancing past 64 invalid replies and settling the valid reply, both
objects remain byte-for-byte unchanged. Reprocessing the same saved reply
retires its evidence without deleting either user object. The missing-reply
attempt remains pending until the fixture explicitly settles it.

The original work pool is closed before recovery, and its old settlement call
still fails. This proves the tested recovery path consumes evidence rather than
replaying a user delete. Storage in this test is local; it does not establish
remote-request behavior or safety for uncovered publication paths. Targeted
strict Clippy and workspace formatting checks also pass.

## Operator guidance in maintenance history

Administrative history entries now include a plain-language `next_step` for
their recorded state. Completed entries distinguish the successful past run
from current readiness; uncertain entries require checking logs and effects
before repeating mutations. Corrupt entries retain their incident reference
and advise preserving evidence without disclosing an unverified owner.

The 18 task-tracking tests pass with an owned PostgreSQL fixture initialized
from released 1.11.3 migrations, without runtime skips. HTTP coverage checks
completed and reconciliation guidance while the server remains ready; corrupt
evidence coverage checks guidance and omitted identity fields. Strict server
Clippy across all features and targets, and workspace formatting, pass.
The field is added only to the administrative view: public library summaries
and stored signed records keep their existing formats.

The report endpoint also returns `next_step` from the same state-based guidance.
HTTP coverage checks that a failed startup sweep has matching list/report
guidance while readiness remains `ready`. An unfinished run with no saved
report remains `running`, returns no report and warns against a duplicate job.
All 32 task-tracking tests, including metadata and relocation tests, pass with
the owned released-migration PostgreSQL fixture and no runtime skips. Strict
server Clippy and workspace formatting pass after this extension.

## Legacy scope bindings during storage copy

Current payload copies now exclude signed scope bindings associated with a
recognized legacy manifest. Previously those bindings entered the payload copy
even when their manifests and run artifacts were excluded. Binding verification
uses the existing manifest and author key; damaged bindings and unrelated
objects retain their original inventory/copy behavior.

All 25 storage-migration tests pass. The new fixture creates a genuinely signed
binding for an unfinished run, checks dry-run and real-copy counts, preserves
all source bytes, and confirms that unrelated and damaged binding bytes still
copy. The partly retired legacy-history fixture now publishes its retirement
marker into object storage rather than modern SQLite metadata, matching the
scenario it claims to test. Strict server Clippy and formatting pass.
The full-feature CLI was rebuilt and the native released/current copy verifier
passes, including exact output/inventory comparison, byte-preserving legacy
relocation, released rollback and execute-only binary fallback.

This does not upgrade old binaries or identify every damaged legacy object.

The copy fixture also finishes its run, retains the exact manifest bytes in a
retirement marker and removes the live manifest. A subsequent copy still
excludes its signed scope binding and run artifacts, preserves all remaining
source bytes, and inserts no additional payload object. Verification uses the
signed manifest digest and pinned author key; a retirement marker never
authorizes a retry or a new completion claim. Damaged binding bytes retain the
original copy behavior even after this interruption.

All 25 migration tests and all 32 task-tracking tests pass for this extension.
The latter use the owned released-migration PostgreSQL fixture with no runtime
skips. Strict server Clippy and workspace formatting also pass.
The rebuilt full-feature CLI also passes the native released/current copy,
legacy relocation, rollback and execute-only fallback verifier.

## Saved delete replies during payload copy

Storage migration now recognizes saved delete replies by their verified
signature, namespace and content-addressed filename. Recognized replies remain
at the source and are excluded from payload inventory and writes. No metadata
connection or source mutation is added; unreadable or unrecognized objects
retain the original behavior, including list-only inventory without read access.
Control-object recognition now lives in the normal `evidence_inventory` module.

The real PostgreSQL recovery fixture passes with dry-run and actual local
copies. It excludes the verified reply, copies both user objects unchanged,
and preserves invalid bytes plus a signed reply under a mismatched namespace.
The original reply still exists and its pending attempt is still blocked after
copy. Recovery subsequently advances past the 64 invalid records and settles
the valid reply without deleting user objects. Released 1.11.3 migrations
bootstrap the fixture; the test runs without skipping.

All 25 migration tests, strict server Clippy and formatting pass. The rebuilt
full-feature CLI passes the native released/current copy, legacy relocation,
rollback and execute-only fallback verifier. Unsigned scan
cursors and other unrecognized objects retain legacy copy behavior; this does
not identify every control object or upgrade older binaries. Operators must
reconcile pending deletes against the original storage before switching targets.

The native S3 verifier now accepts an optional public signed-reply fixture as
its third argument. Generate it with the existing PostgreSQL recovery test by
setting `SHARDLINE_TEST_DELETE_REPLY_EXPORT` to an owned output path; this is
test tooling, not a product setting, and exports no private signing key.

Against an owned MinIO bucket, the rebuilt current CLI excludes the reply from
both a real copy and dry-run inventory using a source account with List/Get
permissions and no write permission. Payload bytes and the original saved
reply remain unchanged. A separate List-only account needs no new Get grant:
its dry run succeeds and matches released stdout and stderr, retaining the
old inventory count when evidence cannot be read. Baseline payload copying,
private metadata tracking, restricted temporary storage and released rollback
also pass. The PostgreSQL fixture runs without skipping; targeted strict
Clippy, shell syntax and formatting checks pass.

## Unavailable pending-delete inspection

The pending-delete endpoint now includes `inspection_notice` when its backend
or account cannot inspect markers. Existing fields remain unchanged. The local
unsupported response gives explicit guidance rather than leaving its empty
array to interpretation. Supported inspection returns a null notice.

The PostgreSQL HTTP fixture enables row-level security while two pending
attempts exist. Inspection returns unsupported with the notice and no object
entries; both markers remain intact. Disabling the policy restores the existing
settlement path. All 32 admin API tests pass with the owned fixture bootstrapped
from released 1.11.3 migrations, without skips. Strict server Clippy and
formatting pass. This preserves the access boundary and makes no health or
retry-authorization claim.

## Optimized existing-workload compatibility

The full-feature release build completed from production source `894aaf06`;
the version remains 1.11.3. The new verifier runs the repository's existing
full local benchmark matrix at concurrency 1, 32 and 128, with three iterations,
a 1 MiB asset, a 64 KiB mutation and 64 KiB chunk configuration. Each binary
uses fresh owned stores and an empty configuration with inherited runtime
settings removed.

All report schemas and non-performance counters match released 1.11.3,
including bytes, chunks, inventory and cache hits. Private filesystem locations,
measured latency, CPU timing, throughput and derived scaling ratios are excluded
from value equality, while their field names remain in the schema comparison.
Released and current fsck both read all nine current-generated stores and return
matching stdout/stderr. Each current fsck invocation records one completion in
SQLite metadata without adding task objects to the payload inventory.

One sequential run measured:

| Concurrency | Released wall seconds | Current wall seconds | Released peak RSS KiB | Current peak RSS KiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.08 | 0.08 | 34,668 | 33,896 |
| 32 | 0.48 | 0.51 | 339,076 | 392,704 |
| 128 | 1.45 | 1.43 | 671,964 | 653,480 |

These are fixture observations, not a statistically established performance
improvement, regression threshold or deployment SLO. The benchmark exercises
uploads, reads, reuse and cache behavior, not multi-node maintenance ownership
or large task-history inspection. Those remain separate acceptance checks.

Reproduce with optimized binaries using
`scripts/shardline/verify_maintenance_benchmark_compatibility.sh CURRENT RELEASED`.
It retains raw JSON, stderr, time/RSS observations, binary checksums and host
information in its owned diagnostic directory. Native execution, shell syntax
and whitespace checks pass.

## Optimized two-machine replacement and ownership loss

The cross-host verifier now hashes shared records from PostgreSQL task metadata
rather than assuming they are payload objects. It also checks metadata history
selection and rejects tracker objects in the payload bucket. PostgreSQL readiness
uses TCP so fixture setup cannot observe the temporary bootstrap socket.

The optimized full-feature binary passes all four modes against two disposable
Debian 13 KVM guests with different machine and boot identities:

- Node loss: a network partition keeps the existing database owner and its
  contender blocked. Killing the source guest releases that ownership; the
  independently admitted rebuild completes. Lost runs remain unresolved and
  their saved record bytes remain unchanged. Replacement startup reports ready
  with one Shardline process and no extra tracker directory.
- Rebuild connection loss: the source command is checked alive before its
  owning backend is terminated. It records failure rather than continuing via
  another session. The distinct waiting rebuild completes and removes the one
  stale fixture record.
- Repair connection loss: the same takeover preserves the expired hold's exact
  row bytes while the source repair fails and the waiting rebuild completes.
- GC-mark connection loss: the source mark fails after coordinator loss and the
  distinct waiting rebuild completes. This covers the metadata boundary, not
  a remote DELETE already issued.

Every mode keeps automatic history in metadata rather than payload objects.
Native execution, shell syntax and whitespace checks pass. Guests and isolated
backend volumes are removed on exit. These results do not establish protection
for every library caller, in-flight remote mutation or older GC binary.
# Optimized native reboot verification

The current optimized binary (production source `894aaf06`) passed
`verify_maintenance_vm.sh` against released 1.11.3 and the verified Debian 13
cloud image. The verifier now compares exact artifact keys and payload bytes
in the existing SQLite metadata table, rather than looking for task records in
payload storage.

The service unit generated by the released binary remained byte-for-byte
unchanged, with `ProtectSystem=strict`, `ProtectHome=yes`, `PrivateTmp=yes`,
`NoNewPrivileges=yes`, and the existing writable root. Ordinary GC completed
in that sandbox. Hard guest resets before mutation and after mutation/report
creation but before completion acknowledgement left both interrupted runs
unresolved. Their three published records stayed unchanged; the materialized
report and existing quarantine row survived unchanged. Inspection did not mark
an orphan introduced after reboot. An explicit fresh GC provided the positive
control that the earlier orphan was discoverable. The ordinary GC owner PID
matched the systemd service main PID during the completion interruption.

This proves these two local SQLite GC interruption cases, not every operation,
external delete acknowledgement window, or production deployment health.
Native log: `.codex/validation/ux-optimized-vm-reboot-verified.log`; disposable
fixture: `/tmp/shardline-maintenance-vm.eT1PrC`. The guest was stopped and its
overlay and private SSH key removed after the successful run.

## Actual metadata inventory and restricted accounts

The metadata inventory verifier now tests production automatic history instead
of inserting a row into a prototype table. It runs current fsck, checks one
manifest and a completed journal event, and compares released/current output.
Released fsck and released storage copy then preserve exact source artifact
keys and payload bytes. Only the synthetic payload is copied; neither source
nor destination gains task objects in its payload inventory.

The optimized binary also passes the existing local-permissions and
retention-permissions verifiers. Four ordinary commands (fsck, rebuild, repair,
GC) preserve released output/status under a 0555 root with an already-writable
metadata database. The isolated retention-only Postgres account retains its
existing permissions and both binaries reject unresolved deletes. These are
the verifier fixtures' permission cases, not every deployment account or RLS
policy.

Logs: `.codex/validation/ux-actual-metadata-inventory-verified.log`,
`.codex/validation/ux-optimized-local-permissions.log`, and
`.codex/validation/ux-optimized-retention-permissions.log`. The current optimized
binary uses production source `894aaf06`; released comparisons use 1.11.3.
The retention fixture's owned database container is removed on exit.

## Distinct login schemas and work-session routing

The current library test
`postgres_logins_can_target_distinct_default_schemas_with_identical_url_options`
passes against a fresh PostgreSQL 16 database bootstrapped by released 1.11.3.
`DATABASE_URL` is set to that owned fixture, so this result does not use the
test's no-database early return. The native log records one executed passing
test and zero ignored tests.

The fixture verifies distinct default schemas for logins with otherwise matching
connection options, stable owner-installed scope identities, actual work-session
binding without changing its backend PID, immutable association after a schema
change, and no replacement after the work pool closes. Restricted routers read
the appropriate legacy shared history without publishing new payload task objects.
Changing the durable backend identity changes its grouping even when endpoint
names match; removing the owner-installed function preserves startup and legacy
inspection. This does not cover every work-specific domain schema or library
write path.

Reproduce with
`bash scripts/shardline/verify_maintenance_schema_routing.sh RELEASED` from the
repository root. It creates and removes its own database container and invokes
the current library test; existing deployment URLs are not used. Set the normal
Cargo build directory/profile variables if reusing a validation build.

## Larger local history backlog

The metadata backlog fixture now covers both 130 and 1,024 finished runs, plus
one unfinished run and one unreadable manifest in each store. It checks complete
pagination without repeated IDs, page boundaries after a cursor's record is
deleted, oldest-first cleanup in batches of at most 64, exact preservation of
unreadable evidence and user payload bytes, and absence of leftover retirement
markers or task payload objects. An invalid later key still cannot hide earlier
eligible history.

The first unoptimized run passed both fixtures. The 1,026-run store took about
15.23 seconds for full paginated inspection and 62.12 seconds across 17 retention
calls; the 132-run store took about 1.87 and 4.68 seconds respectively. These
local unoptimized measurements show the cost of inspecting and repeatedly
scanning this backlog; they do not establish a production latency bound or
optimized performance. Log: `.codex/validation/ux-larger-history-backlog.log`.

The operator and admin API guides now state the existing retention policy:
verified finished runs created more than 30 days ago can be cleaned up at
startup or ordinary maintenance, at most 64 per invocation. Age is measured
from run creation, not completion. Interrupted and corrupt evidence remains
available and can accumulate; history is not a permanent audit archive.

The final rerun after checked arithmetic was added to the shared fixture also
passes both tests: 1,026-run inspection about 14.93 seconds and 17 cleanup calls
about 60.64 seconds; 132-run inspection about 1.87 seconds and four cleanup calls
about 4.96 seconds. Log:
`.codex/validation/ux-larger-history-backlog-verified.log`. Formatting and strict
all-feature/all-target server Clippy pass. No production retention behavior or
version was changed.

## Optimized remote-delete acknowledgement and outage drills

All seven `gc_acknowledgement` deployment chaos drills pass against the optimized
current binary (production source `894aaf06`) and the official released 1.11.3
binary for the restricted-account comparison. Each nextest process owns an
isolated PostgreSQL/MinIO/Redis Compose project and an instrumented proxy. The
final run executes seven tests, with no runtime `SKIPPED:` banner; its 16 skipped
tests are unrelated tests excluded by the selection filter.

Covered cases are acknowledged settlement, applied deletion with lost
acknowledgement and no replay, delayed deletion rejecting a later hold after
ownership takeover, the existing restricted GC account, denied optional reply
writes, ordinary repair after database outage, and fresh-sweep recovery after
database outage. Proxy counters verify one user-object mutation in each recovery
case. The pending block remains until verified evidence settles its exact
attempt. Repair then permits a hold without promoting the original GC's history
to a successful run.

The verifier now reads automatic history through a separate existing Postgres
pool on blocking threads. Its earlier payload-only reader was obsolete. When
Postgres is down at CLI exit, completion publication is unconfirmed: the command
warns and exits nonzero; the original run stays `running_or_interrupted` with no
fabricated terminal record. Recovering the saved reply cannot invent a task
outcome. The two outage fixtures need time for work-pool and optional metadata
timeouts, so they retain a finite 90-second fixture deadline; the normal cases
retain 25 seconds. This is a test deadline, not a production latency guarantee.

Native log: `.codex/validation/ux-optimized-gc-acknowledgement-verified.log`.
The seven drills finished in about 105 seconds total. Strict Clippy, formatting
and whitespace checks pass. Owned CLI containers and backend projects are removed
on exit. These results cover the native GC paths, not every library caller or
older GC's lack of the new delete-admission markers.

Reproduction: set `CARGO_BIN_EXE_shardline` to the current full-feature binary,
`SHARDLINE_N_MINUS_ONE_BINARY` to the official 1.11.3 binary, and run:

```bash
CARGO_BUILD_JOBS=2 cargo nextest run --locked -p shardline-server --all-features \
  --test deployment_chaos -E 'test(gc_acknowledgement::)' --test-threads 1 \
  --no-fail-fast --success-output immediate --failure-output immediate
```

Check for runtime skips as well as the test summary; an unavailable fixture is
not evidence that the mutation boundaries passed.

## Optimized container replacement and graceful shutdown

`drill_tracker_container_replacement_and_graceful_shutdown` passes with the
optimized current binary and isolated real PostgreSQL/MinIO/Redis backends.
The fixture blocks an existing LFS startup sweep, observes admission, preserves
the old PID namespace identity, and kills the original server container. A new
container with a fresh root becomes ready, records a distinct completed sweep,
and retains the old unresolved run with unchanged published records. The
replacement has one server process and no private tracker directory. SIGTERM
exits with status zero without forced kill; a third startup records new work
while both earlier completed and interrupted records remain unchanged.

The test uses the normal optimized binary's owner migration and reads automatic
history in metadata. Shared test-only inspection now lives in the conventional
`deployment_chaos::maintenance_history` module, using blocking execution for
synchronous history calls and a caller-owned inspection pool. No path override
or production process/configuration change is introduced.

The container drill and acknowledged-GC smoke drill both pass after this shared
reader refactor, with no runtime skips. Native log:
`.codex/validation/ux-optimized-container-replacement.log`. Strict test-target
Clippy, formatting and whitespace checks pass. Fixtures are removed on exit.
This covers these container startup and shutdown boundaries, not every operation
during connection drain or every external mutation.

## Current Postgres publication and delete guards

All 21 `postgres::remote_deletes::tests` pass against a newly created PostgreSQL
16 container. Released 1.11.3 bootstraps its database; the current tests use
isolated schemas and install the guards under test. `DATABASE_URL` is explicitly
set to the owned fixture, so the test support's no-database early return is not
used. Zero tests failed or were ignored; execution took about 7.93 seconds.
Native log: the fixture referenced by
`.codex/validation/ux-publication-guard-fixture`, under `test.log`.

These tests cover direct file versions, container/cache/chunk references,
dedupe inserts and identity updates, reconstruction and stored-object
registration. Pending deletes reject publication without replacing earlier
metadata or evidence. Concurrent publication holds its lock until transaction
completion even before any pending marker exists. A 20,000-chunk file exercises
the bounded publication-lock strategy. Restricted reference writers need no
new retention or fence-table privilege.

Settlement tests reject cross-schema admission, preserve an identical key/epoch
in another schema, retain uncertainty after connection replacement, and prevent
obsolete completion from clearing a newer attempt. They also cover unique
concurrent epochs, exhaustion before admission, safe schema quoting, and pending
inspection without new grants under row policies. A missing required retention
trigger disables delete admission while preserving ordinary holds.

This establishes the tested direct metadata boundaries, not references hidden
inside legacy payloads, arbitrary external stores, or callers that bypass the
bound work session. The fixture's container is removed after the test command.
Reproduction in a disposable database bootstrapped by the released binary:

```bash
CARGO_BUILD_JOBS=2 cargo test --locked -p shardline-index --all-features --lib \
  postgres::remote_deletes::tests -- --nocapture --test-threads=1
```

Set `DATABASE_URL` to that disposable fixture before running; without it the
database tests can return early and their summary alone is insufficient proof.

## Optimized upgrade and read-only deployment matrix

The mixed-version rollout fixture now checks the documented owner-upgrade
boundary. Before owner setup, current and released nodes read and overwrite the
same synthetic S3 data correctly, and current inspection returns an explicit
unavailable-history notice with no invented task result. The usual current
`db migrate up` installs optional history despite an already-current domain
ledger. After restarting the current node, the same data remains readable,
shared metadata history records its completed startup sweep, and released
rollback preserves reads. No new domain migration is required for this branch.

The read-only container fixture covers released/current fsck, rebuild and
non-mutating GC before and after owner setup. Pre-upgrade commands do not create
the optional history table or payload task objects. After setup, fsck and rebuild
save verified inline outcomes in Postgres while non-mutating GC creates no run.
A read-only S3 login also completes fsck with exact released stdout and saves a
third completed metadata result. The existing read-only root, unwritable `/tmp`,
non-root user and no-new-privileges restriction remain; no S3 grant is added.

The concurrent ordinary-rebuild drill also passes with the metadata reader.
Both commands remain unresolved while a fixture scan lock blocks the owner;
only one acquires the maintenance lock. After release, the first removes the
one stale record and the next independently finds zero; both record distinct
completed runs. This does not reuse the first scan as the second invocation.

Native log: `.codex/validation/ux-optimized-upgrade-readonly-matrix.log` records
all three executed tests passing with no runtime skips. The broader eleven-test
selection records ten passes and the old pre-owner-history assertion failure in
`.codex/validation/ux-optimized-maintenance-deployment-matrix.log`; the corrected
mixed-version test passes in the three-test rerun. Those ten include all seven
GC acknowledgement drills and container replacement. The selected-test counts
exclude unrelated tests, not unavailable backend fixtures.

Strict test-target Clippy passes. These are the tested default-schema native
deployment paths; they do not close every narrow-account, legacy-reference,
work-specific schema or production-scale gap. Production binary source remains
`894aaf06`, the released comparator is official 1.11.3, and version stays unchanged.

## Captured-output inspection boundary

The existing signed-outcome test now includes actual stdout and stderr chunks,
each 4 MiB. At exactly 8 MiB combined, inspection reads and verifies both SHA-256
digests and reports full output verification. A wrong stdout digest fails
verification. The earlier greater-than-8-MiB cases still exercise the bounded
inspection path for exit codes 0, 1 and 2, rejecting contradictory outcome
headers before returning an unverified-stream indication.

The targeted current library test passes in about 0.40 seconds, with one
executed test and no ignored tests. Log:
`.codex/validation/ux-output-inspection-boundary.log`. The admin API guide now
names the exact threshold and distinguishes optional isolated-runner captures
from ordinary live CLI output and diagnostic exports. The report endpoint's
separate 64 KiB read bound was already documented. This adds boundary evidence
and documentation; no runtime threshold, command behavior or version changes.

## Native client with PostgreSQL and MinIO

Run `bash scripts/shardline/verify_maintenance_native_xet.sh` to exercise the
native client against disposable PostgreSQL metadata and MinIO payload storage.
The fixture runs the ordinary owner migration, checks that the remote-delete
guard is installed, and exercises upload, deduplication, reconstruction and
download through the existing server library. Missing Docker or a runtime skip
fails this explicit verifier instead of counting as successful coverage.

The current fixture passed: one executed test, zero failures and zero ignored
tests, in 5.39 seconds. Evidence is in
`/tmp/shardline-native-xet.e46mHA/test.log` and
`.codex/validation/ux-native-xet-postgres-minio.log`. Strict Clippy for the native
client target also passed. The standalone E2E lockfile was reconciled with
existing workspace dependencies; package versions and manifests were unchanged.

This proves the covered native client flow with the owner-upgraded database.
It does not establish arbitrary remote fault handling, cluster-wide exactly-once
execution or production-scale performance. The deployment binary is validated
separately; this fixture starts the server through its public library API.

The optimized binary built from `144dfdc8` also passed all 11 selected deployment
drills in 150.057 seconds, with no runtime skips. Its SHA-256 is
`e65a0305d8dd421e13e201f80e1b3a0be3bcce25bee0776d28fa6d61d029cc57`;
the comparator is released 1.11.3. Log:
`/tmp/shardline-maintenance-deployments.Gcq12C/test.log`. Twelve other tests were
excluded by the explicit selection and are not part of this claim.

The fixture reserves PostgreSQL's host port alongside both server ports so it
stays stable across stop/start outage drills. MinIO and Redis use Docker-assigned
ports. An earlier run rejected a runtime port-allocation skip; a subsequent
fixture revision with an automatically assigned PostgreSQL port failed both
outage recoveries. The stable-port revision passed both unchanged recovery
checks and the full selected matrix. This corrects test infrastructure without
changing deployment configuration or product retry behavior. Formatting and
strict Clippy for the deployment target passed; the broader merge gates remain
open.

## Conditional cleanup preserves a concurrent publisher

The Postgres S3 loser-upload cleanup formerly read the latest hash and then
deleted the latest alias unconditionally in another transaction. A publication
between those steps could lose its acknowledged alias. Cleanup now repeats the
expected-hash condition in the transactional DELETE. A failed condition returns
false and preserves both the newer alias and the losing version for ordinary GC.
Successful cleanup removes the matching alias and version together.

A configured disposable PostgreSQL fixture publishes a loser, publishes a
different winner before cleanup, verifies the winner and losing version remain,
then verifies matching cleanup and an absent-alias retry. It passed one executed
test with no ignored tests in 0.25 seconds. Log:
`.codex/validation/ux-conditional-cleanup-test.log`. Strict all-target Clippy for
the index and server passed. This closes that specific race, not every remaining
write-path or deployment-scale gate. No migration, configuration or version
change is required.

The same regression fixture now also holds the winner's publication transaction
open while cleanup starts on another connection. It observes the actual wait
through `pg_blocking_pids`, commits the winner, and verifies that the waiting
cleanup returns false and preserves the winner's bytes and losing version.
This tests PostgreSQL's predicate recheck after a row-lock wait without relying
on a sleep to establish overlap. The strengthened fixture passed in 0.31 seconds
with one executed test and no ignored tests; log:
`.codex/validation/ux-conditional-overlap-test.log`.

After the conditional-cleanup fix and overlapping-transaction regression, the
full `postgres::` index library selection passed against a disposable configured
PostgreSQL database: 185 executed tests, zero failures and zero ignored tests,
in 37.94 seconds. Log: `.codex/validation/ux-current-index-tests.log`.
The fixture was bootstrapped using the owner command before isolated test schemas
were created. This includes record publication, delete protection, receipt
recovery, artifact permissions and schema-specific index tests; it does not
replace optimized process-level deployment verification of the new binary.

The optimized binary subsequently built from `92ef6c27` passed all 11 selected
deployment drills in 159.971 seconds, with no runtime skips. Twelve tests outside
the explicit selection were excluded. Log:
`/tmp/shardline-maintenance-deployments.txewFw/test.log`. Binary SHA-256:
`f81ef6c2afc9757b0848a2cee088a2f2c5516891aa8b40163da0629085f45a21`.
It still reports version 1.11.3 and includes the conditional-cleanup fix.

The same binary also passed the restricted released/current OCI and S3 process
drill: `.codex/validation/ux-current-oci-upgrade.log`, diagnostics
`/tmp/shardline-oci-upgrade.h7Lga7`. Existing grants and the released ledger were
preserved; lost ownership, publication failure, SIGKILL and fresh retry were
checked, and ordinary servers had no child processes. Both runs used the
official released 1.11.3 comparator. These results extend the tested deployment
coverage; remaining library write paths and the other open merge gates still
require their own evidence.

## File-reference copy keeps its request ownership session

Postgres file-reference copy now publishes through the request's existing
ownership connection when present. Its record reads also borrow that connection,
avoiding a second-pool acquisition when the request owns the only connection.
Connection loss fails the operation rather than replacing its owner; callers
outside an owned request retain the existing pool-based behavior. Record decoding
uses the existing bounded conversion admission and canonical serialization.

A configured PostgreSQL fixture verifies a successful owned copy with one
connection, terminates that actual database backend, verifies the subsequent copy
fails with no destination metadata, and completes a fresh copy and duplicate
check after releasing the lost request. The individual regression passed in
0.25 seconds. All six publication tests then passed in 0.94 seconds with no
ignored tests. Logs: `.codex/validation/ux-copy-owner-test.log` and
`.codex/validation/ux-copy-publication-tests.log`. Strict all-target Clippy for
the index and server passed. No configuration, migration or version change was
made. The optimized process results above predate this session-routing change;
they must not be treated as verification of this revision.

File-reference deletion now follows the same ownership rule for both ordinary
deletion and expected-hash cleanup. The existing pool-based index methods retain
their signatures and delegate to connection-taking variants. No ownership
connection is replaced after loss, and the conditional DELETE still rechecks the
hash atomically.

The one-connection fixture now also verifies successful ordinary and conditional
deletion, an absent-reference result, and rejection of both delete paths after
terminating the actual request owner. The source remains readable afterward.
All six publication tests passed in 0.98 seconds and all eleven index record-store
tests passed in 0.40 seconds, with zero ignored tests. Logs:
`.codex/validation/ux-delete-owner-publication-tests.log` and
`.codex/validation/ux-delete-owner-index-tests.log`. Strict all-target index/server
Clippy and the AST graph refresh passed. Optimized deployment validation still
needs to include these session-routing changes.

## Guarded reads avoid ownership-mutex re-entry

The initial session-routing change applied to ordinary record reads as well as
copy/delete. Review found that a protocol resource guard can already hold the
request connection's mutex when it performs a read. Ordinary reads now retain
their existing record-pool path; only the copy/delete preliminary reads use the
ownership-aware helper. Locator validation is shared by both paths.

A real PostgreSQL regression holds the actual request resource guard and reads
through the separate record pool, with one connection per pool and a two-second
deadline. All seven publication tests passed in 0.94 seconds, including that
test and the copy/delete owner-loss fixture. Log:
`.codex/validation/ux-guarded-read-tests.log`. Strict all-target server Clippy
passed. The already-running optimized deployment job uses the previous binary;
its results cannot establish this adjustment's process-level behavior.

The pre-adjustment optimized binary passed its 11 selected deployment drills
in 146.378 seconds, but the separate OCI/S3 drill timed out at the expected
publication-failure response (script line 246). Logs:
`/tmp/shardline-maintenance-deployments.YZA4Xk/test.log` and
`.codex/validation/ux-session-oci-upgrade.log`. That failure is retained as
evidence; the deployment selection did not cover this guarded-read regression.

After the guarded-read correction, a fresh optimized build from `4aa579c2`
passed the complete restricted released/current OCI and S3 process drill,
including publication failure, lost ownership, SIGKILL and retry. Existing
grants and the released ledger were preserved, and ordinary servers had no child
processes. Log: `.codex/validation/ux-guarded-oci-upgrade.log`; diagnostics:
`/tmp/shardline-oci-upgrade.qo1DDX`. Binary SHA-256:
`d10fb16ac40e733c51bd694037dc4e2abd075ea4f68c1782be2ec638284a2fde`.
The binary still reports 1.11.3. The corrected binary's 11-drill deployment run
is separate from the earlier result and must be recorded after it completes.

That corrected binary's deployment run has now completed successfully: all 11
selected drills passed in 146.169 seconds with no runtime skips. Twelve tests
outside the explicit selection were excluded. Log:
`/tmp/shardline-maintenance-deployments.7MnPHQ/test.log`; wrapper log:
`.codex/validation/ux-guarded-deployments.log`. This extends the optimized
OCI/S3 result above with mixed-version rollout and rollback, read-only commands,
concurrent rebuild ownership, interrupted tracking, graceful replacement and
remote-delete recovery. Both process checks used the corrected `4aa579c2` binary
and official released 1.11.3. The remaining merge gates still require evidence
beyond these selected fixtures.

The current source also passes the real native-client PostgreSQL/MinIO verifier
after the guarded-read adjustment: one executed test, no ignored tests or runtime
skips, in 5.40 seconds. Log: `.codex/validation/ux-guarded-native-xet.log`;
diagnostics: `/tmp/shardline-native-xet.6xG98p/test.log`. This extends the current
process checks with native client upload, deduplication, reconstruction and
download through the server's public library API. It does not establish
PostgreSQL history-capacity performance or arbitrary deployment layouts.

## Full PostgreSQL automatic-history capacity

Run `bash scripts/shardline/verify_maintenance_postgres_history_capacity.sh CURRENT`
for an explicit optimized shared-history fixture. It creates its own PostgreSQL
container, bootstraps it through the usual owner command, and uses a private
schema through the server's existing metadata pool. Missing configuration,
runtime skips or an unexpected test count fail the verifier.

The fixture created 4,094 real completed runs, one interrupted run and one
unreadable manifest. New admission failed at 4,096 slots. Full pagination returned
every ID once. Sixty-five cleanup calls removed exactly the completed runs in
batches of at most 64, retained both unresolved records, preserved user payload
bytes and kept task artifacts outside payload inventory. Fresh recording worked
after cleanup reclaimed slots.

One executed optimized test passed with zero ignored tests in 379.76 seconds.
Creation took 82.173 seconds; full paginated inspection took 37.073 seconds;
cleanup calls totaled 260.310 seconds, with a slowest call of 5.961 seconds.
Log: `/tmp/shardline-postgres-capacity.8CSN3l/test.log`; verifier output:
`.codex/validation/ux-pg-capacity.log`. Strict test-target Clippy, shell syntax,
diff checks and the AST graph refresh passed.

These are measurements from a local disposable database, not latency bounds or
evidence about larger legacy archives. The cleanup cost remains relevant to the
open performance gate; logical admission limits do not cap physical database
size. This change adds a test and reproduction script, with no runtime, version
or deployment-configuration change.

### Bounded proof and report reads

History verification now uses bounded metadata pages for referenced blobs and
materialized reports as well as native records. Hashes, signatures, semantic
verification, individual size limits and conflicting legacy-copy checks still
apply. Each reader holds at most 64 bodies of at most 64 KiB; records retain
their smaller 16 KiB decoding limit. No evidence format or operator setting changes.

The new local regression preserves oversized blobs and reports, damaged reports and
conflicting legacy blobs as corrupt histories, while retiring the healthy run.
The broader history suite initially reported 24 passes, two explicit capacity
tests ignored and one stale operator-guidance assertion. The assertion was
updated to the existing guidance that a live process does not confirm completion;
its separate rerun passed. Strict library/test Clippy passed.

The first optimized PostgreSQL run passed the capacity assertions in 362.75
seconds: creation 89.388 seconds, inspection 36.007 seconds, cleanup 237.165
seconds and slowest cleanup 4.437 seconds. Its log also contains a Tokio shutdown
panic after the workload, so it is not clean validation. The fixture now calls
its explicit cleanup method, and the verifier rejects panic messages even when
Cargo reports success.
First-run log: `/tmp/shardline-postgres-capacity.6Cb5LL/test.log`.

The corrected optimized run passed cleanly: one executed test, no ignored tests,
runtime skips or panic messages, in 350.35 seconds. Creation took 81.508 seconds,
full inspection 35.159 seconds, and 65 cleanup calls totaled 233.435 seconds;
the slowest call took 4.538 seconds. All capacity, pagination, unresolved-evidence
and payload-preservation assertions passed, and explicit fixture cleanup finished.
Log: `/tmp/shardline-postgres-capacity.Mngbsc/test.log`; verifier output:
`.codex/validation/ux-batched-artifacts-pg-capacity-clean.log`. The final expanded
local blob/report fault test, strict library/test Clippy, shell syntax, diff checks
and AST graph refresh also passed.

These measurements do not establish production latency or a guaranteed speedup.
The scale gate remains open.

### Batched retirement deletion

Retirement now deletes up to 64 PostgreSQL artifact keys in one transaction on
one checked connection. It calls the existing private deletion function with
the existing permissions; it adds no schema change or runtime grant. Legacy
copies are removed before their metadata batch, the expired manifest is hidden
first, and the retirement marker and signed binding remain until cleanup finishes.
Local-only deletion retains its previous behavior.

Eight artifact tests passed against a configured disposable PostgreSQL database,
including caller rollback, a failure on the last of 64 deletions, rejection of
65-key batches, idempotent deletion and preservation of a 17-second caller timeout.
The restricted-account fixture permits batching with existing GC permissions and
rejects read-only and publication-only accounts. Log:
`/tmp/shardline-batched-deletion.Lq0Dga/test.log`.

A separate server fixture added 130 artifacts with duplicate legacy copies and
injected a second-batch deletion failure. Exactly the first 64 metadata artifacts
remained deleted, the failed batch rolled back, and the marker and binding survived.
A fresh call removed the remaining evidence without changing user payload.
One executed test passed in 0.94 seconds; log:
`/tmp/shardline-batched-deletion.ZI63K0/test.log`. The existing PostgreSQL history
module also passed all 15 configured tests in 9.62 seconds, including interrupted
retirement authorization and schema routing; log:
`/tmp/shardline-batched-deletion.DhKAvS/test.log`.

The optimized full-capacity fixture passed cleanly in 327.11 seconds: creation
82.784 seconds, inspection 33.903 seconds, and 65 cleanup calls totaling 210.186
seconds, with a slowest call of 4.180 seconds. All capacity and payload-safety
assertions passed. Log: `/tmp/shardline-postgres-capacity.gGST2u/test.log`;
verifier output: `.codex/validation/ux-batched-deletion-pg-capacity.log`.
Strict index/server library and test Clippy and diff checks passed.
These measurements leave production latency and larger legacy archives open.

## Optional history cleanup at shutdown

Normal serving now signals optional history housekeeping to stop when shutdown
starts and when serving finishes. Public-library routers also stop it when the
last router guard is dropped. The stop flag belongs only to the housekeeping
store clone; request ownership, actual maintenance jobs and ordinary CLI history
keep their existing behavior. Public `AppState` fields and router signatures
remain unchanged. No service, process or operator setting is added.

Cancellation is cooperative: scans and evidence I/O check it between operations;
an already running storage call can finish. Partial retirement remains resumable.
This does not establish a fixed shutdown deadline for arbitrary storage adapters.

Three local fault tests passed in 0.12 seconds: dropping the last guard stops only
its housekeeping store, a scan stops after its first visited item without changing
the inventory, and a cancelled partial retirement preserves its marker for a
fresh store. The real router lifetime test, authenticated startup/status fixture
and existing signal-relative shutdown-timeout test also passed. Logs:
`.codex/validation/ux-housekeeping-shutdown-tests.log`,
`.codex/validation/ux-router-housekeeping-shutdown-tests.log`,
`.codex/validation/ux-housekeeping-router-status-tests.log` and
`.codex/validation/ux-housekeeping-existing-shutdown-tests.log`.
Strict library/test Clippy, formatting, diff checks and AST graph refresh passed.

The final optimized CLI reports 1.11.3 and has SHA-256
`65221eb7c90328b6449f86c5638e7beb8daa632e90b65f638e5f40e8d1a3767d`.
Its restricted released/current OCI/S3 process drill passed owner loss,
publication failure, SIGKILL, fresh retry and released rollback with the ledger
and grants unchanged. Ordinary servers had no child processes at readiness and
completion. Fixture logs contain no matching Tokio shutdown panic. Diagnostics:
`/tmp/shardline-oci-upgrade.pjXI8A`; verifier output:
`.codex/validation/ux-housekeeping-oci-upgrade.log`.

## Administrative history pagination and warnings

Two HTTP regressions use the public local router and existing admin-token
authentication. One follows every returned cursor with a one-entry page, verifies
that unreadable manifests remain visible as corrupt entries without trusted
identity fields, and checks that unsupported history filters return HTTP 400.
The other simulates interrupted retirement and imports unreadable retirement
markers: an empty history response still warns that recording is full, while
the independent readiness endpoint remains ready.

Both tests passed in 0.08 seconds with no ignored tests. Log:
`.codex/validation/ux-http-history-pagination-tests.log`.
These fixtures verify the HTTP contract for local metadata; they do not establish
PostgreSQL pagination or automatic-admission capacity. The capacity fixtures
above cover the latter separately.

The configured PostgreSQL router fixture also follows the HTTP cursor at full
history capacity. The next page retains the recording warning and shows a corrupt
entry without trusted owner or operation fields. All 15 PostgreSQL metadata tests
passed in 9.60 seconds, including account row policies, immutable schema routing
and shared-scope retirement. Diagnostics:
`/tmp/shardline-batched-deletion.468Wtt/test.log`; verifier output:
`.codex/validation/ux-pg-http-pagination-tests.log`.
This extends the real-database HTTP evidence to a second page; exhaustive
pagination remains covered by the separate capacity fixture.

## Automatic embedded report boundary

A local metadata regression records a 65,536-byte report successfully, then
rejects a 65,537-byte report for another run. The rejected run records a need for
reconciliation, has no available report or oversized artifact, and preserves the
earlier verified completion and application payload. No task objects enter the
payload inventory. One test passed in 0.07 seconds; log:
`.codex/validation/ux-report-limits-tests.log`.
This verifies optional embedded report admission and truthful failure at its
boundary. It does not establish larger legacy-report cost, remote storage faults
or physical disk limits. Ordinary command diagnostics retain their own contract.

## Deployment rerun after housekeeping shutdown changes

The final housekeeping CLI (SHA-256
`65221eb7c90328b6449f86c5638e7beb8daa632e90b65f638e5f40e8d1a3767d`)
passed all 11 selected deployment drills in 158.009 seconds. They cover mixed
released/current rollout and rollback, lost and delayed remote-delete replies,
existing GC permissions, denied optional receipt writes, database-outage recovery
through repair and sweep, concurrent rebuild ownership, read-only maintenance and
container replacement with graceful shutdown. Twelve other tests were excluded
by the selection expression; none of the selected drills skipped at runtime.
The verifier also rejects background panic messages, including panics emitted
after otherwise successful assertions. Diagnostics:
`/tmp/shardline-maintenance-deployments.P4mT9e/test.log`; verifier output:
`.codex/validation/ux-final-housekeeping-deployments.log`.

A separate current-source schema-routing drill passed against a database
bootstrapped by released 1.11.3: one test in 0.91 seconds, with no skip or panic
messages. Diagnostics: `/tmp/shardline-schema-routing.85Mkzb/test.log`;
verifier output: `.codex/validation/ux-final-schema-routing.log`.
Both verifiers now reject skip and background panic messages and retain their
exact expected test-count checks. Shell syntax and diff checks passed.
These results cover the selected fault scenarios and do not close the broader
library, legacy-reference or scale gaps in the acceptance checklist.

## Released library publication boundary

Compared the repository's `v1.11.3` source with current
`crates/shardline-index/src/postgres/record_store.rs`. The released
`commit_native_shard_metadata` begins a database transaction, writes version
records, dedupe mappings and latest aliases, and commits. It receives no object
store and performs no backing-content inspection. The current compatibility
method retains its signature and adds the explicit-reference implementation;
the server separately validates backing storage on its ownership connection.

This comparison establishes that metadata-only library publication predates
the integration. It does not prove that every current caller retains released
behavior, that legacy physical references are completely discoverable, or that
arbitrary library publishers receive the server's stronger storage guarantees.
The API documentation now states these boundaries explicitly. Reproduction:
`git show v1.11.3:crates/shardline-index/src/postgres/record_store.rs`.

## Quiet fallback on restricted local roots

Rerunning the mode-0555 root comparison with the final housekeeping binary
exposed a stderr mismatch: private-scratch admission warned despite successful
shared metadata recording. The failed comparison is retained at
`/tmp/shardline-local-permissions.C9eHZs`; output:
`.codex/validation/ux-final-local-permissions.log`.

Private-scratch fallback now logs at debug level. Shared recording failures and
actual cleanup failures still warn. All six automatic-tracking tests passed in
21.07 seconds, covering unusable private storage, capacity and retained evidence;
strict CLI library/test Clippy, formatting, diff checks and graph refresh passed.
Test log: `.codex/validation/ux-quiet-fallback-tests.log`.

The corrected optimized CLI has SHA-256
`8a9b22310b2c5314d955900c8f9cd5d7200a548b5773bd1bc538f3e9f2b9c2e5`.
Its restricted-root fixture matches released stdout, stderr and exit status for
fsck, rebuild, lifecycle repair and GC mark, saves four metadata histories, and
creates no private root entries or task payload objects. Diagnostics:
`/tmp/shardline-local-permissions.fvhMkV`; verifier output:
`.codex/validation/ux-quiet-fallback-local-permissions.log`.
The cleanup-failure process drill also passed with this binary: the real removal
fault warns, preserves the command's success and receipt bytes, and a fresh run
cleans the leftover scratch without replaying the original job. Released rollback
remains compatible. Diagnostics: `/tmp/shardline-cleanup-failure.CH81A3`;
verifier output: `.codex/validation/ux-quiet-fallback-cleanup-failure.log`.
These local fixtures require writable existing SQLite/WAL sidecars; they do not
prove availability with fully read-only metadata.

## Explicit retention-writer grant comparison

The restricted PostgreSQL retention-writer process fixture passed with the
corrected optimized CLI above and released 1.11.3. It now compares sorted explicit
table grants before and after the usual owner upgrade and asserts that the writer
has no direct SELECT/INSERT/UPDATE/DELETE privileges on the fence or task-artifact
tables. Both binaries can maintain holds with the existing credentials, reject a
fixture pending delete without publishing a hold, and work again after removal
of that marker. No remote delete was sent; manual removal is confined to that
disposable fixture and is not an operator recovery procedure.

Diagnostics: `/tmp/shardline-retention-permissions.jFWv4Z`; verifier output:
`.codex/validation/ux-final-retention-permissions.log`. Shell syntax and diff
checks passed. This proves the named retention-writer permission boundary; full
GC accounts and row-policy history behavior have their separate fixtures above.

## Current-source native client verification

The native Xet client fixture passed against current source with disposable
owner-upgraded PostgreSQL and MinIO: one executed test in 6.20 seconds, no ignored
tests in the result and nine tests excluded by the exact selection. It covers
upload, deduplication, reconstruction and download. The verifier rejects runtime
skips and background panic messages. Diagnostics:
`/tmp/shardline-native-xet.nebTlC/test.log`; verifier output:
`.codex/validation/ux-current-native-xet.log`.

The SQLite full-capacity verifier now rejects those messages too; its existing
exact count and capacity-marker checks remain. Shell syntax and diff checks
passed for both scripts. The capacity fixture was not rerun for this log-check
change; its recorded scale measurements above remain the relevant evidence.
The native fixture builds the current library source, rather than executing the
optimized CLI. It does not prove every legacy layout or deployment health.

## Startup and operator gate audit

Reviewed normal router initialization, the public embedding fixture, operator
state guidance, administrative routes and the simplified operator guide against
gates 1 and 8. The current public-library startup fixture passed in 0.24 seconds:
all three roles run fresh sweeps twice, retain distinct completed history with the
current PID, preserve the embedding application's logger, and introduce no private
tracking directory or task payload objects. Log:
`.codex/validation/ux-gate-startup-embedding.log`.

Seven selected current-source HTTP tests passed in 0.709 seconds. They verify all
role status routes, degraded readiness, failed startup diagnostics, corrupt
manifests, full-history warnings, cursor behavior and explicitly unsupported local
pending-delete inspection. No runtime skips or panic messages were observed;
3,366 unrelated tests were excluded by the selection expression. Log:
`.codex/validation/ux-gate-operator-status.log`.

A configured real-PostgreSQL pending-delete HTTP fixture passed in 0.19 seconds.
It checks long-key pagination, no-store responses, recovery guidance and unchanged
markers, including unavailable inspection under a restrictive row policy.
Diagnostics: `/tmp/shardline-batched-deletion.S6Ue6p/test.log`; verifier output:
`.codex/validation/ux-gate-pending-delete-http.log`.

Together with the configured PostgreSQL history and process drills above, these
checks establish the normal startup mechanism and operator response contract.
Remaining deployment-layout coverage keeps gate 1 open; verified operator
distinctions do not close the separate mutation safety or scale requirements.

## Current optimized large diagnostics

The corrected optimized CLI passed the released/current 4,096-entry mark-only GC
diagnostic comparison. Both exports exceed 1 MiB, preserve every persisted entry,
and match released diagnostic content apart from the documented live countdown.
Stdout, stderr and exit status match; payload hashes remain unchanged and current
GC records its own completion outside the payload inventory. Diagnostics:
`/tmp/shardline-large-diagnostics.fI4KaS`; verifier output:
`.codex/validation/ux-current-large-diagnostics.log`.

Source inspection confirms ordinary inline maintenance calls the original handler
without the explicit isolated runner's command-stream capture. Automatic history
artifact limits therefore do not truncate the command's output or exports. The
operator guide now states that distinction. This mark-only synthetic inventory
fixture does not establish reclamation safety or production latency.

## Fresh ordinary GC discovery after completion

Extended the optimized large-diagnostic process fixture beyond receipt counting.
After the released/current comparison and first current completion, it adds a new
synthetic orphan and runs the same ordinary GC mark command again. Both exports
then contain 4,097 marked entries, the history has two completed runs, and all
payload hashes remain unchanged. The fixture passed with the corrected optimized
CLI. Diagnostics: `/tmp/shardline-large-diagnostics.S9LzVt`; verifier output:
`.codex/validation/ux-fresh-gc-diagnostics.log`.

Four current CLI configuration tests also passed in 0.02 seconds. They verify
parallel configuration isolation, restoration after error/unwind, retained
admitted commands despite configuration-file replacement, and pinned migration
endpoints. The unwind test intentionally catches its own panic; that diagnostic
is expected and is not a background teardown panic. Log:
`.codex/validation/ux-gate-command-configuration.log`.

Source review confirms the normal automatic path allocates a new identity and
calls the original handler; completed history is not an execution cache. This
establishes fresh discovery for the tested mark-only inventory. Concurrent
ownership retains the separate named deployment evidence above; synthetic orphan
marking does not establish safe reclamation or every legacy reachability layout.

## PostgreSQL startup across all server roles

A new configured PostgreSQL public-router fixture covers All, API and Transfer
roles. Before each startup it creates a newly expired LFS session; each role
removes that session, reports readiness and exposes shared metadata history with
embedded execution and the current PID. History accumulates three distinct
completed runs and preserves earlier IDs across role changes. No task payload
objects are created.

All 16 configured PostgreSQL metadata tests passed in 10.33 seconds, including
this named fixture, row-policy failures, scope routing and retirement recovery.
Diagnostics: `/tmp/shardline-batched-deletion.I7sTER/test.log`; verifier output:
`.codex/validation/ux-postgres-all-role-history.log`.
This fixture uses PostgreSQL metadata with the existing local object adapter;
MinIO/native-client and process fixtures provide separate remote-storage evidence.
It does not establish every backend/frontend/role combination or legacy layout.

## Publication versus history retirement

Found a PostgreSQL automatic-history race during the scale audit: the bounded
publisher checked manifest existence without retaining its row, so concurrent
retirement could hide it before the artifact publication committed. Exact retries
also needed to retain their existing artifact row through verification and
publication. The SQLite immediate transaction already serializes these writes.

The bounded PostgreSQL publisher now uses row key-share locks for both checks.
Retirement waits for the existing caller transaction to finish; if retirement
wins first, a new artifact cannot pass manifest admission. Formats, grants and
public signatures are unchanged. These are history-storage locks, not authority
to execute application work or settle remote deletes.

A configured transaction regression failed against the original SQL because
manifest deletion succeeded before publication committed:
`/tmp/shardline-batched-deletion.3VSKX4/test.log`. With the fix, the test verifies
that manifest deletion and exact-retry artifact deletion hit the caller's
100-ms timeout while publication is open, then verifies successful cleanup and
rejection of a new artifact after manifest removal. All nine artifact tests
passed in 0.90 seconds; log: `/tmp/shardline-batched-deletion.0QdrxM/test.log`;
verifier output: `.codex/validation/ux-publication-retirement-race-final.log`.

The reverse ordering is also tested with a real open retirement transaction.
New publication hits its 100-ms timeout while manifest deletion remains
uncommitted, then refuses admission after retirement commits. All nine artifact
tests passed in 0.93 seconds with this extended regression; log:
`/tmp/shardline-batched-deletion.yTVocc/test.log`. Strict index library/test
Clippy and formatting passed. This extends the transaction-ordering evidence;
it does not establish a deployment latency bound.

The current full `postgres::` index library selection also passed against a
fresh disposable PostgreSQL database: 187 executed tests, zero failures and
zero ignored tests, in 38.40 seconds. Log:
`/tmp/shardline-batched-deletion.TFtiop/test.log`. The verifier rejected runtime
skip and panic messages. This covers the selected index metadata operations
with the publication/retirement fix and both transaction orderings; it does
not replace process-level mixed-node testing or close the scale gates.

The optimized CLI with SHA-256
`122afac40a21237a7e19f68f52c95bf576c04c16bcac5fbfa13ac66711ba1ad2`
subsequently passed all 11 selected deployment drills in 143.814 seconds.
Log: `/tmp/shardline-maintenance-deployments.JJZBlo/test.log`. Twelve tests
outside the explicit selection were excluded; no selected test skipped at
runtime and no panic was logged. The selection covers restricted maintenance,
remote-delete faults, concurrent ownership, container replacement and mixed
released/current rollout and rollback. These named fixtures extend the current
binary's evidence without certifying every deployment layout.

Cross-account retention's legacy-object checks now observe the housekeeping
stop flag before each identity lookup and at each returned inventory entry.
Previously this direct object-store scan bypassed the ordinary stop checks.
All three housekeeping tests passed in 0.11 seconds and all 16 configured
PostgreSQL history tests passed in 9.27 seconds; PostgreSQL log:
`/tmp/shardline-batched-deletion.E5Norp/test.log`. Strict server library/test
Clippy, formatting and graph update passed. These checks do not establish
cancellation of an active storage call or a measured shutdown latency bound.

Legacy relocation now also checks shutdown before classifying each artifact,
before reading a scope binding and before removing a verified archived copy.
This includes oversized entries that previously returned early without a stop
check. The new regression supplies an oversized inventory entry after shutdown,
verifies an interrupted result and unchanged legacy evidence, then verifies
that an ordinary fresh store resumes relocation. All six relocation tests
passed in 0.43 seconds; strict server library/test Clippy and formatting passed.
This is cooperative cancellation, with the same active-storage-call limit.

The complete `app::admin_api::tests::` selection subsequently passed with a
fresh configured PostgreSQL fixture: 32 executed tests, zero failures and zero
ignored tests, in 0.63 seconds. Log:
`/tmp/shardline-batched-deletion.2UzcRv/test.log`. The verifier found no runtime
skip or panic messages. This includes disabled-route concealment,
authentication before query validation, read-only responses, security headers,
all runtime roles, concurrent polling, maintenance report verification and
PostgreSQL pending-delete pagination without modifying markers. These tests
validate the documented operator API; they do not certify cluster health.

The complete `task_tracking::` server library selection then passed with
PostgreSQL configured: 57 executed tests, zero failures, and two explicitly
ignored full-capacity release-mode drills, in 98.88 seconds. Log:
`/tmp/shardline-batched-deletion.kHTf09/test.log`. No runtime skip or panic
messages appeared. The executed selection includes both shutdown fixes,
relocation, signed outcome/report verification, legacy fallback, PostgreSQL
scope binding and retention. Its 1,026-run local metadata fixture inspected all
pages in 12.595 seconds and cleaned eligible evidence in 17 calls totaling
53.252 seconds, with a slowest call of 4.143 seconds. Unfinished and corrupt
evidence remained intact. These debug-fixture timings are not deployment
latency bounds; the two full-capacity drills are not results of this run.

SQLite retention now removes at most 64 exact artifact keys in one immediate
transaction instead of opening and committing a connection for each artifact.
The retirement path still deletes legacy copies first and preserves its marker
on failure. A trigger-induced second-key failure verifies whole-batch rollback;
tests also cover unselected evidence, missing tables, duplicates and oversized
batch rejection. All eight SQLite artifact tests passed in 0.38 seconds after
the final checked-count adjustment. Strict index/server library/test Clippy,
formatting and graph update passed.

The combined tracking selection with batched retirement passed 57 tests in
113.43 seconds, with the same two explicit capacity exclusions; log:
`/tmp/shardline-batched-deletion.LFVfq3/test.log`. Its 1,026-run local fixture
took 14.676 seconds for inspection and 60.534 seconds for 17 cleanup calls,
with a slowest call of 4.613 seconds. This run overlapped compilation and graph
work; it does not demonstrate an overall speedup over the preceding run.
Atomicity and fewer SQLite commits are established; broader cost remains open.

The current release-mode full SQLite capacity drill then passed with all 4,096
automatic slots occupied. It verifies admission rejection, complete pagination,
oldest-first cleanup, preservation of unfinished/corrupt history and unchanged
user payload bytes. Full inspection took 6.264 seconds; 65 retention calls
totaled 36.619 seconds, with the slowest call at 0.690 seconds. The explicitly
selected capacity test passed in 76.20 seconds with no ignored tests, runtime
skips or panic messages. Log:
`/tmp/shardline-history-capacity.TeWlaN/test.log`. This validates full-capacity
behavior after SQLite batching, not a production latency bound or a controlled
performance comparison. Larger legacy archives and unresolved-history growth
retain the limits described in the acceptance checklist.

The SQLite end-to-end retirement regression now also injects failure in the
second batch after the first 64-key batch commits. It verifies exact retained
marker bytes, absence of the hidden manifest, preservation of every later
metadata row and unchanged payload bytes. Removing the fault and reopening a
fresh evidence store completes retirement without creating another job.
`task_tracking::tests::local_retention::interrupted_sqlite_batches_preserve_authorization_and_resume`
passed in 0.35 seconds. Strict server library/test Clippy, formatting and graph
update passed. The test lives in a normal `local_retention` module.

The current PostgreSQL release-mode 4,096-slot capacity drill passed after the
publication/retirement locking fix: one executed test, zero failures and zero
ignored tests, in 329.61 seconds. Creation took 80.217 seconds, complete
inspection 35.631 seconds and 65 cleanup calls totaled 213.506 seconds, with a
slowest call of 4.384 seconds. Log:
`/tmp/shardline-postgres-capacity.QLcz16/test.log`. Admission, pagination,
unresolved/corrupt evidence, payload preservation and fresh work after cleanup
passed; no runtime skip or panic was logged. CLI compilation overlapped part of
this run, so these fixture timings are not a controlled performance comparison.

## Preserve ordinary configuration failures

A released/current comparison found that malformed TOML produced the original
error twice: automatic preparation added a tracking warning before the original
handler reported the configuration error. Baseline fixture:
`/tmp/shardline-invalid-config.aQxaG6`. Preparation failures now log at debug
level and defer to the original handler. They have not started application work
and are distinct from genuine history-recording failures, which still warn.

The corrected optimized CLI passed all 16 failure comparisons: malformed TOML,
missing config, missing environment file and a regular file used as the root,
each with fsck, index rebuild, lifecycle repair and GC mark. Stdout, stderr and
exit status exactly matched released 1.11.3; no absent root was created and the
existing root file retained its bytes. Fixture:
`/tmp/shardline-invalid-config.aR07sX`. Reproduce with
`scripts/shardline/verify_maintenance_invalid_configuration.sh CURRENT RELEASED`.
Binary SHA-256:
`d3744a4af7d7da87c4c012591a0389cb1626290fab2051fa525de79c6818f26a`.
Strict CLI library/test Clippy, formatting, shell syntax and graph update passed.

The same optimized binary also passed the extended unprivileged local-permission
fixture: `/tmp/shardline-local-permissions.X4ZIry`. Four ordinary commands still
match released stdout/stderr/status under a 0555 root with writable metadata.
The fixture then makes the database, WAL and SHM files read-only and verifies
that a new metadata write is denied. Ordinary fsck still matches released
stdout and successful exit status; stderr clearly warns that optional tracking
is unavailable and no verified outcome will be saved. Database/WAL hashes and
all four existing histories remain unchanged, with no private workspace or
task payload objects. SHM coordination bytes are excluded from the durable-byte
comparison. This tests read-only fsck, not mutation commands on a read-only store.

The PostgreSQL deadline regression holds an exclusive lock on the history table
while a separate blocking reader attempts optional evidence inspection. The
reader returns `TimedOut` through the existing five-second I/O deadline before
the fixture releases the lock. A later read recovers the exact original bytes.
All 17 configured PostgreSQL history tests passed in 10.36 seconds; log:
`/tmp/shardline-batched-deletion.dXBbDM/test.log`. Strict server library/test
Clippy, formatting and graph update passed. This checks a blocked PostgreSQL
read; it does not prove cancellation of arbitrary object-store I/O or a server
shutdown latency bound. Housekeeping still stops cooperatively between calls.

An additional PostgreSQL 17.11 comparison passed the complete `postgres::`
index selection (187 tests, 37.92 seconds) and shared-history selection
(17 tests, 9.43 seconds), with zero failures or ignored tests and no runtime
skips or panic messages. Logs:
`/tmp/shardline-batched-deletion.wBXg8y/test.log` and
`/tmp/shardline-batched-deletion.Kr9Rvw/test.log`; each fixture recorded the
actual server version separately. This includes owner setup, locking,
publication, receipt protection, schema-scoped history, all-role startup,
retirement and the blocked-read deadline. The repository's CI and deployment
examples remain pinned to PostgreSQL 16; this additional result does not
establish a minimum-version policy or compatibility with every HA provider.

The latest optimized CLI, SHA-256
`d3744a4af7d7da87c4c012591a0389cb1626290fab2051fa525de79c6818f26a`,
then passed all 11 selected deployment fault drills in 145.617 seconds.
Log: `/tmp/shardline-maintenance-deployments.cS2WoU/test.log`. This binary
includes SQLite batching, cooperative cleanup checks and the ordinary-error
output fix. Twelve tests outside the explicit selection were excluded; none
of the selected drills skipped at runtime and no panic was logged. The named
rollout/rollback, remote-delete, ownership, restricted-command and replacement
fixtures pass with these bytes. Remaining gate coverage is unchanged.

All 16 configured server metadata tests passed in 10.50 seconds, and the
interrupted multi-batch retirement fixture passed in 1.19 seconds. Logs:
`/tmp/shardline-batched-deletion.LpKeO7/test.log` and
`/tmp/shardline-batched-deletion.au3don/test.log`. Strict index library/test
Clippy, formatting and diff checks passed. Long caller transactions can delay
evidence cleanup; existing statement timeouts and resumable retirement still
apply. Explicit legacy imports retain their separate publication contract.

The corrected optimized CLI has SHA-256
`122afac40a21237a7e19f68f52c95bf576c04c16bcac5fbfa13ac66711ba1ad2`.
It passes the restricted retention-writer owner-upgrade fixture and the
released/current OCI/S3 process fixture, including lost owners, failed
publication, SIGKILL, fresh retries and released rollback. Existing runtime table
grants and the released ledger remain unchanged; normal servers introduce no
child processes. Diagnostics: `/tmp/shardline-retention-permissions.xDnFW1` and
`/tmp/shardline-oci-upgrade.rLl5Qb`; verifier outputs:
`.codex/validation/ux-publication-retirement-race-permissions.log` and
`.codex/validation/ux-publication-retirement-race-oci.log`.

## Additional protocol and backend fault coverage

The remaining 12 deployment drills passed in 135.305 seconds using the same
optimized CLI SHA-256
`d3744a4af7d7da87c4c012591a0389cb1626290fab2051fa525de79c6818f26a`.
Log: `/tmp/shardline-protocol-faults.PVAOWm/test.log`. Together with the preceding
11-drill run, all 23 deployment tests have passing evidence across two runs of
the same runtime bytes. Neither run contains a selected runtime skip or panic.
The 11 exclusions reported by this new run are selection exclusions.

The new verifier exercises workload bursts, PostgreSQL/MinIO/Redis loss,
network partition and packet faults, verifier clock skew, and LFS/OCI/S3 upload
recovery. It copies the supplied binary and clock-shim library into an owned
temporary fixture and uses disposable Compose projects. The clock-shim copy
has SHA-256 `ad7333813b54202579a4960b3635b67691f4810e9847a11607ef31b3445a8c8f`.

The first attempt had nine passes and three MinIO recovery timeouts:
`/tmp/shardline-protocol-faults.SEqRPy/test.log`. An isolated Docker reproduction
changed the automatically published port from 33452 to 33453 after restart.
The fixture kept clients and recovery probes on the original endpoint. It now
reserves a stable MinIO API port, as it already did for PostgreSQL. All three
previously failing tests pass after this fixture correction. No production
runtime behavior changed. Formatting, strict deployment-test Clippy, Bash
syntax, diff checks and the AST graph update passed. Broader merge gates remain
open; these fault results do not establish universal exactly-once execution or
the health of an existing deployment.

## Ordinary shared-history completion acknowledgement

The SQLite-backed ordinary receipt path now has a direct regression for a saved
report whose completion acknowledgement cannot be written. Admission and the
checkpoint succeed; an insert trigger rejects the actual completion record
(sequence 3), after the result body and its signed proof have been saved.
Finishing returns an error. A fresh store does not classify the run as completed
or failed, and completed-history cleanup leaves its manifest intact. The test
passes in 4.91 seconds. This exercises evidence recording without invoking or
repeating application work.

The preceding report-write failure regression now checks the actual completion
sequence rather than nonexistent sequence 4; it passes in 4.84 seconds. These
checks cover two distinct recording failures. They do not prove interruption
safety for every external mutation or close the phase-by-phase merge gate.

The admission regression also passes in 14.65 seconds. It rejects each of the
intent, started and checkpoint record inserts in a fresh SQLite fixture.
After reopening, each run remains unfinished, preceding records remain readable,
the rejected record is absent and completed-history retention preserves the
manifest. This proves the shared receipt's evidence behavior at these three
record boundaries. It does not execute the application command or establish
the domain effects of commands that proceed after optional admission fails.

For this ordinary shared receipt path, the current phase evidence is:

| Phase | Verified evidence | Remaining limit |
| --- | --- | --- |
| Intent and start | Rejected sequence 0 or 1 stays unfinished after reopening. | SQLite receipt fixture; operation fallback is covered separately by command comparisons. |
| Checkpoint | Rejected sequence 2 preserves earlier records without completion. | Does not simulate every interrupted backend write. |
| External mutation | Existing deployment drills exercise backend loss, lost delete replies and upload interruptions. | Full domain and library write-path audit remains open. |
| Report publication | Rejected materialized report leaves no completion record. | Named ordinary receipt fixture. |
| Completion acknowledgement | Saved report plus rejected sequence 3 remains unresolved and retained. | Does not authorize replaying work or infer success from a report alone. |

Strict CLI library/test Clippy, formatting and diff checks pass. No runtime code
or version changed.

The application fallback is now exercised directly too:
`ordinary_command_succeeds_when_shared_admission_records_are_denied` passes in
14.65 seconds. Three fresh fixtures reject intent, start or checkpoint record
publication while the real ordinary index-rebuild handler runs against local
metadata. Each command returns success and exit code 0 with no output capture
and no published completion. Its one unfinished history entry survives
completed-history pruning. Handler summaries remain live on stdout.
Strict CLI library/test Clippy and diff checks pass. This tests successful empty
local rebuilds; it does not substitute for released/current output comparisons,
failure-outcome coverage or the external-mutation audit.

## Incremental automatic history retirement

Normal command and startup housekeeping now uses
`prune_completed_incrementally`, with a shared budget of 256 run-artifact keys
per invocation and the existing ceiling of 64 finished runs. A key may have both
metadata and legacy object copies; both are removed before its metadata batch
commits. Manifest, scope-binding and retirement-marker operations are outside
the artifact-key count. The existing public `prune_completed` API retains its
previous behavior.

When the budget runs out, the hidden run keeps its retirement marker and its
remaining artifacts. A later invocation resumes it before admitting new
retirements. This never repeats application work, changes command status or
deletes payload objects. Inventory scans and active I/O remain outside a wall
clock bound; legacy relocation is a separate housekeeping step.

Three local retention tests pass in 1.21 seconds, including 400-extra-artifact
SQLite and legacy-object histories, exact 256-key progress, metadata/object
copies, reopening, marker preservation, payload preservation and the existing
second-batch failure regression. The configured PostgreSQL fixture passes both
retention tests in 2.45 seconds:
`/tmp/shardline-batched-deletion.v0oRLD/test.log`. It verifies exact progress,
retained scope binding and marker, fresh-backend resumption and final cleanup.
The ordinary command/cleanup-order fixture passes all four combinations in
39.58 seconds, preserving command status when cleanup fails. Strict server and
CLI library/test Clippy and diff checks pass. These checks establish incremental
retirement semantics, not a production speedup or a hard command-exit deadline.

The optimized CLI also passes the two-invocation retirement verifier:
`/tmp/shardline-incremental-cleanup.QAJRua`. Its current binary SHA-256 is
`30d3403fe1f1e48df826baa407d7d01a16de12b6ac79a135b4d3994637a24d87`;
the released comparator is
`166b5558c9895ebbb7439af2915fdfa44d6d4f2a830f816e257f294b639fc9d8`.
It creates a real completed ordinary run, ages that fixture manifest and adds
400 legacy artifact rows. The first subsequent index rebuild removes exactly
256 old run keys, hides the expired manifest and preserves the retirement
marker. The next rebuild removes the remaining keys and marker. Both commands
preserve released stdout, stderr and successful exit status, payload bytes stay
identical, and two fresh run manifests remain. The release build completed in
2m42s. This proves the tested CLI cleanup boundary; inventory latency and broader
deployment guarantees remain open.

The same optimized binary subsequently passes all 11 selected deployment drills
in 143.877 seconds: `/tmp/shardline-maintenance-deployments.m5MAri/test.log`.
There are no selected runtime skips or logged panics. Twelve protocol drills
are outside this selection; their earlier passing run used the preceding binary
and does not establish their results for these new bytes.

The aggregate-backlog regression passes in 0.50 seconds. Three completed local
histories each contain 140 extra artifacts. One incremental call retires exactly
256 keys across run boundaries and completes one history; the second completes
the remaining two. No run artifacts or retirement markers remain. This checks
that the budget is shared across histories and that a partially retired run does
not lose later finished runs.

## Current optimized CLI interruption boundaries

Nextest run `79f16f97-4089-461f-8d25-567698eeccd1` passes three selected
`maintenance_e2e` process tests in 0.628 seconds with optimized CLI SHA-256
`30d3403fe1f1e48df826baa407d7d01a16de12b6ac79a135b4d3994637a24d87`.
The 15 exclusions are selection exclusions, not runtime skips. The tested cases
are `interrupted_mutation_is_fenced_and_remains_unknown`,
`killed_gc_after_domain_mutation_or_report_never_claims_completion_or_replays`
and `killed_ordinary_maintenance_remains_unresolved_and_is_not_replayed`.

The optional-controller test holds the existing local mutation barrier, observes
a claimed and acknowledged worker, rejects competing invocation, kills the
controller and verifies fencing plus a clear unknown-effects recovery error.
The ordinary GC fixture kills after a real quarantine mutation, including the
case where its report is materialized but completion is blocked. Recovery keeps
the interrupted outcome unconfirmed and fresh scans preserve the original
payload. These are actual process interruption checks, not receipt-only fault
fixtures. They do not prove every interruption point, remote-call cancellation
or cluster-wide exactly-once execution. The guide now states that stopping a
worker cannot undo a remote request already received by storage.

## Optional worker lifetime independent of output capture

A stronger controller-loss assertion exposed a gap in the preceding binary:
Nextest run `78e31d7d-5d59-4ace-8f71-e62378c2ebe9` failed after 5.176 seconds
because the optional worker survived controller termination. The fixture held
the local mutation barrier, inspected its recorded worker PID, and ran recovery
to fence the surviving fixture child before reporting failure. The prior test
proved fencing during recovery, not immediate worker termination.

`ProcessTaskRuntime::stop_with_controller` is now a separate opt-in from output
capture. Shardline's optional maintenance controller enables it. Managed runtime
drop fences its owned workers; the existing Linux parent-death signal stops a
claimed child if the controller dies. Existing library defaults remain unchanged,
including reconstructable workers that survive a dropped runtime. Normal inline
maintenance does not invoke this process runtime. Authorization uses the existing
optional controller PID field; no new task schema, service or operator setting
is introduced.

All 11 process-runtime tests pass in 2.09 seconds, including the opt-in drop test,
default surviving-worker recovery, kernel CPU limits, checkpoint acknowledgements
and deadline enforcement. Strict reliability and CLI library/test Clippy pass.
The updated optimized CLI has SHA-256
`aee2f7cba208a6e669bae90b24cacd9f4c7a494dc853e973b3e677cad86691c2`;
the release build completed in 3m05s. Nextest run
`e366253d-bc85-4dde-b4a8-1ea238b19896` passes four selected process tests in
1.332 seconds. These include the strengthened child-termination test, both
ordinary interruption fixtures and successful fsck/rebuild/GC/lifecycle-repair
workflows with completed-history reuse. Fourteen tests are selection exclusions.
The strengthened test observes the child stopped before invoking recovery;
recovery still rejects the uncertain mutation. This does not cancel remote
effects already issued. Formatting, diff checks and the AST graph update pass.

## Full maintenance CLI suite against metadata-backed history

All 18 `maintenance_e2e` tests pass in 3.740 seconds with optimized CLI SHA-256
`aee2f7cba208a6e669bae90b24cacd9f4c7a494dc853e973b3e677cad86691c2`.
Log: `/tmp/shardline-maintenance-cli-audit.yiJA8O/final.log`; no selected runtime
skip or panic is logged. This includes fresh ordinary invocations, verified native
records and signed/materialized outcomes, non-UTF-8 paths, same-PID execution,
permission preservation, failure status, configuration pinning, local storage
migration, optional-worker lifetime and interruption recovery.

The first complete run had 12 passes and six failures; its log is retained as
`before.log` in the same fixture. Five tests still read automatic history from
the old payload-directory layout, and one still required the duplicate tracking
warning that the released-error compatibility fix removed. Tests now read actual
SQLite manifests while retaining their native projection, signature, outcome,
fresh-run and owner-PID assertions. They also check that ordinary history stays
outside payload inventory. The invalid-root case preserves its original status
and diagnostic without requiring the duplicate warning.

The shared-publication fault now installs a SQLite insert trigger on the actual
task-artifact table. Application work succeeds, private native evidence remains,
and removing the trigger allows the next invocation to publish both histories
and clean their workspaces. The unrelated seed evidence is preserved. Cleanup
assertions allow only an empty `retired` directory, rather than treating that
infrastructure directory as an unremoved workspace. The intermediate result
with those two stale cleanup assertions is retained as `metadata.log`.

Strict CLI library/test Clippy, formatting and diff checks pass. These changes
repair verification against the current storage contract; no runtime behavior
or version changed. They do not close the remaining deployment-layout and
external-mutation audits.

## S3 publication API authority boundary

The source audit traces server prepared-S3 publication through
`PostgresBackend::publish_prepared_s3_on_connection`: it opens an outer
transaction, publishes version/latest/listing metadata on that connection,
checks newly staged backing content under reference protection when publication
succeeds, then commits. File and native server publication have corresponding
connection-bound wrappers. The raw index APIs receive metadata and a connection,
not an object store; retaining a connection alone does not prove ownership or
backing-content presence.

`PostgresRecordStore::publish_s3_object_on_connection` now documents that the
caller supplies ownership/reference protection and storage validation. Its
optional resumable completion fence checks that session's completion claim; it
does not replace the other checks. The existing file-version and native APIs
already state their equivalent boundary. This is an API-contract clarification,
not a new claim that every lower-level library caller is storage-protected.
No signature, runtime behavior or version changed.

## Ordinary-command legacy history relocation

`ordinary_commands_relocate_finished_legacy_history_incrementally` seeds a
completed run through the optimized CLI, places its exact records in the legacy
object layout, and adds 80 evidence artifacts plus an unrecognized manifest.
Two ordinary `index rebuild` invocations move the completed history into SQLite.
The first moves exactly 64 run artifacts and leaves the legacy manifest; the
second completes relocation, moving that manifest last. All 80 artifact bodies,
user payload bytes and the unrecognized manifest remain intact, and private
scratch is cleaned.

The full maintenance CLI suite passes all 19 tests against optimized binary
`aee2f7cba208a6e669bae90b24cacd9f4c7a494dc853e973b3e677cad86691c2`
(Nextest run `066e0f7c-683c-49d2-af4b-22670e64336f`). This proves the tested local
incremental upgrade path, not an inventory latency bound or every legacy layout.
No runtime behavior or version changed.

## Ordinary command result-recording failures

`ordinary_command_preserves_success_when_report_or_acknowledgement_is_denied`
executes the original index-rebuild handler through shared inline tracking in
two fresh SQLite fixtures. One rejects report artifacts; the other rejects
completion sequence 3. Both commands keep success and exit code 0, use live
output rather than capture, and report no verified evidence publication.
Reopened history stays unresolved, has no completion record, and cannot be
pruned as finished. An existing fixture artifact remains byte-identical.

The test passes in 9.83 seconds. This extends the receipt-only recording-fault
tests to a real command; it does not establish interruption safety for every
external mutation or every backend. No runtime behavior or version changed.

## In-flight local history call during shutdown

`shutdown_allows_an_inflight_sqlite_call_but_stops_subsequent_housekeeping`
holds SQLite's write lock while a housekeeping publication is pending, drops
the router-lifetime guard, then releases the lock. The pending write completes
with its original bytes. The next housekeeping publication returns Interrupted
and writes nothing; the ordinary application store remains writable.
All four housekeeping tests pass in 0.26 seconds.

This verifies cooperative stopping around a real blocked metadata write.
It does not cancel that write or establish a runtime-drop deadline. SQLite
connections retain their existing 30-second busy timeout; arbitrary filesystem
or remote storage delays remain outside this fixture. No runtime behavior or
version changed.

## Stop retention traversal when its deletion budget is reached

Retirement now uses a distinct scan purpose that honors early stopping across
metadata and legacy object inventory. History-page selection still considers
all unordered legacy entries. Storage failures remain errors; stopping a scan
does not delete unvisited objects or finish a deferred retirement marker.

Two new inventory tests verify a single legacy visit before retirement stops,
all 40 legacy visits for pagination, storage-error propagation, and skipping
legacy traversal after metadata stops. Three SQLite retention tests preserve
exact deletion budgets, shared budgets across runs, resumable batch failure and
payload bytes. Two HTTP pagination tests pass unchanged. The configured
PostgreSQL fixture passes both retirement tests in 2.24 seconds, including
binding preservation and second-batch recovery; its log is
`/tmp/shardline-batched-deletion.NNleol/test.log`.

Strict server library/test Clippy, formatting and diff checks pass. This removes
post-budget traversal, not initial inventory cost or storage-call latency.
The version remains unchanged.

The optimized build containing this traversal change completes successfully
and reports version 1.11.3. Its SHA-256 is
`32f9b631dbea3d80b5999a28a42bf0dff18468ac3b9ae3d79b6328f214f2473e`.
The ordinary-command incremental cleanup verifier passes against released
1.11.3, preserving stdout, stderr, exit status and payload bytes while deleting
exactly 256 keys, keeping the retirement marker, and finishing on the next
invocation. Diagnostics are in `/tmp/shardline-incremental-cleanup.pG6Gmx`.

All 19 maintenance CLI tests also pass against these optimized bytes in
0.725 seconds (Nextest `044396c6-f671-434b-9936-7d6084c354be`), including
same-PID ordinary work, interruption, configuration binding, legacy relocation,
read-only roots and optional-worker recovery. These results cover the named
workflows; broader deployment and scale gates remain open.

The full task-tracking library suite also passes with a configured disposable
PostgreSQL fixture after the traversal change: 65 passed, zero failed, and two
explicit full-capacity drills ignored, in 96.21 seconds. The run includes
purely object-backed and metadata-backed 400-artifact retirement, history
pagination, corrupt/unfinished preservation, scope routing, database deadlines,
router startup and cooperative shutdown. The retained log is
`/tmp/shardline-batched-deletion.QClLW6/test.log`. No runtime skip messages or
panics were accepted. This run does not replace the separately executed release
capacity drills or establish production latency.

## Generic record mutation caller audit

Repository call-site review distinguishes metadata adapters from protected
server workflows:

| Path | Authority and validation boundary |
| --- | --- |
| Native PostgreSQL uploads | Both memory and file staging call the validated server publication wrapper; an existing request connection is retained, and a lost connection rejects publication. |
| Ordinary PostgreSQL rebuild | `run_index_rebuild` constructs its adapters from the dedicated exclusive work pool and closes that pool after work. Generic rebuild writes are metadata reconstruction, not new upload validation. |
| File-reference copy | Uses the request connection when present; otherwise follows the existing pool-backed library behavior. Copies existing metadata rather than validating fresh staged content. |
| Raw `PostgresRecordStore` and `RecordMutation` | Receive no object-store handle. Pool construction supplies neither execution authority nor proof of backing-content presence. |

The `PostgresRecordStore` type documentation now states the contract for generic
trait methods as well as the named publication APIs. This source audit does not
extend physical-content guarantees to arbitrary library callers or hidden
legacy references. No signature, runtime behavior or version changed.

## Deployment drills after retention traversal changes

Optimized binary
`32f9b631dbea3d80b5999a28a42bf0dff18468ac3b9ae3d79b6328f214f2473e`
passes the complete 23-test deployment set across two complementary runs:

- 11 maintenance/deployment drills pass in 143.531 seconds. The log is
  `/tmp/shardline-maintenance-deployments.rVnFbk/test.log`. These cover startup,
  restricted commands, shared ownership, replacement, remote-delete faults,
  and released/current rollout and rollback.
- The other 12 protocol/backend-fault drills pass in 135.770 seconds. The log
  is `/tmp/shardline-protocol-faults.xRmNxO/test.log`. These cover backend loss,
  network partition and packet faults, verifier clock skew, workload burst,
  and LFS/OCI/S3 recovery. The clock fixture uses library SHA-256
  `ad7333813b54202579a4960b3635b67691f4810e9847a11607ef31b3445a8c8f`.

Each verifier rejects runtime skip messages and background-task panics and
requires its exact selected count. Nextest exclusions are the complementary
tests, not missing results: all 23 pass with the same optimized binary bytes.
These are disposable deployment fixtures, not proof of current production
health, exactly-once effects, or every deployment layout. The version remains
1.11.3; the broader acceptance gates remain open.

## Startup final-recording failures preserve availability and fresh work

`startup_sweep_preserves_work_and_readiness_when_final_recording_fails` passes
in 0.13 seconds with two local-router fixtures: one rejects report artifacts,
and one rejects completion sequence 2. Both startup sweeps remove real expired
LFS staging and keep `/api/v1/status` ready. History has no verified completion
and cannot be pruned as finished. The receipt may save interruption evidence
after report failure; denial of sequence 2 prevents that record as well.

After removing the fault, a new router startup removes newly expired staging
and records a distinct completed run. The earlier unconfirmed run and original
fixture bytes remain intact. The first test assertion incorrectly required
sequence 2 to be absent after report failure; inspection showed its valid
SupervisorInterrupted record, and the corrected assertion checks that event
rather than mistaking it for completion. This local test does not simulate
every server mutation or remote backend. No runtime behavior or version changed.

The test subsequently extends the same matrix to legacy filesystem S3 multipart
sessions: both frontends, each with report-write and completion-record denial,
pass in 0.25 seconds. Actual expired part/session files are removed, readiness
stays ready, unconfirmed evidence remains, and fresh startup records distinct
work. PostgreSQL-fenced S3 sessions are intentionally outside this local sweep
and use their durable cleanup path. Source review also confirms that the Hub
background secret upgrade uses its nonreplaceable work pool and preserves
lazy per-row upgrade when its optional sweep fails. This extension does not
claim every Hub or remote-S3 failure phase is covered.

## Hub secret upgrade survives optional final-history failures

`local_upgrade_preserves_encrypted_secret_when_final_history_recording_fails`
runs the actual local Hub upgrade with report-artifact denial and completion
sequence-2 denial. Both cases upgrade the plaintext secret successfully while
history remains unconfirmed and retained. The encrypted value decrypts to the
original fixture secret. After removing the recording fault, a fresh sweep
records a distinct completed run, reports zero further upgrades, and keeps the
existing ciphertext byte-for-byte. The local regression passes in 0.13 seconds.

A configured disposable PostgreSQL fixture passes both Hub tests in 1.61 seconds,
including the existing actual-work-schema binding and owner-loss/no-reconnect
test. Its log is `/tmp/shardline-batched-deletion.OFZ8WW/test.log`; runtime skips
and panics were rejected. Strict server library/test Clippy passes. These tests
extend named recording and ownership evidence, not every secret-delivery or
external-mutation phase. No runtime behavior or version changed.

## Historical evidence and native profile compatibility audit

The annotated `v1.11.3` tag resolves to
`98a4dadb865cca14178a27cb7480e1bb4e9eabe3`, exactly the source commit recorded
for the frozen historical fixtures. Its workspace used StateChronicle/Penelope
0.1.0. The current all-feature reliability run passes all three deployment
compatibility tests and all six native-artifact tests.

Historical tests cover 30 chains across all eleven operation kinds, compare
the reconstructed commitments and signatures with frozen values, independently
verify signatures, retain mixed-encoding continuation, read envelopes without
schema markers, and reject tampered roots, missing commitments and rewritten
storage sequences. Native-artifact tests separately verify the frozen new
maintenance schema and reject forged roots, mismatched authority and oversized
or unsupported documents.

These results establish the tested persisted-format compatibility; they do not
make signed history execution authority or cover every deployment layout.
No fixture, runtime code or version changed.

## Actionable optional-history warnings

CLI admission and final-recording warnings now say that work continues or has
finished with its original exit code, that history has not confirmed the result,
and that the operator should check output, logs and affected data before retrying.
Embedded-job warnings say that the server continues and give the same inspection
action. This replaces receipt/domain-barrier terminology in those messages.

Strict CLI/server library-and-test Clippy passes. The existing permissions
verifier passes with debug binary
`4df32ebb38458fcf17ed2b67a547008e666a7f0b08172a81b57c1aec5a300e1f`
against released 1.11.3: all four commands preserve output and status under a
0555 root, and fully read-only SQLite fsck preserves durable bytes and history
while rendering the new warning. Its assertions check continuation and the
retry-inspection guidance; diagnostics are
`/tmp/shardline-local-permissions.8eV8C0`. This is a behavior/text check, not an
optimized-performance result. Execution, storage semantics and version are
unchanged.

### Optimized actionable-warning compatibility check

The optimized build of `138dd530` completed successfully and has SHA-256
`bfb13098f724e0c01dd11db436a85804d6d59225f2fd4b3e12ec35246adf5a90`.
The local permissions verifier passes against released 1.11.3 with this binary:
fsck, rebuild, repair and GC preserve output/status under a 0555 root and record
metadata-backed history without new root entries or permission changes.
Fully read-only SQLite fsck preserves released output/status, database evidence
and the actionable optional-history warning. Diagnostics are
`/tmp/shardline-local-permissions.LcevEF`.
This confirms the optimized warning behavior; it does not establish performance,
full-capacity incremental cleanup or completion of the remaining merge gates.

### Full-capacity automatic incremental SQLite cleanup

The separate optimized automatic-cleanup fixture passes with all 4,096 history
slots occupied. It uses `prune_completed_incrementally`, not the compatibility
cleanup API: each call removes at most 256 run-artifact keys and retires at most
64 histories. All 4,094 verified completed histories retire in 97 calls;
retirement markers finish, unfinished and corrupt histories remain inspectable,
corrupt bytes and user payload remain unchanged, and fresh recording succeeds.
Initial pagination visits all 4,096 distinct histories and over-capacity admission
is rejected. Existing oldest-first compatibility assertions remain unchanged
in their separate fixture.

The exact ignored release test reports one pass, zero failures and zero ignored
tests in 130.68 seconds; cleanup takes 102.124 seconds including the fixture's
before/after artifact inventories. Diagnostics are
`/tmp/shardline-history-capacity.KrWsMv/test.log`.
The PostgreSQL drill ran concurrently, so these are correctness observations,
not controlled performance measurements. Larger archives, physical disk growth
and the broader acceptance gates remain open.

### Full-capacity automatic incremental PostgreSQL cleanup

The exact optimized PostgreSQL incremental-capacity test passes: one test,
zero failures, zero ignored, 457.02 seconds. The owned PostgreSQL fixture fills
all 4,096 automatic history slots, rejects further admission, paginates every
distinct history, then calls `prune_completed_incrementally` until cleanup
settles. Each invocation removes at most 256 run-artifact keys and retires at
most 64 histories. All 4,094 verified completed runs retire in 97 calls;
no retirement markers remain. Unfinished and corrupt histories, corrupt bytes
and user payload are preserved; new recording succeeds after cleanup.

Diagnostics are `/tmp/shardline-postgres-capacity.hk1D0W/test.log`.
Creation takes 84.715 seconds, inspection 35.452 seconds and measured cleanup
336.591 seconds, including before/after artifact inventories. The slowest
measured iteration takes 5.674 seconds. These observations include concurrent
build/test activity and do not establish a latency bound or a performance
improvement. The original compatibility fixture retains its exact 65-call
assertion; this automatic fixture separately checks that the artifact budget
splits cleanup across more calls. Broader cost, legacy archives and the
remaining acceptance gates stay open.

### Storage-copy destination history failure

The actual optimized CLI storage-copy fixture passes when an owned destination
SQLite trigger denies shared history publication. The copy succeeds, reports
one scanned object and preserves both source and destination payload bytes.
No shared completion is published while recording is denied, and existing
destination evidence bytes survive. After removing the fault, an explicitly
invoked fresh copy succeeds and shared history becomes available without
altering copied bytes or earlier evidence.

The final focused Nextest run `ba27729d-be9f-45d2-aba0-6444a434c0c6` passes
in 0.093 seconds against optimized CLI SHA-256
`bfb13098f724e0c01dd11db436a85804d6d59225f2fd4b3e12ec35246adf5a90`.
Strict CLI integration-test Clippy passes. This verifies optional destination
recording failure during a local raw copy; it does not establish a coordinated
live storage cutover or settle pending deletes. No runtime or version changed.

### Documented ordinary repair under restricted permissions

The optimized released/current permissions comparison now covers both ordinary
`shardline repair` and its `repair lifecycle` form, alongside fsck, rebuild and
GC mark. All five forms preserve stdout, stderr and exit status under a 0555
deployment root, create no private root entries and record five metadata-backed
histories. Fully read-only SQLite fsck still preserves database/WAL bytes and
existing histories while emitting the actionable recording warning.
Diagnostics are `/tmp/shardline-local-permissions.zay6fd`.
This extends coverage to the command recommended in the operator guide; it does
not change the runtime or establish broader remote recovery guarantees.

### OCI library publication authority boundary

The lower-level OCI publication methods now explicitly require a caller-held
repository fence on the supplied connection. They do not acquire ownership or
inspect backing object bytes; the compatibility method supplies no payload
references, and the explicit-reference variant requires the complete set from
a validated payload. Library callers must retain storage validation and
reference protection through publication. The server's locked publisher uses
the original resource guard connection and rejects a missing connection instead
of replacing it. This documents the existing API boundary rather than extending
the server guarantee to arbitrary metadata-only callers.

Strict `shardline-index` rustdoc passes with warnings denied. Public signatures,
runtime behavior and the version are unchanged; the remaining write-path audit
is not closed by this documentation check.

### Upload intent consumers and publication boundary

Source tracing narrows the separate upload-intent ownership audit. PostgreSQL
`transition_intent_with_event` conditionally updates the current intent state and
records its event in a pool transaction. Native shard callbacks instead retain
the request-session connection; a present session with a lost connection returns
`PoolClosed`, and only calls outside that task-local session use the pool path.
OCI locked publication similarly requires its resource guard connection.

GC reachability traverses latest/version file records and live dedupe mappings;
it does not take upload intent status as reachability authority. PostgreSQL
startup reconciliation reads already-visible file metadata or inspects object
storage, then updates intent state. It does not reconstruct file publication from
an intent's status. This source evidence distinguishes bookkeeping from data
publication but does not replace the pending ownership-loss/recovery interleaving
test or claim that every upload lifecycle write is request-session fenced.

### Native upload recovery after ownership or metadata failure

A real ownership-loss regression exposed a recovery defect: a native shard
intent names the original request hash, while validated storage may use a
normalized shard key. Startup reconciliation previously treated absence of the
original key as terminal failure, preventing a valid fresh retry. The original
PostgreSQL regression fails with `Failed` instead of the expected in-flight
state in `/tmp/shardline-batched-deletion.IxGtcB/test.log`.

Local and PostgreSQL recovery now preserve an in-flight native shard intent when
that request key is absent. A shared semantic module identifies this case;
neither backend reconstructs file metadata or replays the upload from the intent.
Other missing-object recovery rules remain unchanged. Existing terminally failed
intents are not rewritten or reopened by this change.

The corrected owned-PostgreSQL test passes in 0.49 seconds:
`/tmp/shardline-batched-deletion.pfxN04/test.log`. It terminates the real request
session, runs native upload in its scope, verifies rejected publication and zero
file/dedupe rows, runs aged-intent reconciliation, and verifies the intent stays
in flight with no file visibility. A fresh validated upload then succeeds and
records visibility. The local SQLite regression denies actual file publication
and verifies the same reconciliation/retry boundary; all 22 local backend tests
pass in 0.40 seconds, including existing missing-object recovery behavior.
Strict server library/test Clippy passes. This fixes the named recovery defect,
not all late upload bookkeeping or every interruption phase.

### Publication regression suite after native recovery correction

All eight configured PostgreSQL publication tests pass with the recovery fix
in 0.95 seconds: owner-loss copy/native upload, guarded reads, staged S3 backing
loss, native xorb backing loss, publication lock lifetime and both stale-snapshot
cases. The fixture uses an owned PostgreSQL database, rejects skipped/empty test
runs, and preserves diagnostics at
`/tmp/shardline-batched-deletion.KVYmvG/test.log`.
This extends the focused native recovery proof without establishing every
external-storage fault phase. The optimized CLI rebuild is still pending.

### Six native upload durability boundaries and optimized CLI verification

The configured PostgreSQL publication suite now passes all nine tests in
2.93 seconds, including native upload interruptions after intent creation,
storing, object work, stored acknowledgement, metadata acknowledgement and
visibility acknowledgement. Each case invokes actual native upload with typed
CAS fault injection, checks its persisted lifecycle state and actual file
visibility, runs aged-intent reconciliation, and succeeds with a fresh validated
upload. No pre-publication interruption becomes visible during reconciliation.
The fault-injection feature is enabled as a server dev dependency only;
production dependency configuration and the lockfile remain unchanged.
Diagnostics: `/tmp/shardline-batched-deletion.Wy4WRB/test.log`.

The optimized production CLI rebuild completed in 3m04s, reports 1.11.3 and has
SHA-256 `355ccbcf7b276cab60e1f6f1c01e0f8337045e1e588aa5169b1b2dcc68ae38c4`.
All 20 maintenance CLI scenarios pass in 0.737 seconds (Nextest run
`c2378239-a17c-4983-a6e3-34d967fd2bd2`). The released/current permission verifier
also passes all five command forms and fully read-only SQLite fsck with this
binary: `/tmp/shardline-local-permissions.xIWzft`.
These results verify the corrected native recovery and named CLI workflows;
they do not establish every remote-storage fault or close the broader gates.
Strict server library/test Clippy passes with the test feature enabled; formatting,
diff checks and the AST graph update also pass.

### Ordinary command final-recording fault matrix

The shared-inline actual-handler regression now covers fsck, index rebuild,
lifecycle repair and GC mark, each with denied report-artifact writes and denied
completion acknowledgement. All eight cases preserve success, exit zero and
live output without capture; no case publishes a finished shared history.
The completion record stays absent, unfinished history does not retire, and
pre-existing evidence bytes remain unchanged. The focused locked test passes
in 39.02 seconds. Strict CLI library/test Clippy passes.
These cases use empty deployment fixtures and prove recording-failure behavior,
not each operation's external-mutation interleaving. Storage-copy destination
publication failure has its separate actual CLI fixture above.

### Deployment drills with corrected native recovery and actionable warnings

All 23 deployment scenarios now have passing results against the same optimized
CLI bytes (`355ccbcf7b276cab60e1f6f1c01e0f8337045e1e588aa5169b1b2dcc68ae38c4`).
The protocol run passes all 12 in 142.798 seconds:
`/tmp/shardline-protocol-faults.A27K1j/test.log`.
The complementary run passes nine initially but fails two saved-reply recovery
cases on an outdated warning assertion:
`/tmp/shardline-maintenance-deployments.feVMRw/test.log`.
Those cases expected the word `unconfirmed`; the command correctly emitted the
new actionable wording, `its result was not confirmed in history`.

The corrected assertion requires both that wording and guidance to inspect
command logs and affected data before retrying. It changes no marker, reply,
delete-count, ownership or reconciliation assertions. Both complete scenarios
then pass in 83.531 seconds (Nextest
`88c2d4e7-3f1e-4b79-b20c-3a2a652fcae6`), with no runtime skips or panics:
`.codex/validation/ux-saved-reply-warning-rerun.log`.
The pinned binaries in both original runs have matching hashes. This is 23
passing named scenarios across complementary runs and a focused assertion
correction, not an initially green 23-test run or proof of every deployment.
Strict deployment-test Clippy passes; runtime behavior and version are unchanged.

### Large legacy-run incremental retirement

The explicit object-store fixture adds 10,000 legacy artifacts to a verified
completed run. It reopens the history store between every cleanup call and
checks positive progress of at most 256 run-artifact keys per call. The expired
manifest stays hidden, the retirement marker preserves the original signed
manifest until cleanup finishes, and the run retires exactly once. Unfinished
history remains inspectable; corrupt history and user payload bytes remain
unchanged throughout.

After scoped local prefix pruning, the exact fixture also passes in the optimized
profile: one passed, zero failed, zero ignored, with 40 cleanup calls in 1.211
seconds and 2.16 seconds for the whole test. Every call retains its 256-key
progress assertion and checks the signed retirement marker, unfinished history,
corrupt bytes and user payload. This is a local fixture result, not a remote
storage latency guarantee.

The exact ignored test passes when explicitly selected: one passed, zero failed,
zero ignored. Retirement takes 40 calls; the debug fixture reports 1.865 seconds
for its cleanup/check loop and 3.04 seconds for the whole test. These are local
diagnostics, not optimized performance results or a production latency bound.
This covers a large artifact inventory within one legacy run, not a large
inventory of distinct legacy histories. Existing smaller retirement fixtures
and all merge gates remain unchanged.

Reproduce with:

```sh
cargo test --locked -p shardline-server --all-features --lib \
  task_tracking::tests::legacy_retention_capacity::large_legacy_run_retires_incrementally_without_touching_unresolved_evidence \
  -- --ignored --exact --nocapture
```

### Actionable partial delete-recovery logs

The existing server recovery wrapper now warns when a pass retains evidence
that could not settle an attempt, directing operators to original GC/storage
logs and pending-delete inspection. A pass examining 64 replies also explains
that another repair can check later replies. Existing structured counts remain
available. These messages do not assert that later replies exist, that all
pending markers were examined, or that retained evidence authorizes a retry.
Recovery, command results and exit status are unchanged; this is logging only.

The optimized CLI including this guidance builds successfully and still reports
`shardline 1.11.3`. Its SHA-256 is
`cd83f50eef7fdb998b24ea663afe2eceb437caa5ebe618599b246aa66148afb0`.
All 20 maintenance CLI scenarios pass with zero skips in 2.117 seconds (Nextest
`e4962a00-3e45-458e-a012-559717270f10`). This verifies the named CLI behaviors;
the earlier deployment fault results belong to their separately identified
binary, and this run does not replace those results or close the merge gates.

### Many distinct legacy histories

The explicit legacy archive fixture creates 1,024 verified completed runs, one
unfinished run and one corrupt manifest in the object store. It follows history
cursors in pages of at most 64 and checks that all 1,026 IDs appear exactly once.
Cleanup reopens the history store on each invocation, retires all 1,024 finished
runs, and checks the 64-run limit, unchanged corrupt bytes and unchanged user
payload. The remaining two histories include the verified unfinished run.
No existing smaller fixture or merge gate is removed.

The exact ignored test passes when explicitly selected: one passed, zero failed,
zero ignored, in 544.85 seconds using the debug test profile. An optimized CLI
build overlapped part of the run. This establishes the fixture's pagination and
preservation behavior, not a production performance bound. Inventory scanning,
larger archives and deployment-scale cost remain open. Strict server library/test
Clippy, formatting and diff checks pass, and the AST graph is updated.

The same fixture also passes in the optimized release profile: one passed,
zero failed and zero ignored in 241.77 seconds. That total includes fixture
creation, pagination, all cleanup calls and preservation checks; it is not the
duration of one maintenance command or a cleanup-only measurement. This result
keeps inventory cost open rather than establishing a production latency bound.

```sh
cargo test --locked -p shardline-server --all-features --lib \
  task_tracking::tests::legacy_retention_capacity::many_legacy_runs_paginate_and_retire_without_losing_unresolved_history \
  -- --ignored --exact --nocapture
```

### Lifecycle repair recovery from reopened local stores

The existing three-boundary lifecycle repair test now closes its store handles
after the injected interruption and reopens the persistent local stores before
retrying. At retention-hold, quarantine-candidate and webhook-delivery mutation
boundaries, the exact expected partial metadata survives reopening. Fresh repair
removes only the remaining eligible entries; a further pass has no such work.
Unrelated payload bytes remain unchanged after interruption and convergence.
All existing partial-state and mutation-count assertions remain in place.

The focused locked test passes in 0.43 seconds. This covers typed local mutation
interruptions and persisted retry state, not process termination, PostgreSQL
ownership loss or maintenance completion recording. Those require their separate
fixtures; the broader phase-by-phase gate remains open.

### Lifecycle repair recovery in fresh PostgreSQL sessions

The configured PostgreSQL fixture checks all three lifecycle metadata mutation
boundaries in an isolated schema. After each injected interruption it closes
the original pool and opens a new session. The exact expected retention-hold,
quarantine-candidate and webhook-delivery counts survive. Fresh repair removes
only the remaining eligible rows, a further pass finds none, and unrelated
payload bytes remain unchanged.

The exact locked test passes with one passed, zero failed and zero ignored in
0.91 seconds, without runtime skips: diagnostics are
`/tmp/shardline-batched-deletion.McZ5OR/test.log`. The earlier fixture setup
omitted schema-specific delete protection and correctly failed with a schema
mismatch; the passing run initializes that protection before seeding mutations.
The fixture uses a disposable PostgreSQL instance and removes its own schema
and container. It does not establish request-owner loss, process-kill recovery,
remote storage outcomes or maintenance completion recording; separate tests
remain necessary for those boundaries. Runtime code and version are unchanged.

With an owned, migrated PostgreSQL fixture configured through `DATABASE_URL`:

```sh
cargo test --locked -p shardline-server --all-features --lib \
  lifecycle_repair::postgres_recovery::mutation_interruptions_survive_new_postgres_sessions \
  -- --exact --nocapture
```

### Cancellation with an already-borrowed ownership connection

The work-pool fixture now distinguishes retained pool handles from an already
borrowed PostgreSQL connection. Cancellation closes the pool and rejects new
acquisitions, but the borrowed original session still holds its advisory lock.
A competing exclusive operation cannot acquire while that session remains
borrowed. Releasing the connection then permits takeover within the fixture's
two-second deadline. Pool closure is not a claim of forced cancellation of
active database work or permission to bypass its ownership.

All three work-pool tests pass with a configured disposable PostgreSQL instance,
zero failures, zero ignored and no runtime skips in 0.25 seconds:
`/tmp/shardline-batched-deletion.vkvKqx/test.log`. Strict server library/test
Clippy, formatting and diff checks pass. This is local-future cancellation with
an observed database lock, not a network-partition or arbitrary shutdown bound.
Runtime logic and version are unchanged; the guard's documentation now states
the borrowed-connection boundary explicitly.

### Scoped local inventory traversal

Local prefix listing previously descended into every directory before filtering
keys. The walker now skips directories that cannot contain a lexical prefix
match. Partial prefixes still include similarly named directories; nested
prefixes include their ancestors. Full inventories still visit all directories,
and existing symlink checks remain in place. A regression first reproduced a
scoped scan failing on an unreadable unrelated directory; the fixed scan skips
that directory while the full inventory still reports its permission error.

All 471 storage library tests pass in 16.71 seconds. Configured history tests
pass with 66 passed and six explicit capacity drills ignored in 95.16 seconds:
`/tmp/shardline-batched-deletion.2U1pDY/test.log`. Strict storage/server
library/test Clippy, formatting, diff checks and the AST graph update pass.
The library test count does not certify live remote-storage integration.

The unchanged 1,026-history legacy fixture passes before and after pruning in
the optimized profile, with the same preservation assertions and 24 cleanup
calls. Instrumentation separates fixture setup from pagination and cleanup:

| Phase | Before pruning | After pruning |
| --- | --- | --- |
| Create history | 0.851 seconds | 0.855 seconds |
| Paginate all histories | 54.587 seconds | 0.754 seconds |
| Cleanup across 24 calls | 186.460 seconds | 5.391 seconds |
| Slowest cleanup call | 16.249 seconds | 0.362 seconds |

Whole-test durations are 241.90 and 7.00 seconds, respectively. These are local
fixture measurements, with other validation/build work overlapping parts of
the runs; they are not a distributed benchmark or production latency guarantee.
Retention still scans eligible history inventory and active storage calls remain
untimed. The change preserves evidence formats, deletion budgets and command
results; no version bump is included.

The rebuilt optimized CLI including prefix pruning still reports
`shardline 1.11.3`; SHA-256 is
`0ea17070c878ae0a6def0bbdfb68a2a606eb3abd5fc4f563f94b57623eaf3808`.
All 20 maintenance CLI scenarios pass with zero skips in 2.105 seconds (Nextest
`e9bf0275-a02d-42ec-ab5e-15a02b564c56`). The released/current permissions verifier
passes all five command forms and fully read-only SQLite fsck:
`/tmp/shardline-local-permissions.eE1lHR`.

Both complementary deployment runs also pass against that same rebuilt CLI.
The maintenance run passes all 11 selected scenarios in 144.797 seconds:
`/tmp/shardline-maintenance-deployments.v2HamP/test.log`.
The protocol run passes all 12 selected scenarios in 133.525 seconds:
`/tmp/shardline-protocol-faults.pDyX43/test.log`.
Both pinned copies have the SHA-256 above. Each run excludes the other run's
scenarios; all 23 have passing results, with no selected runtime skips or panics.
This covers the named startup, ownership, interruption, remote-delete, backend
loss and rollout/rollback faults after prefix pruning. It does not certify every
deployment or close the broader compatibility, phase and cost gates.

### Request ownership loss at all native upload durability boundaries

The existing six-boundary native upload recovery test now performs its initial
upload inside a request-owned PostgreSQL session. After the typed interruption,
it checks the expected intent state and actual metadata visibility, terminates
that original database session, and retries in the same request context.
Every stale retry fails, including the already-visible intent case; existing
metadata visibility remains unchanged. Reconciliation and a fresh validated
upload then succeed with the original assertions intact.

The configured publication suite passes all nine tests in 3.27 seconds without
runtime skips: `/tmp/shardline-batched-deletion.7jXffA/test.log`.
Strict server library/test Clippy, formatting and diff checks pass.
This strengthens the covered native publication boundary. Upload intent/event
transitions still use their separate index transaction; the test does not claim
that every progress update is fenced by the request session or certify current
storage health from a visible intent. Runtime code and version are unchanged.

### Inspect automatic history without enabling the admin API

`shardline history` uses existing configuration, with optional `--root`,
`--limit` (1–100) and `--after`. It lists verified states, damaged evidence,
recovery guidance and recording notices. It does not instantiate a server
backend, run startup sweeps, retry work or retire history. Pagination uses the
same verified scope filtering and ordered inspection as the admin surface.
Local inspection opens SQLite read-only without creation or migrations and
opens the object adapter without creating its directory. PostgreSQL inspection
resolves the actual configured session scope through a single evidence pool.
The public exhaustive `CliCommand` enum retains its released variants; binary
dispatch handles the new command.

Current development-build checks pass:

- Two server inspection fixtures cover paginated completed, unfinished and
  corrupt records, recovery guidance, invalid bounds and a missing root that
  remains absent (0.19 seconds).
- One local index fixture verifies rejected writes through the read-only
  connection and no creation of a missing root (0.01 seconds).
- A configured PostgreSQL fixture resolves the actual scope and compares every
  saved artifact key and payload before and after inspection (0.65 seconds):
  `/tmp/shardline-batched-deletion.g5g9S8/test.log`, with no runtime skip.
- Both `history_e2e` cases pass (4.72 seconds). Ordinary fsck history is readable
  without an admin token or runner configuration; repeated inspection preserves
  every artifact key and payload. Invalid page bounds fail. A damaged SQLite
  file produces a nonzero exit and no JSON success page, retains its exact
  bytes and creates neither chunks nor private maintenance storage.
- All three legacy CLI API checks and both generated-artifact checks pass.
  An existing exhaustive downstream match still compiles; legacy parsing
  rejects history before loading its requested environment file.
- All 20 existing maintenance CLI scenarios pass against the current
  development binary, with zero failures or ignored tests (91.89 seconds).
- Strict index/server/CLI library and test Clippy, formatting and diff checks
  pass.

These inspection checks do not replace the earlier optimized-binary deployment
drills or close the remaining write-path, compatibility and scale gates.
Inventory scans can still be O(N); a bounded page does not bound scan duration.
The version remains 1.11.3.

### History inspection after login replacement and under read-only permissions

The configured replacement-login retention fixture now inspects history through
`TaskEvidenceStore::for_inspection` before its existing cleanup assertions.
The original source login has closed its pool; the replacement resolves the
same actual history scope despite a different legacy deployment fingerprint.
It sees both completed runs and the unfinished run with recovery guidance.
Every artifact key and payload compares exactly before and after inspection.
The original retirement and unresolved-history assertions remain intact.
The test passes in 1.91 seconds with no runtime skip:
`/tmp/shardline-batched-deletion.9kz1YT/test.log`.

All four current `history_e2e` cases pass in 9.25 seconds. Two new cases run as
an unprivileged user with a 0555 root and 0444 metadata files:

- A retained live connection keeps the WAL sidecars present. A second ordinary
  fsck commits a new history while the main database file remains byte-identical.
  Read-only inspection sees both completed histories, including the new WAL-only
  history, and preserves the main database and WAL bytes exactly.
- A closed WAL-mode deployment has no sidecars and its directory permits no
  creation. SQLite cannot inspect it in this state. The CLI returns a nonzero
  exit with an actionable sidecar diagnostic, emits no JSON success page and
  preserves database bytes and the absence of both sidecars.

The second case initially exposed a generic SQLite read-only error. The command
now recognizes SQLite's typed `READONLY_CANTINIT` and `READONLY_DIRECTORY` errors
through the error chain and explains how to inspect the running deployment or
a consistent backup. It does not switch to immutable/main-file-only reads,
change permissions or discard WAL. This is a supported error path, not proof
that sealed WAL databases with missing sidecars can be inspected. SQLite's
[read-only WAL requirements](https://sqlite.org/wal.html#read_only_databases)
describe this boundary. Remaining merge gates retain their original scope.
Strict index/server/CLI library and test Clippy, formatting and diff checks pass.
The version remains unchanged.

### Readiness guidance without the optional admin API

Completed history now directs operators to `/readyz` for current server
readiness, and identifies `/api/v1/status` as an alternative when the admin
API is enabled. The guides distinguish `shardline health` and `/healthz`
process liveness from the backend readiness check. Historical completion
continues to describe only its recorded run.

A current public-router test disables the admin API, replaces its owned
metadata database with an unusable directory, and checks the actual responses:
`/healthz` remains 200 with `status: ok`, `/readyz` returns 503 with only
`error: service unavailable`, and `/api/v1/status` remains 404. It passes in
0.01 seconds. The existing authenticated startup-history test passes with its
updated guidance assertion in 0.04 seconds. Actual CLI history inspection,
without an admin token, includes the `/readyz` action and preserves all saved
artifact keys and payloads (4.65 seconds).

Strict server/CLI library and test Clippy, formatting and diff checks pass;
the AST graph is refreshed. This change adds no health service, process,
configuration or version bump and does not certify cluster-wide health.

### Inspect pending deletes without enabling the admin API

`shardline history --pending-deletes` reads the configured PostgreSQL inspection
function through one evidence connection. It creates no server backend, local
state, object adapter or worker. It issues no storage requests and neither
retries nor settles an attempt. `--limit` bounds the page to 1–100 entries;
`--after` accepts the preceding `next_after` attempt epoch with the same mode
and configuration. Unavailable inspection exposes `supported: false` and the
same inconclusive notice as HTTP; an empty page is not a safety authorization.

The CLI and admin endpoint share a semantic pending-delete view module. The
HTTP response retains its authenticated envelope, cursor, fields and guidance.
All 32 configured admin API tests pass in 0.65 seconds, including long-key
pagination, unavailable inspection and marker preservation:
`/tmp/shardline-batched-deletion.A0Kem4/test.log`, with no runtime skip.

The configured CLI fixture creates its own schema, admits three pending
attempts (including a 3,500-character key suffix), invokes the actual executable
without an admin token, and follows every one-entry page. It compares every
marker row exactly before and after and verifies that its local root remains
empty. The accompanying local fixture returns an explicit unsupported view
without creating state. Both pass in 0.41 seconds:
`/tmp/shardline-batched-deletion.1gBhiQ/test.log`, with no runtime skip.
A separate invalid-cursor CLI check passes in 0.01 seconds and explains the
attempt cursor before contacting an unavailable database.

All four existing history CLI cases, both generated-artifact cases and all
three legacy CLI API cases pass. Strict server/CLI library and test Clippy,
formatting and diff checks pass; the AST graph is refreshed. Upgrade guidance
now supplies this inspection command. The version is unchanged, and the broader
compatibility, write-path and scale gates remain open.

### Optimized CLI validation including both inspection modes

The optimized executable built from `b0e6e634` reports version 1.11.3 and has
SHA-256 `92b570163e03ecbd9f07b3385b7992a05f0ebb5fa25aaba526fd400a6b689a23`.
With this executable selected through `CARGO_BIN_EXE_shardline`, all 20
maintenance cases and all four history cases pass (24 passed, zero skipped,
2.281 seconds; Nextest run `646d7567-e963-46b5-b77f-11ded2ddb445`).
The configured pending-delete CLI suite also passes all three cases on the
development executable, with no runtime skip, in 0.41 seconds:
`/tmp/shardline-batched-deletion.34Y7VY/test.log`.
Although the release override was exported for that invocation, Cargo's test
launcher selects its built executable. That result must not be counted as
optimized-binary inspection evidence; direct executable selection is required.

The released/current restricted-root comparison passes all five command forms
and the fully read-only SQLite fsck check with the same optimized executable:
`/tmp/shardline-local-permissions.HQRC0v`. Output, exit status, existing evidence
and permissions are preserved; optional recording failure warns without
claiming completion. These checks extend the optimized evidence to the new
inspection commands, but do not close the remaining merge gates.

Both deployment verifiers also pass using pinned copies of that same executable:
11 maintenance, replacement and rolling-upgrade scenarios in 145.530 seconds
(`/tmp/shardline-maintenance-deployments.j4AdmM/test.log`) and 12 complementary
protocol fault scenarios in 145.358 seconds
(`/tmp/shardline-protocol-faults.upeDPt/test.log`). Each run intentionally excludes
the other suite's cases; all 23 selected scenarios pass, with no runtime skip.
The verifiers check the pinned binary hashes after execution. This replaces the
older optimized build's coverage for these named deployment scenarios, without
extending their guarantee to untested write paths or deployment layouts.

### PostgreSQL inspection failure guidance

The CLI recognizes typed PostgreSQL connection, TLS and pool-timeout errors,
including boxed errors from the metadata adapter. It tells operators to check
database availability, network access and deployment configuration, then retry
inspection rather than rerunning maintenance for history. Existing error chains
and failure exit status remain available.

An actual-executable test points both history modes at an unavailable database:
both fail without JSON, provide the action, and leave their deployment roots
empty (one test covering both modes, 10.02 seconds). All four history CLI cases
also pass in 9.11 seconds, including the read-only WAL guidance and byte
preservation checks. Strict CLI library/test Clippy, formatting and diff checks
pass. These are development-executable checks; the preceding optimized results
refer to their explicitly recorded source commit and binary hash.

### CLI inspection with row-scoped PostgreSQL metadata

The configured pending-delete CLI fixture now enables row-level security on
its owned retention table while three delete attempts remain pending. The
actual executable returns `supported: false`, an inconclusive inspection
notice, no rows and no continuation cursor. It does not present unavailable
inspection as verified absence. Disabling row-level security restores all
three attempts; every marker row remains byte-for-byte equivalent to its
pre-inspection JSON representation, and the local root remains empty.

The configured four-case CLI suite passes with no runtime skip in 10.02 seconds:
`/tmp/shardline-batched-deletion.JsK7no/test.log`. Strict CLI library/test Clippy,
formatting and diff checks pass. This adds actual-command evidence to the
existing database-level inspection checks; it does not establish support for
every restricted-account or schema layout.

### Reusing existing configuration for CLI history

An actual-command fixture runs fsck and history from a separate directory using
the same explicit config and environment file. The environment file selects the
deployment root. An unrelated malformed `shardline.toml` in the invocation
directory is ignored; history reads the completed fsck run from the selected
deployment, the explicit config stays unchanged, and the invocation directory
gets no metadata or tracking workspace.

The development-executable case passes in 4.47 seconds. Selecting the previously
pinned optimized executable through Nextest also passes in 0.052 seconds (run
`cc8b6f7d-beaa-49f2-9b94-49975a5c27d3`, one selected case, four intentionally
excluded). Strict CLI library/test Clippy, formatting and diff checks pass.
This confirms the existing configuration workflow for the named fixture;
broader schema and account compatibility remains open.

### Direct optimized inspection validation

An audit of executable selection found that `cargo test -p shardline` selects
its built CLI even when `CARGO_BIN_EXE_shardline` was exported beforehand. The
earlier configured Cargo-driven fixture is therefore development evidence,
as corrected above. A direct run against the older optimized binary passes
three cases but fails the subsequently added connection-error guidance case;
that expected failure confirms why source and binary identities matter.

The release executable rebuilt from `8a649b64` has SHA-256
`4c2e03ee590e6ae5ae3e8784e554d046be863c230aaf9210c27e8aa5f9c3ccdc`.
The new `scripts/shardline/verify_maintenance_inspection.sh` pins a copy of the
CLI, creates its own PostgreSQL container, migrates it with that CLI and invokes
the compiled libtest executable directly with the override. All four cases
pass in 10.01 seconds, with zero skips and the pinned hash checked afterward:
`/tmp/shardline-inspection.FjhGZy/test.log`.

All five history cases also pass on this executable through Nextest in 0.097
seconds, with zero skips (run `f9fc5ba5-3b82-4d54-ba7c-e9c74a95f0fd`). The shell
syntax and diff checks pass. No version bump or broader guarantee is implied.

To reproduce, compile the inspection test without running it:

```bash
cargo test --locked -p shardline --test pending_deletes_e2e --no-run
bash scripts/shardline/verify_maintenance_inspection.sh \
  /absolute/path/to/current/shardline \
  /absolute/path/to/the/printed/pending_deletes_e2e-test-executable
```

Use the executable path printed by Cargo, not the source test file. The verifier
requires Docker and runs only its own disposable container.

### Pending-delete recovery steps in both inspection views

The shared pending-delete guidance now asks operators to check whether the
original GC is still running, recover saved replies after it stops, and inspect
`shardline history --pending-deletes` again. It explains that a backlog can need
several repair runs and preserves the rule that missing or invalid replies stay
blocked. This changes operator guidance only; no retry, settlement, deletion or
ownership behavior changes.

All 32 configured admin API cases pass in 0.69 seconds
(`/tmp/shardline-batched-deletion.AlcsCB/test.log`). All four configured CLI
cases pass on the development executable in 10.02 seconds
(`/tmp/shardline-batched-deletion.DP39tS/test.log`), including assertions for
active-GC guidance, follow-up inspection and a possible backlog. Strict
server/CLI library and test Clippy, formatting and diff checks pass; the AST
graph is refreshed. Remaining merge gates remain open.

### Embedded router lifetime and housekeeping

A local library test constructs the actual server router with its housekeeping
guard, retains a clone and drops the original. The clone serves `/readyz` with
200 and keeps housekeeping enabled. Dropping the last router sets the stop
flag; subsequent housekeeping publication is rejected without creating the
artifact. Previously saved evidence remains readable and ordinary application
publication still succeeds.

`app::e2e_tests::embedded_router_clones_keep_housekeeping_alive_until_the_last_router_drops`
passes in 0.02 seconds. This verifies the router's actual guard ownership rather
than only constructing guards directly. It does not prove cancellation of
active storage I/O or every embedded deployment layout, and production behavior
is unchanged. Strict server library/test Clippy, formatting and diff checks pass.

### Reject malformed history bounds before opening the deployment

The CLI now checks the existing 128-byte history cursor limit and page-size
bounds before configuration loading or database access. An oversized cursor
explains that `--after` must come from the history page's `next_after`; valid
cursor handling and the server's pagination contract remain unchanged.

An actual-command test uses a 129-byte cursor with an unavailable PostgreSQL
endpoint. It fails with the cursor action instead of a pool timeout, produces
no JSON and creates no local state (0.01 seconds). All six history CLI cases
pass on the development executable in 9.41 seconds, including configuration
reuse and read-only WAL checks. Strict CLI library/test Clippy, formatting and
diff checks pass. This check does not establish completion of the remaining
compatibility or scale gates.

### Shared-metadata router lifetime for all server roles

The configured PostgreSQL library test constructs real routers for `all`,
`api` and `transfer` roles. Each retains a clone after dropping its original,
serves `/readyz` successfully, and publishes shared evidence before the last
clone drops. The last drop sets the housekeeping stop flag. Subsequent
housekeeping publication is rejected without a new artifact, while existing
evidence reads and ordinary application publication continue against the same
shared metadata store.

`app::e2e_tests::embedded_postgres_router_lifetime_preserves_shared_metadata_for_every_role`
passes in 0.82 seconds with PostgreSQL configured and no runtime skip:
`/tmp/shardline-batched-deletion.D9r31c/test.log`. The owned schema and pools are
removed after the assertions. This extends the local router-lifetime proof to
the three roles; it does not establish every account layout or cancellation
of an active storage call. Strict server library/test Clippy, formatting and
diff checks pass. Production behavior and the version are unchanged.

### Raw-xorb rejection after request-session loss

A real PostgreSQL fixture terminates the original request's backend PID before
attempting a new raw-xorb upload. Before the correction, that upload succeeds
and the test fails (`/tmp/shardline-batched-deletion.a1QBCy/test.log`).

Raw-xorb uploads now check the same request-owned session before and after
staging and around object-store work. These checks never acquire a replacement
connection. Detected session loss returns the existing stale-fence error;
stale-fence HTTP responses now include `Retry-After: 1` with their existing 503
status. Calls without an HTTP request session retain their existing library
behavior.

The corrected configured test verifies the error, HTTP status/header, absence
of a new intent, and successful upload with a fresh invocation. All ten
publication cases pass in 3.18 seconds with no skip or panic:
`/tmp/shardline-batched-deletion.Y2mvNs/test.log`. The fixture's old zero-pass
matcher falsely matched `10 passed`; inspecting the complete log confirmed the
passing suite, and the matcher now requires a numeric boundary. Strict server
library/test Clippy, formatting and diff checks pass.

This does not atomically fence separate intent transitions or cancel a storage
call when ownership is lost during that call. Those phase boundaries remain in
the merge audit; no cluster-wide exactly-once claim or version bump is made.

### Request ownership lost during raw-xorb body staging

A separate configured test supplies an asynchronous request-body stream. Its
first poll terminates the original request session's PostgreSQL backend PID,
so admission has succeeded but staging has not yet finished. The upload rejects
the stale owner after reading the body, creates no upload intent, and leaves no
stored xorb. A fresh upload subsequently stores the xorb successfully.

All eleven configured publication cases pass in 3.20 seconds, with no runtime
skip or panic (`/tmp/shardline-batched-deletion.OijkoK/test.log`). Strict server
library/test Clippy, formatting and diff checks pass. This verifies the staging
boundary separately from owner loss before admission; active object-store calls
and atomic intent-transition ownership remain open. Production code and the
version are unchanged by this additional test.

### Raw-xorb owner loss after a real provider accepted the PUT

The test-support S3 proxy can now delay an accepted PUT's original response.
A configured PostgreSQL/MinIO test waits until that fault is actually injected,
then terminates the original request session's backend PID before releasing
the storage acknowledgement. Shardline rejects the lost owner and leaves the
upload intent in `Storing`, rather than reporting `Visible`. The test reads the
stored object and compares its bytes exactly with the uploaded xorb. A fresh
retry succeeds and advances the intent to `Visible`.

All twelve configured publication cases pass in 12.51 seconds, with no runtime
skip or panic (`/tmp/shardline-batched-deletion.6NOXYc/test.log`). The test uses
its own PostgreSQL schema and MinIO stack. Strict server/test-support library
and test Clippy, formatting and diff checks pass. An unchecked subtraction in
the test was corrected before this final fixture run.

The pause is a test-only provider fault; it adds no operator setting or runtime
worker. This demonstrates preservation of an already-applied remote effect and
unfinished intent at the tested acknowledgement boundary. It does not prove
cancellation of remote storage calls or atomic fencing of subsequent intent
transactions. Those remaining boundaries stay in the merge audit.

### Connection-bound upload transition primitive

The index now provides `transition_upload_intent_with_event_on_connection`
in the semantic `postgres::upload_intent_ownership` module. It locks the intent
row, verifies its current evidence and numeric metadata, validates the proposed
event's operation and object identity, and commits the state and event in one
transaction on the supplied connection. It never opens a replacement session.
Existing pool-based APIs retain their implementation and behavior.

A configured PostgreSQL test rejects an event for an unrelated object without
changing the intent, commits a valid transition, terminates the original
connection's backend PID, and verifies that the next transition fails while
state and saved evidence remain unchanged. It passes in 0.31 seconds with no
runtime skip (`/tmp/shardline-batched-deletion.7a70IG/test.log`). Strict index
library/test Clippy, formatting and diff checks pass.

This additive primitive is not yet wired into server upload coordination.
Request-bound creation, reads and transitions still need composition through
the original request session before the stronger upload ownership guarantee
can be claimed. It does not establish object-store presence or exactly-once
remote effects, and the version remains unchanged.

### Request-bound upload intent integration

The raw-xorb and native-shard coordinators now use a private request-aware index
adapter. With an HTTP request session, intent creation, verified reads and atomic
state/event transitions use its original PostgreSQL connection. The adapter
retains a typed owner-loss flag because CAS errors otherwise erase adapter error
types; the server returns `StaleResourceFence` (HTTP 503, `Retry-After: 1`).
Standalone calls without a request session retain the pool-based behavior.
Creation shares the existing implementation, including legacy evidence migration
and idempotency checks, rather than duplicating those rules.

All 188 configured PostgreSQL index cases passed after the creation refactor
(`/tmp/shardline-batched-deletion.q64cFl/test.log`, 38.16 seconds). All twelve
configured publication cases passed with the server composition
(`/tmp/shardline-batched-deletion.DdnTq8/test.log`, 12.46 seconds). These include
six native upload interruption boundaries and the real MinIO accepted-PUT
acknowledgement fault. The interrupted native recovery test now saves `Storing`
on the live original session before terminating it; it no longer depends on a
dead request being allowed to create an intent.

A separate adapter test terminates the original PostgreSQL backend PID and
independently verifies rejected creation, reads and transitions, unchanged
intent/evidence, and successful advancement with a fresh request session. It
passed without runtime skips in 0.34 seconds
(`/tmp/shardline-batched-deletion.JaNIbc/test.log`). Strict index/server library
and test Clippy passed. These results supersede the preceding primitive-only
integration limitation; they do not certify every mutation path, cancel an
already-active remote call, or establish exactly-once remote effects.

### Ownership-loss retry responses across protocols

A response regression test exposed that the default HTTP ownership-loss error
included `Retry-After`, while OCI omitted it and S3 classified it as an internal
failure. `StaleResourceFence` now uses the shared retryable-publication
classification. HTTP and OCI return 503 with `Retry-After: 1`; S3 returns 503
`SlowDown` with the same delay. The message instructs clients to retry with a
fresh request. The OCI error envelope and code remain unchanged.

The new test failed before the classification fix and passes afterward, checking
the delay, actionable message and protocol-specific response bodies. The error
test selection passes all 262 cases (including six test-invariant cases), with
no failed or ignored tests. This verifies response formatting, not actual
automatic retry behavior in every external client.

### Optimized deployment validation after upload ownership integration

The optimized CLI built from `9f38dc0c` reports version 1.11.3 and has SHA-256
`b4cf17301ad8caa2cc2a0a89a826617386139bbdc09fd607e06aabb7f2526049`.
Both deployment verifiers used pinned copies with that exact hash:

- `verify_maintenance_deployments.sh`: all eleven selected cases passed in
  145.691 seconds (`/tmp/shardline-maintenance-deployments.15pEMz/test.log`).
  Coverage includes restricted released/current commands, concurrent rebuild
  ownership, container replacement, remote-delete faults and mixed-version
  rollout/rollback against the released 1.11.3 binary.
- `verify_maintenance_protocol_faults.sh`: all twelve complementary cases passed
  in 136.151 seconds (`/tmp/shardline-protocol-faults.yjEnlB/test.log`). Coverage
  includes backend loss during uploads, network faults, clock skew, resumable
  LFS interruption and workload bursts.

Nextest reports twelve excluded cases in the first run and eleven in the second;
these are complementary filters, not runtime skips. Together they execute all
23 cases in the deployment binary, with no selected runtime skip. These are
current optimized-binary results for the named fixtures, not certification of
every deployment layout or closure of the remaining merge gates.

### Digest-checked file upload ownership and repository identity

The remaining `upload_file_stream` coordinator now uses the request-aware
intent adapter. Digest-checked file uploads create their baseline with the
same repository scope used for subsequent transitions. The additive public
`CasCoordinator::begin_upload_scoped` entry point exposes the existing scoped
admission implementation; `begin_upload` retains its default-scope behavior.

A configured PostgreSQL test terminates the original request session when the
body stream is first polled, after `Storing` was committed. It failed before
the composition change (`/tmp/shardline-batched-deletion.Udrl6p/test.log`).
Afterward, the request returns `StaleResourceFence`, keeps exactly the two
admitted evidence events and the `Storing` state, and publishes no file record.
A fresh request with a GitHub repository scope reaches `Visible`; all five
evidence events verify under that same scope, and the record is readable using
its scope. An initially unscoped read assertion in this new test was corrected
before the final verification.

All thirteen configured publication cases pass in 12.51 seconds with no runtime
skip (`/tmp/shardline-batched-deletion.9VBf8X/test.log`). This verifies the named
digest-file boundary as well as the existing native/raw publication fixtures.
All sixteen CAS coordinator tests also pass, including scoped identity,
concurrent duplicate uploads and interruption recovery. Strict CAS/server
library and test Clippy, formatting and diff checks pass.
Uploads without a supplied digest do not create these intents; their metadata
publication ownership remains a separate boundary. Immutable storage work may
already have run when ownership loss is detected.

### Plain file uploads: ownership loss before and during publication

File uploads without a supplied digest still create no upload intent. They now
check the original request session before reading the body and after ingestion.
If publication fails, another check on that same session distinguishes owner
loss from an error on a still-live session. No replacement connection is opened;
the publication transaction retains its existing backing-data validation.
Normal request uploads add two liveness queries, plus a query on failures that
have not already been classified by the intent adapter. Standalone calls without
a request session perform no such database query. These checks do not bound
network latency or cancel an already-active object-store call.

The configured plain-upload regression test failed before the fix with PostgreSQL
error `57P01` exposed as an index error
(`/tmp/shardline-batched-deletion.x55zYd/test.log`). The corrected test covers
owner loss before the first body poll and during body ingestion, verifies that
an already-lost request consumes no body, checks absent file metadata and zero
upload intents, and succeeds with a fresh session.

A second test holds a real PostgreSQL table lock and observes the upload's
publication transaction waiting on its blocker through `pg_blocking_pids`.
Only then does it terminate the original request session. The upload returns
`StaleResourceFence`, publishes no record, and a fresh request publishes the
same content after the blocker is released. This exercises loss after the
liveness checks, rather than relying on a check immediately before publication.

All fifteen configured publication cases pass in 12.57 seconds with no runtime
skip (`/tmp/shardline-batched-deletion.emvln5/test.log`). They include both new
plain-file tests and the existing digest-file, raw-xorb, native-shard and physical
publication fixtures. These named scenarios do not close the remaining write
and deployment audit or establish exactly-once remote storage effects.
Strict server library and test Clippy, formatting and diff checks also pass.

### Healthy upload owners preserve ordinary failures

A configured PostgreSQL test now checks the other side of the ownership-loss
classification. A body-stream I/O failure retains its original `InvalidData`
error and HTTP 500 response; a checksum mismatch retains
`ExpectedBodyHashMismatch` and HTTP 400. Neither receives `Retry-After`.
The failed uploads publish no file record, and the checksum attempt retains
exactly its two admission events in `Storing`. A subsequent valid upload succeeds
on the same request session, with its PostgreSQL backend PID unchanged.

All sixteen configured publication cases pass in 12.45 seconds with no runtime
skip (`/tmp/shardline-batched-deletion.o9b6Au/test.log`). Production code is
unchanged by this additional verification. Source inspection confirms that the
two normal file-upload liveness checks are outside the chunk loop; their number
does not grow with file size. This is a query-count property, not a measured
production latency or throughput guarantee.

### File recovery cannot infer success from a reused filename

The healthy-owner regression was extended to upload a later valid file at the
same name as a failed checksum attempt, age the unfinished intent, and run real
reconciliation. It exposed false completion: the old attempt became `Visible`
(`/tmp/shardline-batched-deletion.WmdkMW/test.log`).

Both backends now distinguish logical `record/` intents from physical object
keys. File publication follows the durable `Stored` checkpoint, so earlier
states remain unfinished regardless of a later file's presence. For eligible
default-scope file attempts, the shared semantic recovery helper streams the
candidate's bytes, checks length and SHA-256 against the saved identity, and
only then permits completion. Missing, unrelated or unverifiable bytes preserve
progress; a logical record key is never tested as if it were a physical CAS key.
Recovery writes no file metadata and starts no upload automatically.

All seventeen configured publication cases passed in 12.60 seconds with no
runtime skip (`/tmp/shardline-batched-deletion.uXMCwE/test.log`). A new PostgreSQL
case verifies successful recovery for matching bytes, unchanged `Stored` state
and evidence for unrelated bytes, and preservation when metadata is absent.
The failed-checksum regression now keeps its two original events in `Storing`.

Opaque scoped repository identities are not parsed or guessed. Exact scoped
recovery and large-file verification cost remain open. Verification reads the
eligible file's contents; streaming avoids a new whole-file buffer but does not
bound startup time. Unsupported or unavailable byte verification preserves the
intent with guidance. Existing signatures, records and public data types are
unchanged. The local positive-recovery fixture now supplies a real content
SHA-256 in place of its previous synthetic `sha256` placeholder.
All 22 local backend cases pass in 0.45 seconds, including matching recovery,
preservation of a checksum-failed `Storing` attempt and an unrelated `Stored`
attempt at the same filename.

### Released-server recovery of exact scoped file evidence

The earlier 19-test pass (`/tmp/shardline-batched-deletion.dsgFqn/test.log`)
does **not** prove rollback safety. Its process fixture waited for the listener,
but the released server binds before startup recovery completes. It could stop
the child before recovery touched the tested record. Expanding the fixture to
four unfinished states exposed this race: the stricter inspection check failed
in `/tmp/shardline-batched-deletion.ewKN5y/test.log`.

The fixture now waits for HTTP readiness and requires recovery logs for each
record. It creates a scoped file and a different default-scope file with the
same name, starts the actual released 1.11.3 server against the same database
and payload root, then compares each unfinished state and complete event history.
The corrected readiness-based run failed in
`/tmp/shardline-batched-deletion.BRPnNV/test.log`: released recovery changed the
new scoped `Created` intent to `Visible` using the unrelated default-scope file.
That run demonstrated a compatibility defect requiring protection before merge.
An initial `fsck` experiment also does not establish startup behavior because
that command opens stores directly without server reconciliation.

The fix installs a compatibility trigger through the existing PostgreSQL owner
setup and automatically during writable SQLite initialization. New default file
attempts use opaque `file-v2-` IDs; exact scoped evidence also protects arbitrary
intent IDs. Existing IDs, domain records and signed history are unchanged. The
guard suppresses state changes from older code for these new logical file
intents, causing the released atomic transition to append no event. Legacy
intents remain eligible for the released behavior.

Current writers identify their atomic transitions automatically. PostgreSQL uses
an intent-specific transaction-local setting and restores its previous value
before releasing a nested savepoint. Protected transitions add two fixed queries;
raw xorb/native/legacy transitions add none. SQLite puts an intent-specific marker
in a private table within the same transaction and removes it before commit;
other connections cannot observe it, and interruption rolls it back. These
markers are compatibility checks, not ownership, authorization or execution
proof. No tracker process, operator flag or new runtime grants are introduced.
An initial SQLite function-discovery prototype was rejected by the existing
trusted-schema protections and replaced without relaxing those protections.

The configured PostgreSQL publication suite passed 19 tests with the corrected
readiness-based fixture (`/tmp/shardline-batched-deletion.Z1q5RI/test.log`). It
checks all four unfinished states in scoped and default namespaces: the actual
released 1.11.3 server inspects all eight records and preserves their states and
complete event histories, while completing an unprotected legacy control record.
Current recovery subsequently completes Stored/MetadataCommitted records after
scope/content verification and preserves Created/Storing. The equivalent actual
released-server SQLite fixture passed within the 105-test local backend suite
(`/tmp/shardline-upload-rollback-local.log`). Both namespaces remain readable.
The final SQLite rollback fixture also verifies the marker table is empty after
current recovery (`/tmp/shardline-upload-rollback-local-final.log`, one test).
All 189 configured PostgreSQL index tests passed
(`/tmp/shardline-batched-deletion.kqJ7vN/test.log`), including nested marker
restoration and a quoted schema containing template tokens. All 316 selected
SQLite index tests passed (`/tmp/shardline-upload-guard-sqlite-index.log`), including
read-only metadata, schema initialization and existing inventory behavior.
Strict index/server Clippy, workspace formatting and whitespace checks passed.
These cases establish this guard's behavior, not every deployment layout or
closure of the full rollback gate. Large-file recovery startup cost and the
remaining acceptance gates remain open.

### Exact scope identity for new file recovery

New scoped file intents encode their validated repository components in the
existing repository string as `scope-json-v1:` followed by structured JSON.
This string is part of the existing authenticated operation identity. It keeps
slash-containing components separate and distinguishes an absent revision from
the literal revision `-`. Default-scope evidence retains `default`; existing
opaque labels and signatures are never rewritten. No new database column,
public data type, configuration or process is added.

Local file admission now uses the scoped coordinator entry point from its
baseline onward, matching PostgreSQL. Recovery decodes exact new scope evidence,
reads the corresponding record namespace, checks its typed scope and bytes,
and then advances progress. Legacy scoped labels remain unresolved because their
slash-separated form can represent more than one valid scope.

All eighteen configured PostgreSQL publication cases passed in 12.83 seconds
without runtime skips (`/tmp/shardline-batched-deletion.UbiyH1/test.log`). The
new case exercises two distinct scopes whose old labels collide, absent versus
literal-dash revisions, successful recovery in each actual namespace, rejection
of bytes present only in another scope or the default namespace, unchanged file
metadata, and unchanged evidence for an ambiguous legacy intent.

All 104 local backend tests passed in 1.29 seconds, including new scoped upload
and recovery for absent and literal-dash revisions. Strict server library/test
Clippy, formatting and diff checks pass. The released 1.11.3 digest enum already
supports the existing canonical encoding used here, but source inspection alone
does not establish old-server interpretation of these new scope strings. The
mixed-version audit and large-file startup verification cost remain open.

## File recovery outside readiness startup

Normal public-router startup now checks upload evidence and candidate metadata,
then runs eligible file-byte verification in one task owned by the router's
lifetime. It uses the existing process and stop signal, not a worker PID, timer,
task ID or operator service. Shutdown preserves unfinished evidence; already
running storage I/O may finish. The public standalone backend constructor keeps
its eager recovery behavior. The existing 30-second eligibility rule is retained.

The 65 MiB held-read fixtures exposed a real multi-container reader defect:
records containing different container hashes fell through to individual-chunk
keys. The reader now groups adjacent container terms, authenticates one container
at a time and decodes bounded physical chunks. Logical file offsets are independent
of container-relative chunk ranges. Selected ranges, repeated terms and mixed
individual-chunk fallback remain supported. No domain record is rewritten.

Executed checks for this change:

- Five configured startup fixtures pass in 16.55 seconds, with no runtime skips
  (`/tmp/shardline-batched-deletion.WnLRJh/test.log`). Both SQLite and PostgreSQL
  exercise 65 MiB files for All, Api and Transfer roles. The storage read is held
  while `/readyz` succeeds; release then reaches verified completion. Separate
  cases cover eager standalone recovery, router-stop/abort with fresh recovery,
  and preserved evidence after checksum mismatch or corrupt stored bytes.
- All 51 download-stream tests pass, including cross-container ranges, selected
  multi-chunk terms, repeated references, individual-chunk fallback, inconsistent
  authenticated lengths/ranges and one-work-slot streaming
  (`/tmp/shardline-multi-container-stream-tests.log`).
- All 59 object-store and 105 local-backend cases pass
  (`/tmp/shardline-file-recovery-object-tests.log` and
  `/tmp/shardline-file-recovery-local-tests.log`). The local group executes the
  actual released 1.11.3 server rollback fixture.
- All 19 configured PostgreSQL publication cases pass in 12.47 seconds, with no
  runtime skips (`/tmp/shardline-batched-deletion.TrTAnN/test.log`), including
  actual released-server preservation of new unfinished file evidence.
- Strict server library/test Clippy passes
  (`/tmp/shardline-file-recovery-clippy.log`); formatting and diff checks pass.

These fixtures establish that file checksum reads no longer block normal router
readiness. They do not bound metadata inventory scans, total verification cost,
remote-storage latency or all startup work. A verification warning preserves the
intent; readiness is not certification of every stored file. Whole-file formats
unsupported by this streaming verifier remain unconfirmed. Broader deployment,
failure and capacity gates remain open; the version is still 1.11.3.

## Optimized deployment verification after responsive recovery

An optimized CLI containing the responsive recovery and multi-container reader
changes builds successfully in 3m 06s and reports `shardline 1.11.3`. Its SHA-256
is `1133a529de2a49bf1faf0dbe580418612332cb7fa5a5f0e2d54c2303a1b04b95`.
Both deployment verifiers copy and test these exact executable bytes; neither
uses the earlier optimized CLI that predates file-recovery and rollback fixes.

- `verify_maintenance_deployments.sh` passes all 11 selected drills in 144.611
  seconds (`/tmp/shardline-maintenance-deployments.YemYge/test.log`). This includes
  released/current rollout and rollback, uncertain remote deletes, saved replies
  across database outages, existing restricted roles, concurrent rebuilds and
  container replacement with graceful shutdown.
- `verify_maintenance_protocol_faults.sh` passes all 12 selected drills in
  138.084 seconds (`/tmp/shardline-protocol-faults.GR7Ske/test.log`). This includes
  database/storage loss during upload, read fallback, network faults, protocol
  interruption, clock skew and the selected workload burst.
- `verify_maintenance_local_permissions.sh` passes the five released/current
  command comparisons and fully read-only SQLite fsck
  (`/tmp/shardline-local-permissions.mr1Hrd/`). Existing output, status, root
  permissions and read-only evidence remain intact; recording failure warns
  without making fsck fail or creating new completion history.

There are no runtime skips in the selected deployment drills. Nextest's skipped
counts refer to tests outside each complementary filter, not skipped test bodies.
These results strengthen the named deployment evidence; they do not close every
phase, library path, deployment layout or capacity requirement in the eight gates.

## Native shard reuse scope audit: failure reproduced

The next audit fixture uses actual valid xorb/shard bytes and uploads the same
native shard twice in the default namespace, then twice in each of two repository
namespaces. It also requires the file bytes to be readable in every namespace.
The current local backend fails at the first repository publication with
`UploadIntentConflict` (`/tmp/shardline-native-upload-scope-reuse.log`). This is
a runtime failure, not an observation timeout or skipped fixture.

Both server backends derive native shard intent IDs solely from the request body
hash. Scoped intent admission verifies the repository against existing history;
the default upload has already occupied that ID. Resolving this must preserve
valid retries, distinct typed scopes, existing unfinished history and mixed-version
interpretation. PostgreSQL and released-server evidence for the eventual fix are
still required. The successful file recovery and optimized deployment drills
above do not close this native publication case.

## Native repository reuse and migration: fix verified

The preceding failure is resolved. New native uploads use stable private IDs
derived from request content and exact repository identity, including the default
namespace. Domain object keys, file records and content hashes are unchanged.
Default uploads also need the new identity: an older scoped upload can already
occupy the historical body-only ID. Existing rows and authenticated history are
never rewritten or silently settled by a fresh request.

The semantic `upload_identity` module supplies the exact repository encoding to
both file and native upload callers. The historical flat spelling is retained only
in compatibility fixtures. No new public type, user task ID, configuration,
process, runtime grant or database column is required. The existing owner-run
PostgreSQL setup refreshes its compatibility guard; writable SQLite initialization
adds the native guard automatically alongside the existing file guard.

Both backend fixtures publish the same valid shard twice in each of six distinct
scopes, including formerly colliding labels and absent versus literal-dash
revisions. They verify readable bytes in every namespace, distinct histories and
exactly five events per history after retries. An older unfinished scoped intent
with the historical body-only ID remains unchanged while default and new scoped
publication succeed.

Separate actual released-server fixtures cover sixteen new unfinished intents
per backend: all four checkpoints, default and scoped identities, and present
versus missing physical request objects. They wait for `/readyz`, require released
logs to show every intent was visited, and compare complete history and state.
Current writers can still advance protected rows. This is writer-protocol
compatibility evidence; the synthetic checkpoint cases do not prove domain
publication. Domain publication is exercised separately by the valid-shard tests.

Executed checks:

- All 21 configured PostgreSQL publication cases pass in 12.73 seconds, without
  runtime skips (`/tmp/shardline-batched-deletion.0rW9eJ/test.log`). Existing owner
  loss, interrupted native publication, file recovery and file rollback cases
  remain included.
- All 107 local-backend cases pass in 2.14 seconds
  (`/tmp/shardline-native-scope-local-regression-final.log`), including the new
  native scope and released-server tests and existing file rollback coverage.
- All 189 configured PostgreSQL index cases pass in 42.38 seconds
  (`/tmp/shardline-batched-deletion.MQBgdc/test.log`).
- All 316 SQLite index cases pass in 169.41 seconds
  (`/tmp/shardline-native-scope-sqlite-index.log`).
- Strict index/server library and test Clippy passes
  (`/tmp/shardline-native-scope-clippy.log`); formatting and diff checks pass.

The earlier optimized deployment drills test the executable before this native
identity fix. A fresh optimized CLI containing the fix builds in 3m 03s, reports
`shardline 1.11.3` and has SHA-256
`3f839d2c9717fbb696f73fa3f445645e41886f362c52a52e31faec0b704da792`.
Both deployment verifiers confirm their pinned copies have these bytes:

- All 11 maintenance deployment drills pass in 142.199 seconds
  (`/tmp/shardline-maintenance-deployments.IZ2psG/test.log`).
- All 12 protocol fault drills pass in 139.628 seconds
  (`/tmp/shardline-protocol-faults.NzTc8D/test.log`).
- The five restricted local command comparisons and fully read-only SQLite fsck
  pass (`/tmp/shardline-local-permissions.WSTSFe/`).

There are no runtime skips in the selected deployment cases. The eight merge
gates retain their remaining requirements; the version remains 1.11.3.

## Current ordinary-command output and fresh-scan audit

The current optimized executable (`3f839d2c9717fbb696f73fa3f445645e41886f362c52a52e31faec0b704da792`)
passes `verify_maintenance_large_diagnostics.sh` against actual released 1.11.3
(`/tmp/shardline-large-diagnostics.sLWj9V/`). A 4,096-object inventory produces a
1,490,946-byte retention export and a 1,376,258-byte orphan export. Released and
current stdout, stderr, status, inventory keys and stable diagnostic fields match;
every synthetic payload byte remains unchanged. Countdown fields are excluded
from comparison because wall time changes them.

After the recorded completion, the fixture adds another orphan. The next ordinary
invocation discovers and marks all 4,097 objects, preserves their bytes and saves
a distinct completion. A prior saved result is not reused as a fresh scan.

Code review confirms the relevant distinction: ordinary commands in
`maintenance/shared_inline.rs` call `entry::run_direct` with their original output
path. The full stdout/stderr files in `maintenance/capture.rs` belong to the
explicit optional runner. Thus the 64 KiB automatic history-artifact admission
limit does not impose that limit on ordinary command output or these exports.
The explicit runner's capture and overall physical storage cost retain their
separate audit requirements; logical admission limits are not disk quotas.

The PostgreSQL history access functions also explicitly check row-policy access
before their schema-wide operations. The existing row-policy fixture verifies
refusal and preservation of ordinary startup work, rather than tenant-scoped
history. It does not certify arbitrary tenant policies. These findings refine
the remaining capacity and deployment audit; they do not close all eight gates.

### Canonical xorb retry identity and recovery

A concrete regression fixture reproduced `UploadIntentConflict` when a valid
footerless xorb was uploaded first and its exact normalized representation was
retried. The immutable intent recorded the wire length, whereas storage added
the canonical footer. That also made physical-length recovery evidence wrong
for an interrupted footerless upload.

Both backends now determine the canonical stored length before admission and
use the stable private identity `xorb-v2-{root}-{canonical_length}`. Existing
body-only identities and their signed events remain untouched by the new
request. Storage keys, logical hashes and public responses remain unchanged.
Canonical staged files are validated without whole-file buffering; footerless
file inputs retain the existing 16 MiB compatibility normalization cap. Work
uses the existing bounded blocking pool, now shared through the semantic
`admission::blocking` module. PostgreSQL rechecks the original request owner
after that work and retains its connection for intent writes.

The SQLite upgrade installs an additional additive xorb recovery trigger.
PostgreSQL owner setup refreshes the existing recovery function to recognize
the new identity prefix. Current atomic writers supply the existing marker;
released writers cannot change these histories during rollback. The marker
remains a compatibility mechanism, not a resource fence or execution lease.

Validation on the final sources:

- 181 selected local-backend, download-stream and admission tests pass
  (`/tmp/shardline-canonical-xorb-stream-regression.log`). Nextest's other
  3,249 tests are excluded by the explicit filter, not runtime fixture skips.
- 23 configured PostgreSQL publication tests pass without fixture skips
  (`/tmp/shardline-batched-deletion.KR9oBU/test.log`), including original-owner
  loss and provider acknowledgement interruption.
- Both backends exercise footerless/canonical retries, preserve legacy history,
  and reconcile all four unfinished checkpoints against canonical stored bytes.
  Only `MetadataCommitted` is completed from that recovery evidence; earlier
  states remain retryable and the subsequent validated upload finishes them.
- The actual released 1.11.3 startup fixtures now visit 24 protected checkpoints
  per backend: the previous 16 native shard cases plus eight xorb cases, covering
  all four unfinished states with physical bytes present or missing. States and
  complete signed event sequences remain unchanged; current writers can advance
  them afterward.
- All 311 adapter unit tests and strict Clippy for server, index and adapter
  libraries/tests pass. These checks do not prove deployment health or close
  the remaining acceptance gates.
- All 189 configured PostgreSQL index tests pass without runtime fixture skips
  (`/tmp/shardline-batched-deletion.YpYwdv/test.log`); the two SQLite upload-intent
  identity/evidence boundary tests also pass
  (`/tmp/shardline-canonical-xorb-index-local.log`).

### Benchmark command compatibility matrix

The optimized executable before this canonical-xorb fix, with SHA256
`3f839d2c9717fbb696f73fa3f445645e41886f362c52a52e31faec0b704da792`, passes
`verify_maintenance_benchmark_compatibility.sh` against released 1.11.3
(`/tmp/shardline-benchmark-compatibility.A5z9Oc/`). Concurrency 1, 32 and 128,
with three iterations each, preserve scalar JSON schema and all nonperformance
counters/settings. Released fsck accepts each generated workload; current and
released ordinary fsck diagnostics agree. After current fsck there is exactly
one completed receipt and no payload history directory, so the benchmark itself
does not create maintenance receipts. Timing/RSS files record fixture costs;
they do not establish an SLO or a performance improvement. This binary pin is
retained explicitly rather than attributing that run to a later executable.

### Optimized deployment checks after canonical xorb correction

The optimized build for `9fc1549f` finishes successfully in 3m19s and reports
`shardline 1.11.3`. Its SHA256 is
`1e41b44dfb4ecefd0da9c863e903171feb30ed547b3691e4f973a0e6de0db8e0`.
Both complementary deployment scripts pin these same bytes:

- `verify_maintenance_deployments.sh`: all 11 selected scenarios pass in
  145.634s (`/tmp/shardline-maintenance-deployments.sOdMGt/test.log`), including
  actual mixed-version rollout/rollback, restricted commands, replacement,
  competing ownership and remote-delete faults.
- `verify_maintenance_protocol_faults.sh`: all 12 selected scenarios pass in
  146.454s (`/tmp/shardline-protocol-faults.wGy3fW/test.log`), including protocol
  interruption, backend/connection loss, network faults and clock skew.
- `verify_maintenance_local_permissions.sh`: all five released/current command
  forms match under a 0555 root. Fully read-only SQLite fsck also preserves
  released output/status and existing evidence; unavailable optional recording
  warns without reporting completion (`/tmp/shardline-local-permissions.PdGhgZ/`).

No configured scenario skips at runtime. The two Nextest filters select
complementary halves of the same 23-test binary; their excluded-test counts are
not skipped deployment fixtures. These results update the executable evidence
without closing the remaining compatibility, phase and capacity audit.

The operator guide now presents the existing process model, practical benefits,
upgrade steps, inspection and recovery actions in 139 lines rather than 263.
Pagination, pending-reply scan details and quantitative history budgets live in
the CLI reference, with direct links from the short guide. The guide still
states the PostgreSQL owner-setup prerequisite, automatic SQLite setup, old-GC
rollback restriction and absence of a cluster-wide exactly-once guarantee.

### Legacy chunk read request budget

A real HTTP fixture for two existing individual chunks found six requests:
two xorb routing probes, two chunk HEADs and two data GETs. Both FixedChunkV1
and individually compressed XorbCdcV1 records showed the same cost. A selected
file range used three requests. The released `v1.11.3` reader's source skips
xorb probes for these multi-hash records and performs HEAD plus GET per read
chunk; that comparison is a source audit, not an executed released HTTP fixture.

The new additive S3 `stream_range_if_present` API checks full object length and
returned range from the GET response. The server uses that response instead of
a separate chunk HEAD, preserving its typed missing-object and stored-length
errors. Length now belongs to the response supplying the bytes, rather than a
separate earlier observation. The existing `stream_range` API is unchanged.

The HTTP fixture then passes at four requests for two chunks and two requests
for a selected file range, for both representations. It checks byte-exact
output, no reads of unselected chunk objects, trailing-byte rejection and
missing-object behavior. A direct partial-object GET also verifies the full
object length from Content-Range and returns only the requested bytes with one
HTTP request. This restores the released source's request budget for the named
legacy formats without removing multi-container routing.

All 52 download-stream tests pass
(`/tmp/shardline-legacy-read-stream-regression.log`), as do all 163 selected S3
storage tests (`/tmp/shardline-legacy-read-storage-regression.log`). Strict server
and storage Clippy passes (`/tmp/shardline-legacy-read-clippy-final.log`). The
HTTP fixture uses an owned local provider and dummy credentials; these counts
do not establish cloud latency, throughput or a production SLO. Larger archives
and broader deployment costs remain separate gate requirements.
The final focused fixture also passes after the lint corrections
(`/tmp/shardline-legacy-read-cost-final.log`).

### Initial chunk failures precede HTTP success

Optimized deployment validation of the checked-GET change exposed a regression:
the MinIO partition drill returned initial HTTP 200 headers before its lazy
first fallback chunk read failed. The complementary protocol suite passed 11
of 12 scenarios; the partition failure reproduced in isolation. The separate
11-scenario deployment suite passed. These results do not constitute a passing
23-scenario run for that executable.

The fallback reader now reads and validates its first chunk before returning
the stream. Later chunks remain lazy; the change adds no provider requests or
whole-file buffering. A missing initial chunk now fails stream construction,
with a regression assertion that failed before this correction.

All 52 streaming tests and strict server Clippy pass after the correction
(`/tmp/shardline-legacy-read-eager-first-tests.log` and
`/tmp/shardline-legacy-read-eager-first-clippy.log`). The optimized executable
still reports version 1.11.3 and has SHA256
`cae74a6f813de4233065b38e32a048c4be5bb7eb1c328ced5f50e57ed3dab207`.
Its isolated partition drill passes: GET fails with 500 during the confirmed
partition and recovery succeeds after reconnection
(`/tmp/shardline-eager-first-partition.JcmidC/test.log`). This targeted replay
does not replace the outstanding broader acceptance review.

The complete complementary protocol-fault suite subsequently passes all 12
configured scenarios on that same optimized executable
(`/tmp/shardline-protocol-faults.zb1kkZ/test.log`, 136.394 seconds). This includes
the partition regression and rejects runtime fixture skips. These results
precede the public-library ownership changes described below.

### Automatic ownership for public PostgreSQL uploads

A configured library fixture reproduced public `PostgresBackend::upload_xorb`
completion while an exclusive GC barrier was held. Public file and xorb uploads
now establish their request session automatically before executing upload work.
Calls within the matching HTTP session reuse it; a lost session is rejected
without reacquisition. Cancellation retains the existing close-on-drop lock
release behavior. No public method signature or configuration changes.

The new tests verify waiting for exclusive GC, successful completion after
release, and rejection of both public upload methods after actual termination
of the existing database owner. The configured publication suite passes all 25
tests, including actual released-server recovery, with no runtime fixture skips
(`/tmp/shardline-batched-deletion.MIB6wX/test.log`). Strict server Clippy passes
(`/tmp/shardline-library-ownership-clippy.log`). The earlier 24-test attempt
omitted the released binary environment setting and was rejected by the fixture
wrapper for runtime skips; it is not the complete suite result.

This strengthens the public upload boundary. Lower-level metadata-only methods
still require caller ownership and physical-reference validation. Broader cost,
deployment and mutation-phase gates remain open.

### Embedded cancellation and configured PostgreSQL workload cost

The cancellation fixture holds an automatically established embedded session,
verifies that exclusive GC waits, cancels the owning task, then verifies that
GC acquires ownership. The complete configured publication suite passes all 26
tests, including this fixture and actual released recovery, without runtime
skips (`/tmp/shardline-batched-deletion.tD6x7B/test.log`, 12.65 seconds). Strict
server Clippy passes (`/tmp/shardline-library-cancellation-clippy.log`). Initial
test linking failed with a bus error and then an explicit output-quota error;
moving the owned incremental build cache off quota-limited `/tmp` allowed the
tests to run. Those linker failures provide no application-behavior evidence.

The optimized executable with automatic public upload ownership reports 1.11.3
and has SHA256
`51040330bdc95e78f446d145b26ef5134572c1aa130a9312fbf99f3b4504976d`.
All 11 selected deployment scenarios pass on it
(`/tmp/shardline-maintenance-deployments.5gRi5F/test.log`, 143.502 seconds).
This includes mixed-version rollback, saved-reply recovery, competing rebuilds
and tracker replacement. The previous 12-scenario protocol result belongs to
the earlier executable, as recorded above.

`scripts/shardline/verify_maintenance_postgres_benchmark.sh` compares that
executable with actual released 1.11.3 using one owned PostgreSQL 16 container,
fresh databases per version/concurrency, local payload storage and the existing
configured full benchmark. Each run uses three iterations, a 1 MiB base,
64 KiB mutation and 64 KiB chunks. Concurrency 1, 32 and 128 all preserve every
non-performance report counter and setting after private paths are removed.
Backend assertions require PostgreSQL metadata and local object storage.
The setup installs released migrations first and performs normal owner setup
for the current binary. Initial fixture attempts failed during database setup
and JSON parsing of startup logs; the corrected fixture uses TCP readiness and
the same error-level logging setting for both binaries.

| Concurrency | Released wall / peak RSS | Current wall / peak RSS |
| --- | --- | --- |
| 1 | 0.11 s / 33,884 KiB | 0.16 s / 34,836 KiB |
| 32 | 0.49 s / 397,552 KiB | 0.43 s / 102,648 KiB |
| 128 | 1.17 s / 731,704 KiB | 1.27 s / 293,596 KiB |

Reports, exact binary hashes and measurements are preserved in
`/tmp/shardline-postgres-benchmark.igzYKe`. These are single fixture measurements,
not a distributed throughput claim or production SLO. The test supplies stronger
evidence for the public PostgreSQL upload workload; larger inventories, remote
storage, long operations and remaining merge gates still require review.

### Corrected report-phase fault and current protocol replay

Review found that the ordinary-command report-denial trigger also rejected the
earlier checkpoint artifact. Its report-denial half therefore established
admission fallback behavior, not the claimed post-work report failure. The
completion-record denial half was already distinct. The trigger now rejects
only a materialized result body beginning `maintenance.success:` while allowing
the checkpoint. Assertions require all three admission records to persist,
exactly one materialized artifact after report rejection or two after completion
record rejection, and no final completion record in either case.

The corrected fixture passes all eight fsck/rebuild/lifecycle-repair/GC-mark
and report/acknowledgement combinations
(`/tmp/shardline-report-phase-correction.log`, one test, 40.08 seconds).
Original success and exit status are preserved, history remains unfinished,
unfinished evidence survives retention, and unrelated evidence is unchanged.
This is a test correction, not a production implementation change; it does not
establish every storage-copy, PostgreSQL or process-interruption phase.

The complete complementary 12-scenario protocol suite now also passes on the
automatic-public-ownership executable with SHA256
`51040330bdc95e78f446d145b26ef5134572c1aa130a9312fbf99f3b4504976d`
(`/tmp/shardline-protocol-faults.zJf1Lo/test.log`, 146.179 seconds). Together with
the 11-scenario deployment result above, all 23 configured scenarios pass on the
same executable. Runtime fixture skips are rejected; excluded Nextest filters
select the complementary half. The version remains 1.11.3.
Strict CLI Clippy also passes after the test correction
(`/tmp/shardline-report-phase-clippy.log`).

### PostgreSQL receipt write faults at each durable boundary

The configured receipt fixture injects real PostgreSQL trigger failures at
admission records 0, 1 and 2, the materialized result and completion record 3.
Each case reopens history, rejects a completed/failed outcome, verifies every
earlier record and the missing rejected record, and confirms that finished-run
retention preserves its manifest. Direct database queries also check exact
record and materialized-artifact counts, so a payload-history fallback cannot
satisfy the fixture accidentally. No application work is replayed by this test.

The checked-in verifier
`scripts/shardline/verify_maintenance_postgres_receipt_phases.sh` creates an owned
PostgreSQL 16 container, installs actual released migrations, performs current
owner setup and runs the exact named test. Runtime skips, panics and a missing
or renamed test fail verification. The final verifier passes all five cases in
one test (`/tmp/shardline-postgres-receipt-phases.ct1l7r/test.log`, 29.33 seconds).
Strict CLI Clippy passes (`/tmp/shardline-postgres-receipt-clippy.log`). The first
compile attempt was rejected by SQLx's dynamic-SQL audit requirement;
fixture-only DDL now explicitly marks generated hex identifiers and fixed
predicates as audited.

This establishes the shared metadata receipt's admission/report/acknowledgement
behavior. It does not replace interruption tests around actual external effects,
storage-copy phases, controller death or ownerless domain writes. Those remain
separate requirements in the merge gates.

### Storage copy after destination mutation with result-recording faults

The new ordinary-CLI fixture copies an actual payload, then injects destination
history failure at either report materialization or final completion record.
Both cases retain the successful copy status and output, byte-exact destination
payload, three admitted records and unrelated evidence. The final completion
record is absent and shared inspection reports `needs_reconciliation`, with
guidance to inspect affected data and original command logs. Retention preserves
the unfinished run.

After removing the recording fault, the test adds a new source object and runs
the read-only `history` command at the destination. The new object remains
uncopied, demonstrating that inspection does not replay the prior copy merely
because its receipt is incomplete. These checks use the pinned optimized binary
with SHA256 `51040330bdc95e78f446d145b26ef5134572c1aa130a9312fbf99f3b4504976d`.
The focused Nextest scenario passes both cases
(`/tmp/shardline-storage-copy-postwork-final.log`, 0.176 seconds). Strict CLI
integration-test Clippy passes
(`/tmp/shardline-storage-copy-postwork-final-clippy.log`). Test helpers use normal
Rust modules under `maintenance_support`, without path attributes.

This supplements the existing admission-failure copy fixture. It establishes
local destination-metadata faults after actual copy effects, not killed-copy
processes, every remote-provider phase or live-snapshot consistency. The operator
guide now gives the specific action for a successful copy with missing history.

### Automatic GC coordination for public local uploads

A direct local xorb upload reproduced successful completion while exclusive GC
held the filesystem barrier (`/tmp/shardline-local-library-ownership-red.log`).
Public `LocalBackend::upload_file` and `upload_xorb` now acquire the existing
shared barrier before upload work. The backend retains an absolute metadata
root so this lock follows its metadata even if a library caller changes working
directory; local metadata with a remote object adapter uses that metadata root
too. Streamed HTTP methods keep their existing request-owned outer barrier.
No public signature, domain encoding, configuration or process is added.

New tests verify xorb waiting, file waiting before any payload write and byte-exact
completion after GC releases ownership. A cancellation fixture stages physical
file bytes while publication is held, proves that exclusive GC still waits,
cancels the actual public upload, then verifies that GC proceeds and file metadata
remains unpublished. Staged immutable bytes may remain as unreferenced data;
cancellation does not authorize replay or fabricate publication.

All 37 selected local-backend tests pass with actual released 1.11.3 recovery and
no runtime fixture skips (`/tmp/shardline-local-library-ownership-final.log`,
1.93 seconds). Strict server Clippy passes
(`/tmp/shardline-local-library-ownership-clippy.log`). The earlier 35-test run
omitted the released binary environment setting and skipped the two rollback
fixtures; it is not the complete configured result. Optimized workload validation
for this local ownership change remains pending.

### Optimized workload and permission checks after local ownership

The optimized build completes in 2 minutes 44 seconds
(`/tmp/shardline-local-library-ownership-release.log`) and still reports 1.11.3.
Its pinned SHA256 is
`f66ee56e6c07669e9110efc3c35d00835f61f70aabf5b205c2598c8faf401ab4`.
The existing released/current local benchmark verifier passes at concurrency
1, 32 and 128, preserving every non-performance counter and setting, report
structure, released reads and ordinary fsck stdout/stderr. Each current workload
root records exactly the expected single fsck completion and no payload task
directory (`/tmp/shardline-benchmark-compatibility.SfIQ2r`).

| Concurrency | Released wall / peak RSS | Current wall / peak RSS |
| --- | --- | --- |
| 1 | 0.08 s / 35,180 KiB | 0.08 s / 35,444 KiB |
| 32 | 0.43 s / 252,888 KiB | 0.46 s / 284,516 KiB |
| 128 | 1.39 s / 633,824 KiB | 1.44 s / 638,588 KiB |

These single local fixture measurements support compatible workload results,
not a general speed improvement or production SLO. The existing permission
verifier also passes on this executable: fsck, rebuild, both repair forms and GC
preserve released output/status under a 0555 root without new root entries or
permission changes. Fully read-only SQLite fsck preserves output/status and
existing evidence while warning about unavailable optional recording
(`/tmp/shardline-local-permissions.oNZN7H`). These checks cover CLI commands,
not every embedded-library filesystem layout. The previous 23 deployment-fault
results belong to the earlier executable, whose hash is recorded above.

### Complete maintenance CLI suite after local ownership

All 21 current `maintenance_e2e` scenarios pass on the pinned optimized executable
with SHA256 `f66ee56e6c07669e9110efc3c35d00835f61f70aabf5b205c2598c8faf401ab4`
(`/tmp/shardline-local-owned-release.7D3Ad9/maintenance-cli.log`, 4.088 seconds).
Nextest reports zero skipped tests. This includes the new post-copy result faults,
existing controller death, ordinary GC interruption after effects/report,
fresh-run allocation, same-process execution, explicit configuration, non-UTF8
paths and restricted-root behavior. These executable results supplement the
focused local upload ownership and cancellation tests; broader deployment
and remaining merge-gate requirements still need their own evidence.

## SQLite metadata with S3 payloads: startup history

The real MinIO fixture exposed a supported-layout gap: startup completed but
SQLite metadata with S3 payloads had no saved startup history. Tracking now binds
to the actual local metadata backend, and read-only inspection finds its existing
SQLite database independently of the payload adapter. Inspection still does not
initialize a missing deployment; the legacy raw-S3 constructor is unchanged.

`sqlite_s3_startup_records_history_in_metadata_for_every_role` passes for All,
Api and Transfer roles. Each has one completed startup sweep owned by the server
PID, physical task artifacts in SQLite, no task objects in S3 and no separate
maintenance directory. The fixture failed before the fix and passed afterward
in 2.363 seconds. Both inspection regression tests pass; strict server Clippy
with all features, library and tests also passes.

The preceding optimized executable, SHA-256
`f66ee56e6c07669e9110efc3c35d00835f61f70aabf5b205c2598c8faf401ab4`,
also passed all 23 deployment scenarios: 11 in 143.744 seconds and 12 protocol
fault scenarios in 139.711 seconds, with no runtime skips. These runs precede
the SQLite/S3 fix and do not establish full deployment coverage for that fix.

## Embedded local upload cancellation and restricted directories

The five `local_backend::tests::library_ownership` tests pass in 0.36 seconds.
The new queued-cancellation fixture holds real exclusive filesystem ownership,
starts both public upload methods, confirms they remain pending and cancels
them. Neither writes a payload. After releasing the original owner, another
exclusive GC acquisition and a fresh file upload succeed.

The restricted-directory fixture runs as UID 1000, keeps SQLite WAL/SHM files
alive and creates the normal barrier before restricting the metadata root to
0555. A real attempt to create a new root entry fails. Public file and xorb
uploads still succeed, file download returns the original bytes and root
permissions remain 0555. Cleanup restores permissions. This proves the layout
with an existing writable barrier; it does not prove compatibility when that
barrier is absent or itself read-only.

## Optimized deployment checks after the SQLite/S3 fix

The optimized binary built from `c3be89e0`, SHA-256
`2b13ae613c9acdd6546fba9d77ab9e035b98355e8559fe4c01a11741d2a33962`,
reports version 1.11.3 and passes all 23 deployment scenarios. The deployment
verifier runs 11 in 144.913 seconds; the protocol-fault verifier runs the other
12 in 140.949 seconds. Their complementary filters exclude the other group's
tests; no selected scenario skips at runtime. Each verifier pins its own copy
of the binary and uses disposable owned Docker fixtures.

Diagnostics: `/tmp/shardline-maintenance-deployments.XWkd0l/test.log` and
`/tmp/shardline-protocol-faults.KU2CX0/test.log`. These are current evidence for
the named startup, ownership, replacement, remote-delete, rollout/rollback,
network-partition and protocol-interruption cases, not closure of every gate.

## Released/current storage-copy size boundary

`verify_storage_migration_size_compatibility.sh` compares the same optimized
binary with actual released 1.11.3. Both copy a 64 MiB object byte-for-byte;
both reject a 65 MiB object without creating its destination payload. Stdout,
stderr and exit status match at both sizes. Both dry-run inventories accept
the 65 MiB object, report its byte count and create no payload, with identical
output. Diagnostics: `/tmp/shardline-storage-size.Ftw2jM`.

The 64 MiB buffered-object limit also appears in the `v1.11.3` source. The CLI
reference now documents it. This establishes the existing boundary and its
preservation; it does not measure production throughput or long-operation cost.

## Ordinary storage-copy process interruption

`maintenance_support::storage_copy_interruptions::killed_storage_copy_preserves_effects_without_completion_or_inspection_replay`
uses the ordinary optimized `storage migrate` command, without the optional
worker. Each owned fixture has 256 local objects and isolates private scratch
with `TMPDIR`. After three durable admission records, a real SQLite writer
holds the private completion journal. The test observes a complete destination
object and kills the process either before its report exists or after the exact
successful report artifact exists. The first cut checks report absence after
process death; the second also checks live output reported all 256 insertions.

Both cuts verify the original command PID, byte-exact copied data, three
remaining journal records, absence of the shared completion record and one
`needs_reconciliation` result. After adding another source object, the ordinary
read-only history command still reports that unresolved run and does not copy
the new object. Completed-history pruning preserves the unfinished run.

The initial named-object observation could reach that object only after the
report was saved. The corrected fixture observes the first visible committed
object, rather than assuming inventory order. Its phase assertions pass ten
consecutive stress iterations (20 real process kills) in 2.274 seconds, with no
runtime skips. Diagnostics: `/tmp/shardline-storage-copy-interruptions-stability.log`.
Strict CLI integration-test Clippy passes. The full maintenance CLI suite on
the same optimized SHA-256 `2b13ae613c9acdd6546fba9d77ab9e035b98355e8559fe4c01a11741d2a33962`
passes all 22 tests in 2.336 seconds with zero skips
(`/tmp/shardline-storage-copy-interruptions-full-cli.log`). This is local-copy
evidence; remote-copy and remaining phase requirements still need review.

## Interrupted local-to-S3 copy: ordinary source-root inspection

A real MinIO fixture reproduced an operator gap: an ordinary local-to-S3 copy
applied its PUT, but after process termination `history --root` on the local
source returned zero tasks. The source had existing SQLite metadata. Task
history instead lived in host-private scratch with the raw destination scope.
Diagnostics: `/tmp/shardline-remote-copy-history-red-final.log`.

Publication now selects the actual local source's metadata when the destination
is a raw S3 endpoint. The task definition still binds both copy endpoints.
Local destinations keep their existing history selection. An unrelated server
PostgreSQL configuration is not inherited, and two-S3-endpoint copies retain
their existing private archive pending the separate inspection improvement.
No payload migration, new command flag or worker configuration is required.

`maintenance_profiles::storage_copy::interrupted_local_to_s3_copy_is_inspectable_from_source_without_replay`
owns a disposable MinIO container and response proxy. The proxy forwards an
accepted PUT, then delays its reply. The test independently reads the exact
bytes from MinIO, kills the ordinary CLI PID and adds another source object.
Normal source-root history reports one `needs_reconciliation` task owned by
that PID. Inspection copies no new object, preserves the original bytes and
creates no `_shardline_tasks/` payload objects.

The optimized binary SHA-256
`b8d56cfd7169078f65a740efa42f13a84847d986ef7217cc60a703eef65e634b`
reports version 1.11.3. The MinIO scenario passes in 4.888 seconds and all 22
maintenance CLI scenarios pass in 2.314 seconds with zero runtime skips.
Logs are in `/tmp/shardline-remote-copy-release.hPAGKM/remote-copy.log` and
`maintenance-cli.log`. All five focused history tests pass, including an
unreachable unrelated PostgreSQL endpoint and metadata reopening. Strict server
Clippy with all features, library and tests, and strict CLI library/integration
Clippy pass. The release build passes in 2 minutes 28 seconds. These checks
prove this accepted-PUT interruption and history workflow, not every remote-copy
phase or closure of the remaining merge gates.

## S3-to-S3 copy history without worker setup

The MinIO fixture reproduced the two-S3-endpoint inspection gap: after an
accepted destination PUT and process kill, ordinary project history returned
zero tasks. Diagnostics: `/tmp/shardline-s3-only-history-red-final.log`.
New S3-to-S3 outcomes now use the host's normal local project metadata. The
existing project-root discovery is shared with normal history; it does not
load unrelated server settings into the endpoint-only copy. Both payload
endpoints remain bound by the task definition, and local-to-S3/local-destination
selection is preserved. Existing private archives are not rewritten or removed.

The fresh-project fixture starts with no local metadata database. After a real
accepted PUT and process kill, plain `shardline history` from that project
reports one `needs_reconciliation` run with the actual CLI PID. New source
objects are not copied by inspection, original destination bytes remain exact
and S3 contains no task-history payload objects. The cold-start scenario passes
in 5.140 seconds. A separate unsafe-root fixture points `.shardline` through a
symlink: copying still succeeds and reports one inserted object, recording
warns, the outside directory stays empty and the symlink remains unchanged.

The optimized binary SHA-256
`e29233dae92bc44ea53802e01c4f5def8b5654eba8bcb345caead2045a6c8dae`
reports version 1.11.3. The three real MinIO cases pass in 14.963 seconds;
the cold-start case is additionally verified after removing fixture bootstrap.
All 22 maintenance CLI tests pass in 2.411 seconds with zero skips. Six focused
history tests and all 21 configuration tests pass. Strict server Clippy with
all features/library/tests and strict CLI library/tests Clippy pass. The
released/current 64/65 MiB copy-boundary comparison also passes. The release
build takes 2 minutes 34 seconds. Logs are under
`/tmp/shardline-s3-only-release.3sL0eR`, with focused unit and Clippy logs named
`/tmp/shardline-s3-only-history-*`. These cover the named raw-endpoint workflows;
older archives, other configurations and remaining phase/cost gates remain open.

## Explicit inspection of predecessor copy archives

`shardline history --archive --root PATH` reads an explicitly selected private
archive as its owner. It uses the existing read-only evidence backend and signed
history verification. The archive page is labelled separately and does not
apply a current deployment filter; normal history keeps its original scope and
signed association checks. No task execution, evidence migration or private
journal export is performed. Private journals awaiting export stay retained.

Two real MinIO process tests generate completed and interrupted S3-to-S3
archives using the predecessor branch binary SHA-256
`b8d56cfd7169078f65a740efa42f13a84847d986ef7217cc60a703eef65e634b`.
The current optimized binary SHA-256
`9ed2ba71f2c7d91d9bc85549f0a0a537a891db97cfbbecbce92e9e05bd55e583`
inspects them successfully. The recorded CLI PID and expected state are
preserved. New source objects remain absent from the destination. Every
pre-existing database, WAL, native journal, identity, signing key and artifact
file remains byte-exact; SQLite SHM read-lock bookkeeping is excluded from that
comparison. Both cases pass in 9.702 seconds. This predecessor is a prior
integration-branch build, not the released 1.11.3 deployment comparison.

Three SQLite inspection tests pass, including one-record pagination across
original deployment identities, corrupt-record visibility and unchanged normal
scope isolation. The real PostgreSQL inspection test passes against an owned
PostgreSQL fixture and preserves its saved artifact rows. All 23 maintenance
CLI tests pass in 2.398 seconds, including archive argument conflicts, private
directory requirements and absence of initialization or permission changes.
Strict CLI and server Clippy pass. The release build completes in 2 minutes
47 seconds and still reports version 1.11.3.

Logs: `/tmp/shardline-archive-release.fKvCzJ/legacy-evidence-immutable.log`,
`maintenance-cli.log`, `/tmp/shardline-archive-inspection-unit.log` and
`/tmp/shardline-batched-deletion.TGGfNo/test.log`. The first PostgreSQL attempt
stopped at a linker bus error near the `/tmp` user quota, before executing the
test. Moving an owned prior size fixture to workspace storage allowed the
successful rerun. This was validation resource pressure, not a PostgreSQL
runtime result. These checks cover saved metadata archives; other private
journals, configurations and remaining merge gates still require their audit.

## Read-only native journal verification and export

Retained native journals now use a read-only SQLite handle during verification
and evidence export. These paths no longer initialize tables or change journal
settings. Execution and recovery retain the original writable constructor.

All 13 journal tests pass. The new inspection case rejects missing-database
creation and commits, leaves an empty database uninitialized, verifies stored
records and blobs, and preserves the database bytes. All 40 maintenance unit
tests pass in 40.34 seconds. A new failed-export case preserves the unfinished
journal and identity bytes, retains its workspace and exposes its outcome as
`NeedsReconciliation` after evidence export becomes available. Existing completed
export, corrupt-backlog, failed-recording and recovery checks remain green.
Strict reliability and CLI Clippy pass.

Logs: `/tmp/shardline-read-only-journal.log`,
`/tmp/shardline-read-only-maintenance.log`,
`/tmp/shardline-read-only-reliability-clippy.log` and
`/tmp/shardline-read-only-cli-clippy.log`. This establishes read-only handling
for these native journal paths; it does not close the remaining predecessor,
deployment or failure-phase audit.

## Direct inspection when shared export failed

`history --archive --root WORKSPACE` now recognizes an individual native
workspace containing its saved identity and journal. It verifies the complete
task projection, referenced blobs, signed checkpoints and materialized results.
Isolated results also require matching captured stdout/stderr. The same state
and recovery guidance are used as ordinary history. Inspection opens SQLite
read-only with symlink refusal and performs no export or application execution.
Existing journal file permissions are accepted inside their private owner-only
directory; inspection does not chmod them.

Five journal/export tests pass in 5.58 seconds. The new cases cover completed
and interrupted unexported native histories, unchanged database bytes,
single-workspace pagination, damaged materialized artifacts and damaged captured
stdout. All 13 journal storage tests pass, including symlink refusal. Two actual
CLI archive tests pass in 4.52 seconds: a real ordinary rebuild with denied
shared export remains directly inspectable, while damaged result bytes are
rejected. The initial full CLI run passed its 23 existing tests; its new fixture
incorrectly expected an explicit runner to have automatic shared-history
identity. The corrected fixture uses ordinary rebuild and passes its focused
rerun. These results do not claim that the failed initial full run was green.

The predecessor interrupted S3-to-S3 process fixture also passes in 5.06 seconds.
The current debug binary inspects both shared archive metadata and the retained
native workspace written by the earlier optimized branch binary, preserving
all existing evidence bytes except SQLite SHM read-lock bookkeeping. It verifies
the recorded PID and unresolved state; the later source object remains uncopied.
The predecessor is the same `b8d56...` branch binary described above, not released
1.11.3. This check extends the named legacy-archive proof without asserting that
every older workspace layout is covered.

Logs: `/tmp/shardline-native-archive-tests-final.log`,
`/tmp/shardline-native-archive-journal.log`,
`/tmp/shardline-native-archive-cli-focused.log` and
`/tmp/shardline-native-archive-predecessor.log`.

## Prepare embedded upload ownership during initialization

A new unprivileged fixture reproduced a public-upload failure when a local
backend initialized normally, then its metadata root was changed to mode 0555
before the first GC barrier acquisition. Initialization now opens the existing
barrier file while root entries can still be created. It does not acquire a
lock, change directory permissions or wait for active GC. Uploads still acquire
their shared barrier before writing.

All seven local library ownership tests pass in 0.37 seconds, with no skipped
tests. They cover file and xorb uploads after restriction without a manual GC
call, existing barriers, cancellation and ownership waits. A root already
restricted without a barrier still initializes and reads existing data; its
uploads reject the unavailable lock. That preserves read-only initialization
and prevents unguarded writes, but does not prove writable compatibility for
that legacy library layout. Four local barrier tests pass in 0.08 seconds,
including preparation during an existing exclusive owner without waiting,
altering its file bytes or releasing ownership. Strict server Clippy passes.

The current debug CLI also passes the actual released-1.11.3 permission
comparison for fsck, rebuild, both repair forms and GC under a 0555 root, with
unchanged output/status and permissions. Fully read-only SQLite fsck preserves
released output/status and evidence; unavailable optional history warns without
recording completion. This is a correctness comparison, not a performance claim.

Logs: `/tmp/shardline-embedded-barrier-library-final.log`,
`/tmp/shardline-embedded-barrier-locks.log`,
`/tmp/shardline-embedded-barrier-clippy.log` and
`/tmp/shardline-local-permissions.9vut8b`. The first build stopped at a linker
bus error before executing the regression test. Moving owned completed release
fixtures to workspace storage allowed the red fixture and successful reruns.

## Remote copy shutdown at accepted PUT and saved report

Four new real MinIO process fixtures send SIGTERM to the original ordinary CLI
PID during local-to-S3 and S3-to-S3 copying. Each source type is interrupted
after an accepted PUT with its response delayed, and after materializing a
successful report but before committing completion. The saved-report fixtures
hold the native SQLite writer, verify three admission/checkpoint records and
successful original stdout, then assert termination by signal 15 and unchanged
record count. They observe a completed artifact filename rather than temporary
output files. Destination bytes are independently read back exactly.

Every cut exposes `needs_reconciliation` with the original CLI PID through
ordinary history. Adding another source object before inspection proves that
inspection does not continue copying. No task payload is written into remote
user-object storage. These are interruption checks for ordinary in-process
maintenance, not changes to its process model.

All nine remote-copy fixtures pass together in 43.77 seconds with the current
debug CLI and two test threads: the four new SIGTERM cuts, two existing kill
cuts, both predecessor archive cases and the unsafe-history-path success case.
There are no runtime skips; the other 60 S3 tests are excluded by the filter.
Strict server Clippy passes. Logs:
`/tmp/shardline-remote-copy-all-phases.log` and
`/tmp/shardline-remote-copy-sigterm-clippy.log`.

The first four-case run passed three cases and exposed an incorrect fixture
assumption that a local-source copy keeps private scratch below its source root.
All storage-copy scratch uses the owned TMPDIR. The fixture now finds that
owned workspace and verifies the completion cut explicitly. The corrected
four-case run and the complete nine-case run pass. These results establish the
named shutdown/report cuts, not every remote call or remaining admission phase.

## Operator guide and retained-workspace warning

The operator guide now centers on normal commands, readiness, saved results,
retryable writes and upgrade steps. Technical retry encodings and storage-copy
routing details are left in their references. The existing admin API link to
`#handle-a-retryable-write` now has a matching heading. Its CLI reference targets
were checked against the current headings.

The initial shared-export warning now names the retained workspace directory,
matching `history --archive --root`, rather than naming `tasks.sqlite`. The real
CLI denied-export fixture verifies that warning path and successfully inspects
the same directory. Both archive CLI tests pass in 4.64 seconds; strict CLI
Clippy passes. The initial assertion rejected ANSI formatting around log field
names; the corrected assertion checks the actual directory value. No recording,
execution or retry semantics change. Logs:
`/tmp/shardline-archive-warning-directory-final.log` and
`/tmp/shardline-archive-warning-clippy.log`.

## Host-local history with unrelated PostgreSQL configuration

`history --local` explicitly selects existing host-local SQLite metadata rather
than the configured server database. Without `--root`, it uses the same project
state location as raw S3-to-S3 copy recording. The page labels its source and
states that PostgreSQL history is excluded. Normal history retains its configured
database scope. The local mode rejects missing SQLite metadata and conflicts
with archive and pending-delete modes; it never initializes storage or retries
work.

Two actual CLI tests pass in 9.67 seconds. A local-to-local copy runs with an
unavailable unrelated PostgreSQL URL, preserves destination bytes, and records
local history. Ordinary history still fails against that configured database;
the explicit local mode verifies the copy's completed result. A later source
object is not copied by inspection. The second test checks mode conflicts and
absence of initialization in an empty root.

A real MinIO S3-to-S3 process fixture also passes in 9.95 seconds with unrelated
PostgreSQL settings on both copying and inspection. It kills the ordinary copy
after an accepted PUT, then reads its unresolved history with `--local` from
the same project. The original PID and destination bytes are preserved, and a
later source object remains uncopied.

All 26 maintenance CLI tests pass with the current debug binary in 234.451
seconds, with no skipped tests. The real-workflow fixture takes 90.336 seconds
and is reported slow; this is a correctness run, not a performance comparison.
Strict CLI and server Clippy pass. Logs:
`/tmp/shardline-local-copy-config-history-verified.log`,
`/tmp/shardline-local-history-s3-config.log`,
`/tmp/shardline-local-history-cli-all.log`,
`/tmp/shardline-local-history-clippy-final.log` and
`/tmp/shardline-local-history-server-clippy.log`.

### Restricted PostgreSQL history inspection

The configured PostgreSQL 16 fixture uses a real separate login with only schema
usage and SELECT on the existing reliability-events table. Owner setup seeds a
completed run in each of two independent schemas. The restricted login resolves
its owner's history scope and verifies exactly one completed run. Direct reads
of the artifact table and access to the other schema both fail with permission
denied. Inspection leaves the artifact keys and payload bytes unchanged. No
artifact-table grant or runtime schema-creation permission is added.

Both PostgreSQL inspection tests pass without skips in 0.93 seconds. This proves
the tested read-only account configuration; arbitrary RLS policies and other
deployment layouts remain outside this result. Fixture output:
`/tmp/shardline-batched-deletion.a7dE8h/test.log`.
Strict server Clippy also passes; its output is
`/tmp/shardline-restricted-inspection-clippy.log`.

The same restricted-login fixture now enables and forces a deny-all row policy
on the ordinary reliability-events table. Direct function reads reject the
schema-wide evidence path with `SLD04`; configured inspection falls back without
a shared scope and exposes no shared tasks. It includes the explicit warning
that an empty history list does not prove maintenance did not run. Artifact keys
and payload bytes remain unchanged across both inspections. This checks a policy
on domain metadata rather than only the optional artifact table. The two-test
run passes without skips in 0.96 seconds. After the environment restart cleared
temporary files, a fresh build and PostgreSQL fixture reproduced that result.
The retained test output is
`.codex/validation/rls-inspection.jcH0BM/test.log`; strict server Clippy output is
`.codex/validation/rls-inspection.jcH0BM/clippy.log`.

### Storage-copy termination before destination writes

A real MinIO S3-to-S3 fixture delays the successful source inventory response
body while the ordinary CLI is running. The source alone uses that proxy; the
destination uses MinIO directly. Before SIGTERM, the native journal contains
exactly three starting records and the destination object is absent. The CLI
terminates from signal 15 without printing a completed copy report.

Ordinary history inspection identifies the original PID and reports
`needs_reconciliation`. The journal still contains exactly three records after
inspection, the original destination object remains absent, and a source object
added after termination is also not copied. No task evidence enters S3 payload
storage. The current debug binary passes this test in 9.96 seconds with no skips:
`.codex/validation/source-inventory-test.log`. This covers the saved starting
checkpoint and an active source inventory request, not earlier admission cuts
or every maintenance operation.

Both existing local-to-S3 and S3-to-S3 saved-report SIGTERM fixtures also pass
after the native-workspace helper is shared, in 10.91 seconds without skips.
Strict Clippy for the integration-test target passes. Logs:
`.codex/validation/source-inventory-report-regression.log` and
`.codex/validation/source-inventory-clippy.log`.

### Storage-copy admission process interruptions

Three ordinary local-to-local CLI fixtures send SIGTERM at exactly zero, one or
two committed native records. The zero-record cut holds the existing destination
metadata writer while the manifest is being published. The later cuts use a
fixture-only SQLite trigger to hold export after the native intent or started
record; terminating the owned CLI also stops that query. No production pause
hook or extra worker is introduced.

Each fixture verifies the original PID, signal 15, absent destination bytes and
no completed report. Owner-selected native archive inspection reports `pending`
for the zero/one-record cuts and `needs_reconciliation` for the two-record cut.
It preserves the native database, WAL and identity bytes exactly, leaves the
record count unchanged, and copies neither the original source object nor an
object added after termination. All three tests pass without skips in 4.59
seconds. Log: `.codex/validation/copy-admission-all-sigterm.log`. These are actual
process interruptions for copying; they do not establish every operation's
admission or mutation behavior.
Strict CLI integration-test Clippy also passes:
`.codex/validation/copy-admission-clippy.log`.

### Restricted legacy SDK roots without a barrier file

A source build of tag `v1.11.3` verifies that the public local SDK can initialize
and upload under an already restricted 0555 metadata root with no barrier file,
when existing SQLite WAL/SHM files and payload directories remain writable. Its
file upload and download pass in 0.02 seconds. The corresponding current test
initially fails with permission denied before publication, confirming a
compatibility regression rather than an unsupported legacy assumption.

On Linux, current local upload and GC guards now also share the existing root
directory inode. They acquire the legacy file lock when available; when its
creation is denied and the file is absent, the directory guard protects the
operation without creating an entry or changing permissions. Every current
owner acquires the directory guard, including owners that can create the file,
so a permission change and later file creation cannot bypass an existing owner.
Other platforms retain the original file-lock path.

The corrected SDK fixture preserves existing reads, verifies both file and xorb
uploads, and leaves the root mode and missing barrier file unchanged. Four
directory-lock fixtures check permission changes followed by file creation,
cancelled ownership waits, blocking by an existing legacy file owner, and
rejection of a symlinked root without outside changes. Logs:
`.codex/validation/released-sdk-restricted-upload.log`,
`.codex/validation/restricted-upload-current-red.log`,
`.codex/validation/restricted-directory-tests.log` and
`.codex/validation/restricted-upload-library-final.log`.

All seven public-library ownership tests pass in 0.38 seconds, including the
restricted-root file and xorb calls waiting for GC before any new payload write.
The four existing local barrier tests and the cancellation/runtime-shutdown
fixture also pass. Strict server Clippy passes. A fresh current CLI comparison
against the released 1.11.3 executable preserves output and exit status for fsck,
rebuild, both repair forms and GC mark under a 0555 root, plus fsck with fully
read-only SQLite files. Logs:
`.codex/validation/restricted-barrier-regression.log`,
`.codex/validation/restricted-barrier-shutdown.log`,
`.codex/validation/restricted-directory-clippy-final.log` and
`.codex/validation/restricted-directory-cli-permissions.log`.
The local GC suite also passes with the new barrier path; its executed cases
and timing are recorded in `.codex/validation/restricted-directory-gc-tests.log`.

After this production change, all 29 maintenance CLI tests pass without skips
in 93.45 seconds with the current debug build. This includes the three new
admission cuts and the existing real workflows, process interruptions,
configuration routing, archive inspection and restricted-root cases. The
real-workflow test exceeds 60 seconds in this debug run; this is correctness
evidence, not a production timing comparison.

The public-library startup fixture also passes in 0.26 seconds. It verifies all
three server roles, application-owned logging, fresh cleanup on consecutive
starts and history in the original server PID. Logs:
`.codex/validation/restricted-directory-cli-all.log` and
`.codex/validation/restricted-directory-embedded-startup.log`.

### Ordinary-command admission interruption matrix

The admission fixture now has its own semantic test module and covers fsck,
index rebuild, GC mark, `repair lifecycle` and default `repair`, each at zero,
one and two committed native records. Together with the three local-copy cuts,
it verifies 18 actual SIGTERM interruptions. Each checks the saved operation
identity and original PID, exact native database/WAL/identity preservation,
pending versus unresolved guidance, unchanged user payload and no work during
inspection. The four test functions pass without skips in 70.44 seconds with
the debug CLI. Strict integration-test Clippy passes. Logs:
`.codex/validation/ordinary-admission-interruptions.log` and
`.codex/validation/ordinary-admission-clippy.log`.

The complete 30-test CLI suite passes without skips in 95.73 seconds with Cargo's
debug executable. A direct invocation of the compiled harness with
`CARGO_BIN_EXE_shardline` selecting the current optimized binary also passes all 30
tests, in 5.06 seconds. Direct invocation is necessary here: Cargo overrides that
environment setting with its own debug executable when it launches the harness.
All CLI launches in these fixtures use the runtime-selectable `binary()` helper.
Logs: `.codex/validation/ordinary-admission-cli-debug-all.log` and
`.codex/validation/current-release-cli-direct.log`.

### Current optimized local compatibility benchmark

The current optimized binary reports version 1.11.3 and has SHA-256
`5bc30ebbbd0f3ab68857ecdb5231ff010fb8aa9d14243c71039a8e4982afdb84`.
The existing local benchmark matrix at concurrency 1, 32 and 128 preserves every
non-performance counter, setting and report field against released 1.11.3.
Released fsck reads current data, and ordinary fsck preserves stdout/stderr and
records its outcome outside payload storage.

In this single fixture run, released/current wall times are 0.25/0.26, 0.94/0.97
and 2.76/2.92 seconds at the three concurrency levels. Current times are slightly
higher; these observations do not establish a production regression threshold
or a distributed SLO. Raw timing, memory, report and binary-hash records are in
`.codex/validation/shardline-benchmark-compatibility.OB15u2`; the executed
verification output is `.codex/validation/current-release-benchmark-compatibility.log`.

### Existing SDK deployment on a read-only Linux mount

An actual Docker bind mount, rather than directory permissions alone, exposed
a compatibility regression: released 1.11.3 uploaded successfully while the
current SDK returned EROFS when creating its absent legacy barrier file.
The root is read-only; existing SQLite, WAL, SHM and chunks are writable nested
mounts. The probe confirms the kernel returns error 30 for a new root entry.

Linux owners now retain their existing root-directory lock when an absent
legacy lock file cannot be created because the filesystem is read-only. Other
errors and failures opening an existing legacy barrier still propagate.
No directory entry, permission change or new operator configuration is needed.

The checked-in `mounted_sdk_upgrade` integration probe is explicitly ignored
outside its required mount fixture. The owned verifier
`scripts/shardline/verify_sdk_readonly_mount.sh RELEASED_CLI RELEASED_PROBE CURRENT_PROBE`
executes it inside `debian:13-slim`, alongside the public upload/read-back probe
compiled against tag v1.11.3. Both execute without skips. The current probe
also holds an exclusive directory lock: upload stays pending without writing
chunks until that owner releases it, then succeeds. The four existing directory
ownership/cancellation/legacy-lock tests pass, as does strict server library
and test Clippy. Logs are `.codex/validation/mounted-sdk-red.log`,
`.codex/validation/mounted-sdk-owned.log`,
`.codex/validation/mounted-sdk-barrier-tests.log`,
`.codex/validation/mounted-sdk-clippy.log` and
`.codex/validation/mounted-sdk-probe-clippy.log`.
This establishes the named Linux mount layout; it does not establish behavior
on every network filesystem or upgrade layout.

The comparison now also covers an existing legacy barrier file, still on the
read-only mount. Released 1.11.3 succeeds, while the earlier current build failed
before taking legacy ownership. The Linux fallback now opens the existing inode
read-only and takes its original shared or exclusive flock. The current mounted
probe waits for both directory and legacy-file owners before writing chunks.
All four released/current, absent/present cases pass without skips; logs:
`.codex/validation/mounted-existing-red.log` and
`.codex/validation/mounted-existing-fixed.log`.
The final verifier also compares the existing barrier's bytes before and after
both SDK runs (`.codex/validation/mounted-existing-preservation.log`). All five
focused directory-barrier tests pass without skips, including exclusive GC
ownership through a read-only legacy descriptor. Strict all-feature server
library/test Clippy passes. Logs:
`.codex/validation/mounted-existing-local-final.log` and
`.codex/validation/mounted-existing-clippy-final.log`.

### Ordinary completion interrupted before shared acknowledgement

The semantic `maintenance_support::native_interruptions` module now covers five
additional actual SIGTERM cuts: fsck, rebuild, GC mark, lifecycle repair and
default repair. Each fixture blocks export of the completion record using a
trigger on its owned metadata database, after native record four is durable.
This distinguishes incomplete shared recording from completed native evidence.

Shared inspection reports `needs_reconciliation`. Explicit native inspection
verifies `completed` while preserving the journal/WAL/identity bytes. A later
ordinary invocation exports the old verified result without replaying its work,
retires its private scratch and records a distinct completed run. Shared history
then contains both results; the existing user payload remains unchanged.
These cuts do not stand in for operation-specific external mutation faults.

The newly built optimized binary includes both mounted-root fixes and reports
version 1.11.3. SHA-256:
`467c7ce20ecbd8806e5095cf9ded13afd8d0debb8c45a7314e65a6a027de2f99`.
Direct execution of the compiled CLI harness against that binary passes all 31
tests without skips in 5.06 seconds. Strict CLI integration-test Clippy passes.
Logs: `.codex/validation/mounted-barrier-release-build.log`,
`.codex/validation/ordinary-completion-release-cli-all.log` and
`.codex/validation/ordinary-completion-clippy-final.log`.

### Lifecycle repair interrupted during partial mutation

The semantic `maintenance_support::repair_interruptions` module covers six actual
SIGTERM cases against the current optimized CLI: expired retention holds,
missing-object quarantine entries and stale webhook deliveries, each through
`repair lifecycle` and default `repair`. A trigger in the owned SQLite fixture
holds the second deletion. The test observes the committed first deletion and
an active database writer before terminating the original CLI PID.

The first mutation survives; the interrupted transaction rolls back. Public
lifecycle readers verify the surviving domain records and reliability evidence.
The native journal retains three records. Ordinary history reports unresolved
work and changes neither domain rows nor user bytes. A new eligible record is
then added; the next explicit repair handles both the remaining and new work,
preserving a permanent hold and payload bytes. Shared history contains the old
unresolved run and a distinct completed run. The original native database and
WAL remain byte-identical, proving that fresh recovery does not rewrite the
earlier task into completion.

All six cases execute without skips in the single matrix test. Strict CLI
integration-test Clippy and formatting pass. Logs:
`.codex/validation/repair-mutation-final.log` and
`.codex/validation/repair-mutation-clippy-final.log`.
The binary is the current optimized build recorded above. These SQLite cases
do not establish all PostgreSQL or remote mutation boundaries.

### Fresh GC after interrupted marking

The existing two actual GC termination cuts now live in the semantic
`maintenance_support::gc_interruptions` module. They cover observed domain
marking and materialized report creation while the native completion commit is
blocked. Inspection through a subsequent fsck leaves a later orphan unmarked,
preserves the earlier candidate and keeps the interrupted run unresolved.

The fixture now explicitly runs the next ordinary `gc --mark`. It discovers and
marks exactly one new candidate, preserves the original candidate and both
objects' bytes, and retains the old native journal's three records. History
contains the original unresolved GC and separate completed fsck and fresh-GC
runs. Both cuts pass against the current optimized binary; strict CLI
integration-test Clippy passes. Logs:
`.codex/validation/gc-fresh-module.log` and
`.codex/validation/gc-fresh-module-clippy.log`.
This covers local marking; it does not establish every sweep or remote-I/O fault.

### Latest-pointer rebuild interrupted during partial mutation

The semantic `maintenance_support::rebuild_interruptions` module seeds real
files through the public SDK, removes only two derived latest pointers, and
holds the second rebuild insertion in an owned SQLite trigger. It observes the
first restored pointer and an active writer before sending SIGTERM to the
original CLI PID. The committed pointer survives and the second transaction
rolls back. Immutable version metadata stays byte-identical; both original
versions and an unaffected latest file download correctly.

Ordinary history reports unresolved work without rebuilding anything. A new
uploaded version is added and its derived pointer removed. The next explicit
rebuild restores the two missing pointers, including that later version; all
four payloads download correctly. The old native journal/WAL remain unchanged
and unresolved, alongside the new completed run. The focused actual-process
test passes against the current optimized CLI without skips; strict CLI
integration-test Clippy passes. Logs:
`.codex/validation/rebuild-mutation-cut.log` and
`.codex/validation/rebuild-mutation-clippy.log`.
This covers local latest pointers, not every reconstruction, dedupe or remote
rebuild mutation.
The complete CLI harness also passes all 33 tests without skips against this
optimized binary in 5.06 seconds, including the repair and fresh-GC additions.
Log: `.codex/validation/rebuild-mutation-release-cli-all.log`.

### Derived rebuild indexes interrupted during partial mutation

The semantic `maintenance_support::rebuild_derived_interruptions` matrix adds
three actual SIGTERM cuts: stale reconstruction pruning, stale dedupe mapping
pruning and dedupe mapping restoration from structurally valid retained shards.
Each owned SQLite trigger pauses the second mutation after the first committed;
the test observes that progress and an active writer before terminating the
original CLI PID. Surviving rows remain readable through their public APIs.

Read-only history leaves partial indexes unchanged. A later eligible row or
retained shard is added, and a fresh rebuild processes both remaining and newly
discovered work. Restored mappings point to their expected retained shard keys.
The protected SDK file still downloads, retained shard bytes stay unchanged,
and the earlier native database/WAL retain their three records byte-for-byte.
Shared history keeps the interrupted run unresolved alongside the new completed
run. All three cuts pass against the optimized CLI without skips; strict CLI
integration-test Clippy passes. Logs:
`.codex/validation/rebuild-derived-final.log` and
`.codex/validation/rebuild-derived-clippy-committed.log`.
The complete CLI harness passes all 34 tests without skips against the optimized
binary in 5.07 seconds. Log:
`.codex/validation/rebuild-derived-release-cli-all.log`.

The fixture adds only a dev-dependency on the existing workspace Xet core crate;
no dependency version changes. The generic runner's comment now matches its
existing ownership contract: deployment entry points hold the exclusive barrier,
and lower-level callers supply applicable ownership throughout scan and mutation.
These cases cover local derived indexes, not every PostgreSQL or remote fault.

### PostgreSQL ordinary CLI process cuts

The semantic `maintenance_support::postgres_interruptions` fixture covers fsck,
rebuild, GC mark and both repair forms at five shared-recording boundaries:
intent, start, checkpoint, materialized outcome and completion record. These
are 25 actual SIGTERM cuts against the optimized CLI in a fresh PostgreSQL 16
container bootstrapped by released 1.11.3 and upgraded through the normal owner
migration command.

An owned trigger pauses the selected publication. The fixture identifies the
exact paused PostgreSQL backend and original CLI identity/PID, terminates the
CLI, then cancels only that backend's injected server-side sleep. Cancellation
rolls back the uncommitted publication. This deliberately separates client
termination from database-query cancellation; it does not establish immediate
cancellation of arbitrary remote calls when their clients exit.

The fixture verifies the precise durable record counts on both sides of the
handoff. Shared inspection changes no saved artifacts or native journal bytes.
A fresh command records a distinct run. Native intent remains pending; native
start/checkpoint remains unresolved; durable native completion is verified and
republished without replaying application work. Earlier shared artifact bytes
and user payload remain intact. Unfinished private journal/WAL bytes remain
unchanged; verified finished private scratch is retired after successful export.

The ignored integration fixture requires an explicitly owned database marker;
ordinary test invocation does not count its skip as proof. Run it through
`scripts/shardline/verify_maintenance_postgres_process_cuts.sh CURRENT RELEASED CLI_TEST_HARNESS`
after building the harness with
`cargo test --locked -p shardline --test maintenance_e2e --no-run`.
The wrapper directly invokes that compiled harness so Cargo cannot override the
selected optimized CLI. It requires all 25 phase confirmations and one passed
test with no skips, and removes only its owned container.
The final owned run passes all 25 cases without skips in 36.49 seconds; strict
CLI integration-test Clippy, Rust formatting and shell syntax checks pass.
Logs: `.codex/validation/postgres-process-cuts-preservation.log` and
`.codex/validation/postgres-process-cuts-clippy-preservation.log`.
Detailed per-phase output is retained in
`.codex/validation/shardline-postgres-process-cuts.aRWU88/test.log`.

### PostgreSQL partial repair and competing ownership

The owned process verifier additionally interrupts ordinary lifecycle repair
after one expired hold is deleted and the next deletion is paused in PostgreSQL.
SIGTERM ends the CLI; the paused server session retains its maintenance lock.
A competing ordinary index rebuild demonstrably waits on that lock. The fixture
then explicitly cancels its injected query, permitting the session to release
ownership and the rebuild to finish. This tests safe waiting, not immediate
cancellation of server work after client exit.

The first deletion survives and the interrupted next deletion rolls back.
History inspection leaves the remaining holds unchanged. A newly expired hold
is added, and fresh lifecycle repair handles both remaining and new work.
The permanent hold, protected payload and interrupted native journal/WAL bytes
are preserved. The old receipt remains unresolved; fresh commands complete as
distinct runs. This establishes the named shared-repair/exclusive-rebuild
handoff, not serialization of every maintenance command or exactly-once effects.

The extended owned wrapper passes the 25 receipt cuts in 37.48 seconds and this
mutation case in 2.88 seconds, without skips. Strict CLI integration Clippy passes.
Logs: `.codex/validation/postgres-mutation-owned.log`,
`.codex/validation/postgres-mutation-clippy.log` and
`.codex/validation/shardline-postgres-process-cuts.v8Dbm1/mutation-test.log`.

### Remote history calls during shutdown

The shutdown audit reproduced a real gap: optional legacy-history reads could
start an S3 body download after shutdown occurred during their metadata lookup.
The new regression test failed before the check was added. Housekeeping now
checks its stop flag before the download and at additional remote-to-metadata
publication, deletion and inventory boundaries. Stores used by ordinary
application work have no housekeeping stop flag and retain their behavior.

Four loopback HTTP fixtures use the actual S3 adapter. Their response handlers
drop the housekeeping lifetime after a request arrives and before its response
returns, avoiding timing assumptions. They verify that:

- Shutdown during HEAD prevents the subsequent GET.
- Shutdown during GET allows that active read to finish, but blocks a later read.
- Shutdown during an absence check prevents subsequent SQLite publication.
- Shutdown during an accepted history DeleteObjects request preserves the SQLite
  copy; fresh cleanup can finish without requiring the earlier lifetime.

Exact request counts and keys are checked. Fresh ordinary reads still succeed,
and fresh publication/deletion produces the expected metadata state. The fixture
models the runtime's SQLite/S3 history composition without starting unrelated
upload recovery. It tests actual storage-client HTTP boundaries, not a production
S3 service or immediate cancellation of active remote requests.

All eight housekeeping tests pass without skips in 0.26 seconds.
Logs: `.codex/validation/housekeeping-remote-red.log` and
`.codex/validation/housekeeping-remote-final.log`.
The six relocation tests and three SQLite retention tests also pass without
skips, including interrupted cleanup and fresh resumption. Strict all-feature
server library/test Clippy and Rust formatting checks pass.
Logs: `.codex/validation/housekeeping-remote-relocation.log`,
`.codex/validation/housekeeping-remote-retention.log` and
`.codex/validation/housekeeping-remote-clippy.log`.

### Bounded legacy-manifest reuse during remote copying

An actual HTTP/S3 client fixture exposed repeated reads of the same immutable
legacy task manifest: one unfinished run with 64 additional saved artifacts
caused 136 manifest requests during inventory. Recognition now retains only the
most recent recognized run's manifest and parsed identity within one migration.
The manifest read remains bounded to 64 KiB; the cache does not grow with the
inventory and is discarded after the command. Failed recognition is not cached.
Signed scope bindings and delete replies keep their individual verification.

The fixture runs both dry-run and actual S3-to-local migration. Each now uses
one HEAD and one GET for that manifest, excludes its unfinished control evidence,
and inventories the same four payload objects with exact byte counts. The real
copy preserves user bytes, arbitrary objects in the reserved namespace, a
malformed manifest and its unrecognized artifact. Source requests remain reads
only and all original source bytes remain unchanged. A second invocation checks
that recognition does not persist across copies.

All 26 storage-migration tests pass without skips in 0.13 seconds. Existing tests
also cover signed scope bindings, damaged bindings and partly retired histories.
Strict all-feature server library/test Clippy and Rust formatting pass. This is
a request-count improvement for the named inventory; unordered inventories,
cloud latency and overall production cost retain their existing limits.
Logs: `.codex/validation/legacy-copy-http-red.log`,
`.codex/validation/legacy-copy-http-green.log` and
`.codex/validation/legacy-copy-http-clippy-final.log`.

The final focused HTTP test also passes after the lint adjustment. The rebuilt
optimized CLI passes the released 1.11.3 copy/rollback verifier: payload inventory
and output match, completed legacy history relocates byte-for-byte, and an
execute-only binary preserves copying when optional tracking cannot initialize.
All 34 ordinary CLI integration tests pass against that optimized executable in
5.07 seconds. The two owned PostgreSQL process fixtures are ignored by this
ordinary invocation and are not counted as proof here; their separate runs are
recorded above.

The binary still reports version 1.11.3. SHA-256:
`14eb3b53933caaa278a6dba901b914d9fd8f2ffe03badc75ba3febbf17f68ac3`.
Logs: `.codex/validation/legacy-copy-http-final.log`,
`.codex/validation/legacy-copy-release-build.log`,
`.codex/validation/legacy-copy-release-compatibility.log` and
`.codex/validation/legacy-copy-release-cli-all.log`.

### Ordinary PostgreSQL/S3 startup across server roles

The owned verifier bootstraps a fresh PostgreSQL 16 database with released
1.11.3 migrations and runs the usual current owner upgrade. It starts real MinIO
with a protected payload, then launches ordinary `serve --role all`, `api` and
`transfer` processes using separate roots and the same metadata/payload targets.
Each starts without worker configuration, reports readiness and its expected
role, and removes a newly expired LFS session. Shared history retains previous
IDs and exact artifact bytes, adds one distinct completed run and records that
server's PID. No tracker child is present. The payload remains byte-identical,
the bucket contains only that payload and no GC candidate is created.

The database runtime role receives existing ordinary-metadata privileges and
has no direct SELECT/INSERT/UPDATE/DELETE grant on the task-artifact table.
The S3 runtime policy explicitly denies task-object writes and deletes. Positive
payload writes/deletes and an actual rejected task PUT verify that policy before
startup. All three roles still record and inspect shared PostgreSQL history.
This proves the named grant model; it does not override tenant row policies or
establish live cluster health.

Run the reproducible process check with:

```bash
bash scripts/shardline/verify_maintenance_postgres_s3_startup.sh CURRENT RELEASED
```

An optional third argument accepts the compiled server library test harness.
The final owned run also invokes the existing PostgreSQL/local role fixture
against its released-ledger database, requiring the exact test and one non-skipped
pass. Its three roles perform fresh same-process startup cleanup in 0.77 seconds.
Only the verifier's own containers and server process are removed afterward.
Final log: `.codex/validation/postgres-s3-startup-final.log`.
Detailed diagnostics:
`.codex/validation/shardline-postgres-s3-startup.wCXoTC/`.
The first attempt failed on a wrong quarantine-table name in the verifier;
the corrected, stricter runtime-account fixture passes all assertions.

The existing Docker-backed PostgreSQL/S3 public-library test was stale: it read
legacy object-backed history with `from_config` and found zero automatic runs.
It now uses `for_inspection` with its caller-owned PostgreSQL pool and resolved
scope, verifies consistent scope across roots and retains its shared-history and
unauthorized-request assertions. The corrected test passes without skips in
6.68 seconds. Logs: `.codex/validation/postgres-s3-library-current.log` and
`.codex/validation/postgres-s3-library-final.log`.

Current SQLite/S3 public-router startup also passes for all three roles against
real MinIO in 2.45 seconds. Public local embedding passes all three roles and two
fresh starts per role in 0.26 seconds, preserving the application's logging
subscriber. Logs: `.codex/validation/postgres-s3-sqlite-profile-regression.log`
and `.codex/validation/postgres-s3-embedding-regression.log`.
Together these checks verify gate 1's ordinary startup, supported backend/role
matrix and embedding requirements. Other compatibility, failure and cost gates
retain their separate criteria. Strict all-feature server library/test Clippy,
Rust formatting and shell syntax checks pass. Clippy log:
`.codex/validation/postgres-s3-startup-clippy.log`.
The tested optimized CLI remains version 1.11.3 with SHA-256
`14eb3b53933caaa278a6dba901b914d9fd8f2ffe03badc75ba3febbf17f68ac3`.

### Current ordinary-command compatibility checks

The same optimized CLI passes twenty exact released/current configuration-failure
comparisons: fsck, rebuild, GC and both repair forms, each with malformed TOML,
missing configuration, a missing environment file and an unusable root file.
The verifier compares stdout, stderr and exit status and preserves root-file
contents without creating absent roots. Five command forms under a 0555 root
also preserve released output and status without changing permissions or adding
root entries. Fully read-only SQLite fsck preserves existing evidence and reports
optional history failure without recording completion.

The large-diagnostics fixture preserves released GC output and payload bytes,
then discovers newly added work and records a distinct completion. The real
MinIO fixture preserves restricted S3 copying and released rollback. Its retained
inline archive uses the native journal: explicit archive inspection confirms the
original copy PID and completed state, leaving journal and identity bytes exact.
The verifier now checks that format instead of expecting the older private
metadata layout. No runtime change was needed.

An additional S3-to-local copy runs after removing its working directory. Current
and released commands preserve identical stdout and stderr, successful status
and destination payload bytes. The current destination contains no newly created
history metadata. Optional preparation falls back to the original command;
missing history does not prove that copying never occurred.

All four verifiers pass against the current optimized CLI. Logs:
`.codex/validation/command-compatibility-invalid.log`,
`.codex/validation/command-compatibility-permissions.log`,
`.codex/validation/command-compatibility-diagnostics.log` and
`.codex/validation/command-compatibility-s3-final.log`.
The optional signed-reply cases in the S3 verifier were not selected in this run.
These checks strengthen gate 2; service/deployment verification is separate
from these command comparisons.

### Current released-service and real reboot verification

The current optimized binary also passes `verify_maintenance_vm.sh` against
released 1.11.3 in an owned Debian 13 KVM guest. The cached cloud image's SHA-512
matches both the saved checksum and Debian's freshly fetched published checksum.
The released binary generates and runs the GC service. Replacing only the binary
then runs that same service twice successfully; its file checksum is unchanged.
The service retains `ProtectSystem=strict`, `ProtectHome=yes`, `PrivateTmp=yes`,
`NoNewPrivileges=yes` and `ReadWritePaths=/var/lib/shardline`.

Two real guest resets interrupt GC before mutation and after mutation/report
materialization. Reboot changes the boot ID while preserving machine identity.
Cold startup and inspection retain unresolved tasks and exact saved records;
the second cut also retains the native checkpoint, report bytes, quarantine
candidate and user payloads. Newly introduced objects are not marked by recovery.
Only a fresh explicit GC marks the first later orphan and records a separate
completion. The service's main PID matches the recorded inline owner.

The verifier exits successfully and removes its owned guest. Log:
`.codex/validation/command-compatibility-vm-current.log`.
Detailed assertions and guest logs:
`.codex/validation/shardline-maintenance-vm.BIIL89/`.
The transient HTTP connection reset during readiness polling precedes a successful
retry; all final assertions pass. This verifies the current Linux/systemd layout
and the two specified reboot cuts, rather than every storage or filesystem layout.

### Current restricted deployment, replacement and rollback checks

The current optimized CLI passes all eleven selected deployment drills in
143.93 seconds using owned Compose stacks, real PostgreSQL and MinIO, and the
actual released 1.11.3 binary. None of the selected tests skips or panics. Nextest's
twelve skipped entries are the other tests excluded by the explicit filter.
The verifier copies both binaries before running; the current copy has SHA-256
`14eb3b53933caaa278a6dba901b914d9fd8f2ffe03badc75ba3febbf17f68ac3`.

| Check | Verified result |
| --- | --- |
| Restricted PostgreSQL/S3 commands | Released/current fsck, rebuild and read-only GC succeed with matching stdout in a read-only container without a writable root or temporary directory. Before owner setup, commands do not install optional history. After setup, fsck and rebuild record completed inline outcomes. Read-only S3 credentials still permit metadata-backed history without S3 task writes. |
| Concurrent ordinary rebuilds | One process holds shared deployment ownership while the second waits. Both remain unresolved until the held scan is released; the first removes stale metadata and the second sees fresh state. They finish with distinct completed runs. |
| Container replacement and shutdown | Replacement after an actual killed process preserves the earlier unresolved run and records a new completed run with the replacement's PID namespace. The server is the only process, needs no tracker mount, exits successfully on normal SIGTERM and records another distinct run on fresh startup. |
| Remote-delete faults | Lost acknowledgement preserves unresolved deletion without replay. A delayed former owner's delete rejects a conflicting hold after takeover. An acknowledged deletion settles its marker. Existing released runtime grants need no additional fence grant. |
| Saved replies and recording faults | Recovery through repair and through a fresh GC sweep uses the saved reply after a real PostgreSQL outage without another user-object deletion. Denied optional receipt writes preserve the existing successful GC outcome. |
| Mixed versions and rollback | Actual released/current overlap, ordinary owner setup and rollback preserve bytes, compatible writers and native task outcomes without new domain migrations. This retains the documented requirement to pause older destructive GC during rollout and rollback. |

These source-backed assertions cover the named deployment paths. The restricted
command fixture compares stdout and successful status; it does not assert
byte-identical stderr across optional recording warnings. The source inventory
and other operation-specific write/failure paths retain their separate audit.
Log: `.codex/validation/command-compatibility-deployments-current.log`.
Detailed results:
`.codex/validation/shardline-maintenance-deployments.gSGP0N/test.log`.
Reproduce with `verify_maintenance_deployments.sh CURRENT RELEASED`.

### Current optimized local history resource checks

The new `verify_maintenance_history_resources.sh SERVER_LIB_TEST_HARNESS`
requires each exact resource test to exist and pass without skipping. The
current all-feature server library harness builds in the optimized release
profile and passes all three selected drills:

| Drill | Result |
| --- | --- |
| Full SQLite history | All 4,096 slots are occupied; further admission is rejected and pagination returns each history once. Automatic cleanup retires 4,094 finished runs in 97 calls, preserving unfinished/corrupt evidence and payload bytes. Fresh recording succeeds. Complete fixture: 336.91 seconds; cleanup including before/after inventories: 163.03 seconds. |
| Many legacy runs | Pagination returns 1,026 distinct entries. Finished history retires in 24 calls, preserving unresolved/corrupt history and user payload. Pagination: 1.20 seconds; cleanup: 13.85 seconds; slowest call: 0.66 seconds. |
| Large legacy run | A 10,000-artifact finished run retires in 40 incremental calls, with reopening between calls and preserved unresolved evidence and user data. Cleanup: 11.02 seconds. |

The SQLite test checks at most 256 run-artifact removals and 64 retired runs per
call. These are current correctness and fixture-cost observations, not controlled
benchmarks: the PostgreSQL capacity drill ran concurrently. The earlier faster
SQLite fixture remains historical evidence, not a promise of current latency.
Inventory cost and physical disk use remain separate from logical history limits.
Source inspection also confirms that normal inline commands stream their output
directly and impose no CPU/deadline limit; their small saved receipt does not
truncate live output or diagnostic exports. Private admission counts active and
retired workspaces together before accepting another workspace. Reaching its
limit preserves evidence and falls back to existing command execution.

Logs: `.codex/validation/history-resource-release-build.log` and
`.codex/validation/history-resource-local-current.log`.
Detailed assertions: `.codex/validation/shardline-history-resources.tEK07M/`.
The verifier, shell checks and graph update pass. Gate 4 remains open for the
remaining operation/deployment cost assessment; these measurements do not bound
remote storage latency or establish a disk quota.

### Current optimized PostgreSQL history resource check

The owned PostgreSQL incremental-capacity verifier also passes against the same
optimized all-feature library harness: one exact test, no failure or skip,
516.91 seconds. It fills all 4,096 slots, rejects excess admission, paginates
all distinct histories and retires 4,094 finished runs in 97 cleanup calls.
Every call stays within 256 run-artifact removals and 64 finished runs. Retirement
markers settle; unfinished/corrupt evidence, corrupt bytes and user payload stay
unchanged. No task objects are written to payload storage, and fresh recording
succeeds after cleanup.

Creation takes 140.83 seconds, inspection 35.22 seconds and cleanup 340.24 seconds;
the slowest measured cleanup call takes 4.90 seconds including the fixture's
before/after inventories. Local resource drills ran concurrently, so these
measurements establish neither an isolated benchmark nor a production SLO.
Log: `.codex/validation/history-resource-postgres-current.log`.
Detailed result: `.codex/validation/shardline-postgres-capacity.EwoRQN/test.log`.
The verifier removes only its owned disposable database container.

### Current released workload and diagnostic comparison

The existing local full-workload verifier passes at concurrency 1, 32 and 128
against actual released 1.11.3. Every non-performance counter, setting and report
schema matches. Released/current fsck also produces identical stdout and stderr
for all nine current workload roots, records one completed ordinary run per root
and creates no task history under payload chunks.

| Concurrency | Released wall time / peak RSS | Current wall time / peak RSS |
| --- | --- | --- |
| 1 | 0.25 s / 36,436 KiB | 0.26 s / 40,080 KiB |
| 32 | 1.00 s / 393,088 KiB | 1.03 s / 406,988 KiB |
| 128 | 2.82 s / 725,264 KiB | 2.89 s / 725,408 KiB |

These are single fixture observations. They show preserved workload behavior
with small observed wall-time differences, not a throughput improvement or
production capacity guarantee. The PostgreSQL resource fixture was finishing
when this comparison began; these are not isolated performance measurements.
Log: `.codex/validation/history-resource-benchmark-current.log`.
Reports, timings, binary hashes and fsck comparisons:
`.codex/validation/shardline-benchmark-compatibility.wE2Khh/`.

### Current protocol and backend interruption checks

The remaining twelve deployment drills pass in 147.37 seconds against the
current optimized CLI, with no selected runtime skip or panic. Together with
the eleven restricted-deployment/rollback drills above, all 23 named deployment
tests have current evidence using identical runtime bytes, SHA-256
`14eb3b53933caaa278a6dba901b914d9fd8f2ffe03badc75ba3febbf17f68ac3`.
The eleven skipped entries here are excluded tests selected by the complementary
verifier. The supplied clock shim matches its saved verified-fixture hash,
`ad7333813b54202579a4960b3635b67691f4810e9847a11607ef31b3445a8c8f`.
It is copied into an owned fixture and is not installed system-wide.

The executed cases cover deployment workload bursts; PostgreSQL loss during
ordinary upload, LFS PATCH, OCI blob upload and S3 multipart upload; MinIO loss
during ordinary upload and LFS staging; Redis loss during reads; MinIO partition
and LFS repair; packet duplication/reordering and asymmetric network recovery;
and live verifier clock skew. Each uses disposable Compose infrastructure and
its operation-specific committed-data or recovered-byte assertions. This is
evidence for the named fault cases, not cluster-wide exactly-once execution.

Log: `.codex/validation/protocol-faults-current.log`.
Detailed results: `.codex/validation/shardline-protocol-faults.V70ZW5/test.log`.
The operator guide now states the existing storage-cutover rule explicitly:
pause writers and GC; saved copy history does not coordinate cutover. The
acceptance checklist maps ordinary operation effects, ownership and interruption
evidence, retaining the remaining backend/reference review requirements.

### PostgreSQL latest-pointer mutation interruption

The owned process verifier now includes a semantic
`postgres_rebuild_interruptions` module. It writes two immutable empty-file
version records through the public PostgreSQL record API, then runs the real
ordinary index-rebuild CLI. An owned trigger pauses the second latest-pointer
insert after the first has committed. Actual SIGTERM terminates the original
CLI; its paused server-side query still retains the maintenance advisory owner.
The fixture explicitly cancels that query and waits for its backend to end
before removing the trigger. It does not claim immediate cancellation of arbitrary
remote I/O when a client exits.

The committed first head survives and the interrupted second insert rolls back.
Both immutable versions remain exact. Native history contains three records;
read-only shared inspection does not resume rebuilding. A later third version
is added, and a fresh ordinary rebuild discovers it and restores all three
heads. Earlier immutable versions, protected payload bytes and interrupted native
SQLite/WAL bytes remain unchanged. The original receipt stays in reconciliation;
the new completed receipt must match the fresh CLI PID, so a prior completed
rebuild cannot satisfy the assertion.

The final owned wrapper passes the existing 25 receipt-publication cuts, partial
repair cut and this new partial-rebuild cut. The new test passes in 2.49 seconds.
This verifies PostgreSQL latest-pointer transactions and fresh discovery with
metadata-only empty files; dedupe/reconstruction mutations and other physical
reference layouts retain separate audit requirements. No production code or
version changed. Strict CLI library/test Clippy, formatting, shell syntax and
diff checks pass. The initial Clippy run identified two checked-JSON-access
fixes in the new test; the corrected strict run passes.

Logs: `.codex/validation/postgres-rebuild-owned-final.log`,
`.codex/validation/postgres-rebuild-clippy-final.log` and
`.codex/validation/postgres-rebuild-graph.log`.
Reproduce with the extended
`verify_maintenance_postgres_process_cuts.sh CURRENT RELEASED CLI_TEST_HARNESS`.
