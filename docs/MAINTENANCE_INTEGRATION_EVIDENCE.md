# Maintenance integration: detailed evidence

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
