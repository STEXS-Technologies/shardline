# Maintenance integration: merge checklist

This branch adds automatic task history to existing startup and maintenance.
It is **not ready to merge**. The version stays unchanged; a version bump belongs
in a separate PR.

Operators should start with the [maintenance guide](MAINTENANCE_INTEGRATION_UX.md).
This document is for reviewers deciding whether the integration is safe to ship.

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
  Existing object-backed history is not moved or discarded. Such history can
  still contaminate a released copy, and PostgreSQL/S3 CLI configurations still
  publish object-backed evidence. Those cases, S3-only copy endpoints and
  deployment permissions remain required work before the full isolation gate
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
  Reproduce the local prototype with
  `bash scripts/shardline/verify_maintenance_metadata_inventory.sh RELEASED`,
  where `RELEASED` is the official 1.11.3 executable. This checks the proposed
  storage surface only; it does not exercise the production task backend.
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
