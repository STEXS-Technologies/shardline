# Shardline 1.14.0 release work

Status: development; release is blocked. Do not tag, publish, or describe the
cleanup checkpoint as a release candidate qualified for production.

## Scope

The release includes the simplification work, efficient authenticated random
access, and recoverable metadata migration. Passing cleanup tests alone does not
complete this scope. The future API framework migration is separate.

The design keeps SQLite/PostgreSQL authoritative for mutable state and atomic
publication, and CAS authoritative for immutable payloads. Independently durable
metadata supplements SQL; it does not turn an object bucket into a replacement
transaction database. Reuse existing verification, storage, lifecycle, rebuild,
and fsck machinery.

## Ordered gates

| Work | Required result | Current disposition |
| --- | --- | --- |
| Integrate current main | Preserve maintenance changes and new main fixes; resolve conflicts and verify | Main a6847ad4 merged into cleanup at e6039bd8; 40 hash tests pass |
| Contributor gate | Format, strict Clippy, dependency policy, tests, migration parity | Merge passes format and Clippy; dependency policy fails, later tasks not executed |
| Reliability failures | Reproduce, diagnose and fix each failure; retain negative checks | Local historical replacement passed 20/20 with published 1.12.1; adjacent replacement passed 2/2 with published 1.13.0. The original hosted run remains failed evidence, and no hosted rerun has occurred. |
| Historical vs adjacent compatibility | Explicit predecessor per contract; preserve 1.12.1 feature-transition assertions and qualify 1.13.0 separately | Workflow matrix separates the two. Historical selector passed 20/20; adjacent local and PostgreSQL cutover selectors passed 2/2. Direct 1.13.0 restart after current-only writes is rejected and is not a supported rollback path. |
| Range source coherence | Same provider generation for suffix, footer and each selected member | S3 snapshot foundation committed at `bef21d23`; pins strong ETag/version and length, validates exact range response/body. Snapshot generation and malformed ETag tests pass; provider-specific 412/unsupported-adapter matrix remains incomplete. |
| Selective serialized-Xorb transfer | Opt-in; original packed bytes, exact capability scope, bounded admission, first-member verification before headers | Implemented behind `SHARDLINE_SELECTIVE_SERIALIZED_XORB_RANGES` / `server.selective_serialized_xorb_ranges`, false by default. S3 authenticated-footer v1 only; selected bytes are verified before response creation; unsupported/broad layouts retain strict whole-object path. |
| Shared range proof | Reuse existing trusted-root/footer/member verification; no second Merkle or storage format | Both logical-file and serialized-Xorb paths share bounded snapshot-footer loading, `validate_serialized_xorb_footer`, `decode_authenticated_xorb_chunk`, and layout-bound repository coverage. No new wire format or proof engine. |
| Range qualification | Real MinIO/local sources, malformed responses, replacement, cancellation, multi-replica/GC races, stock native clients and measured backend I/O | Actual MinIO 4 MiB incompressible fixture: cold 4 KiB native range used 3 GETs / 68,744 bytes vs 4,198,008 full-object bytes; MinIO footer/member/corruption and same-root layout-denial checks pass. An incompressible member with 8 MiB decoded / 8 MiB + 8 bytes packed falls back after suffix/footer only (2 GETs), proving the packed-fetch budget is applied before payload GET. S3 HTTP opt-in endpoint returns exact 206 bytes and denies outsider scope before provider GET. Config, server download-stream, snapshot and ETag tests pass. Raw range logs: `/home/ac/projects/.shardline-cleanup-state/release-114/selective-minio.log`. Still pending: supported stock-client matrix, broad workload/p95/p99/cost sweep, version-swap/412 and unsupported-provider fault matrix, multi-replica/GC races, and complete P0–P5 deployment qualification. |
| Recovery contract | Versioned archive format, deployment/domain isolation, keys, ordering, coverage and R1/R2/R3 guarantees | Accepted `docs/RECOVERABLE_METADATA.md`. Hub ref SHADOW receipts, NDJSON/Git receive-pack version manifests, and signed per-repository current-ref checkpoints are integrated. The checkpoint binds a consistent SQL snapshot to an archived watermark; it is not complete mutation coverage or a restore path. Recovery remains R0; no R1/R2/R3 claim is enabled. |
| Atomic recovery outbox | SQLite and PostgreSQL append in the owning publication transaction; per-domain order and idempotence | Hub ref batches only: additive frontend migrations, OFF default, SHADOW appends only actual ref changes (empty/no-op batches allocate no sequence). NDJSON SHADOW publications store a version manifest before SQL visibility and pin its locator in the committed outbox event. SQLite rollback/no-op and PostgreSQL ordering/no-op tests pass. Other Hub mutations and non-Hub domains are uncovered. |
| Archive worker | Bounded ordered retries outside SQL locks; immutable verified writes; gaps/conflicts fail closed | A scheduled worker pages at most 16 pending repositories, archives at most one next event per repository per pass, verifies immutable readback and predecessor linkage, and marks only the verified event. Page and archive operations use request deadlines; shutdown cancels active async work; failures remain pending for retry with bounded backoff. Worker unit tests cover prompt shutdown, pass wrap, and failure continuation. No status/backpressure API, complete archive inventory, resumable catalog backfill, or restore enumeration is integrated; R04 and R05 remain incomplete. |
| Backfill/checkpoint | Resumable historic scan and certified complete coverage; manifests alone never establish publication | Implementation commit `5621b142` adds `shardline backup hub-checkpoint --repository-id ID`, which publishes a signed state-only snapshot for one repository. The writer requires SHADOW and equal SQL/archive watermarks, verifies the terminal receipt when present, and reads the immutable object back before returning its pin. Docker smoke test `operator_cli_fails_offline_then_publishes_a_pinned_local_checkpoint` passed 1/1 against local SQLite and disposable MinIO. Its offline endpoint case exited 2 with an archive error; afterward the SQLite BLAKE3 was unchanged and mode still SHADOW. The successful subprocess emitted the object key, digest and byte length; SQLite BLAKE3 again matched its baseline, and the test independently read and verified the pinned object. This is not a resumable backfill, catalog inventory proof, full receipt-chain verification, full-domain state checkpoint, payload-closure proof, freshness proof without an external pin, or isolated restore. |
| Isolated restore | Destroy disposable metadata DB and backup; replay authenticated archive into fresh DB, use existing derived rebuild and fsck, verify bytes | Not implemented |
| Mutable state and GC | Covered refs, deletions, policies, holds and unresolved deletes; valid recovery roots protected | Not implemented |
| Protected acknowledgments | Explicit enforced mode; no protected success before durable receipt; bounded archive outage error; exclude old writers | Not implemented |
| Frozen integrated qualification | Same source SHA and binaries for contributor, SQL/object-store, real clients, recovery, GC, resource and upgrade checks | Pending. PostgreSQL tests cover atomic ref updates, domain-ordered outbox writes, and prepared manifest linkage. Docker test `postgres_minio_shadow_ndjson_receipts_survive_router_restart` passed (1/1): authenticated NDJSON HTTP publications stored manifests and linked signed receipts in MinIO; after a server restart with the same PostgreSQL and MinIO state, the next signed receipt extended its predecessor. The actual CLI smoke test above separately covers a local SQLite source and MinIO checkpoint archive, including fail-closed archive outage and pinned readback. These prove targeted continuity and checkpoint publication, not process-loss or destructive source-loss restore, and do not qualify R1/R2/R3. Other recovery mutation coverage and full restore qualification remain pending. Candidate unit-test checkpoint: index 821 passed/6 ignored, reliability 73 passed, server 3,090 passed/28 ignored; strict Clippy passed for server/index/reliability/Hub API all targets and features. |
| Release artifacts | Coherent versions/lockfiles, changelog, package dry runs, images, migration and rollback runbooks | Prepare after scope is implemented; no version bump/tag now |

## Recovery promises and constraints

- R0 is the current matching-database-and-payload-backup recovery model.
- R1 reconstructs covered committed immutable history from archive plus CAS.
- R2 additionally reconstructs certified mutable state, deletion and policy facts.
- R3 promises recovery of protected acknowledged operations after metadata DB loss.

Do not advertise R1/R2/R3 until independently demonstrated on explicitly named
protocols and topologies. A hash chain stored only in the lost DB is insufficient.
A receipt must never certify a rolled-back transaction. A committed SQL mutation
may precede its archive receipt; shadow mode retains legacy recovery semantics.
Enforced mode must withhold protected success until the receipt is durable.
SQL-visible but unacknowledged mutations during that window remain a documented
limitation. Restore defaults to denying access until policy is revalidated.

Keep archive keys outside the metadata DB and separate archive retention and
credentials from ordinary destructive maintenance. Unknown deletes and missing
receipt predecessors cannot be converted into success. Multi-ref publication
must remain atomic on replay. OFF/SHADOW/ENFORCED transitions require an explicit
coverage report and supported predecessor/cutover procedure.

## Evidence discipline

Freeze a source SHA for each integrated run. Preserve commands, binary hashes,
client versions, backend topology, dataset identity, passed/failed/skipped counts
and raw logs outside Git. Never count conditional skips as backend qualification.
Measure unique and incremental uploads after retention expiry and GC, along with
backend GET bytes, SQL lock time, RSS and p95/p99 latency. Development-host numbers
do not establish production throughput or customer savings.

The earlier cleanup E2E run 44076507189 at 81c149a4 completed successfully. It
does not qualify later cleanup commits, the merge, or the unimplemented features.
Main reliability run 45003241777 at a6847ad4 failed: mixed-version job reports
11 passed/9 failed/30 skipped; deployment job reports 40 passed/1 failed/9 skipped.
Its replacement evidence is local and targeted, not a hosted rerun. The historical
selector used the published 1.12.1 executable (SHA-256
`e54b2006e0b763f20cea54fafd3d3950d5d72b9029ceb29afc9ae13c7df6f7e0`) and passed
20/20 selected tests. The adjacent selector used the published 1.13.0 executable
(SHA-256 `94cb40b7ae5d23ca18ffc48d06398cd6f11d6ddd7708c7513f1c5cda350a949f`)
and passed 2/2 selected tests. Raw logs are in
`/home/ac/projects/.shardline-cleanup-state/release-114/`:
`historical-1.12.1-qualification.log`, `adjacent-1.13-cutover.log`, and
`adjacent-1.13-distributed-phase-trace.log`. The latter records the old binary's
expected fail-closed rejection only after current-only writes in the former
unrestricted rollback drill.

## Adjacent-release cutover and rollback

For the 1.13.0 predecessor, drain and stop old writers before applying current
schema migrations. Keep a matching metadata and immutable-payload backup. Start
the candidate and verify reads of predecessor bytes. The old binary rejects the
upgraded frontend migration history; do not restart it on that database. To
rollback before current-only writes, restore the matching metadata and payload
backup into an isolated environment, start the predecessor there, and verify
byte-exact reads and a write/readback. After current-only writes are accepted,
use forward recovery or an explicitly approved restore that accepts losing
writes since the backup.

The PostgreSQL drill clones the pre-upgrade database and leaves its isolated
MinIO payload set unchanged while it verifies restore behavior. It does not
qualify an independent object-store backup procedure; production rollback must
restore the matching payload backup as well. The 1.12.1 historical workflow
continues to run its existing mixed-version rollout contract separately.

## Dependency policy blockers

The root and E2E locks now use patched `parquet` 60.2.0 and `url` 2.5.9. The
remaining `cargo deny` failures are unmaintained [`chrono`
(RUSTSEC-2026-0543)](https://rustsec.org/advisories/RUSTSEC-2026-0543) and
[`humantime` (RUSTSEC-2026-0542)](https://rustsec.org/advisories/RUSTSEC-2026-0542);
neither advisory has a safe version fix. The [chrono maintainers recommend
Jiff or time](https://github.com/chronotope/chrono/issues/1768).
`chrono` is used directly by Shardline and remains a non-optional dependency of
Arrow/Parquet, `object_store`, Xet runtime and Statechronicle. Migrating
Shardline's timestamps is useful follow-up work, but cannot clear the advisory
without compatible upstream replacements or removal of those libraries.

`humantime` reaches the graph through `object_store` and Xet runtime. Upstream
[`object_store` PR #908](https://github.com/apache/arrow-rs-object-store/pull/908)
proposes a Jiff replacement but remains unreleased. Xet's current [runtime
manifest](https://github.com/huggingface/xet-core/blob/main/xet_runtime/Cargo.toml)
also declares `humantime`; track both upstream paths and recheck the dependency
graph after releases. Keep the dependency policy unchanged and report these
blockers until maintained replacements remove them from both locks.
