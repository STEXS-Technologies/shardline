# Stability Graduation Policy

This document defines the evidence required before a Shardline surface moves from
Experimental or Beta to Stable. A maturity label records demonstrated behavior; changing
the label is never the objective by itself.

The policy applies to protocol frontends, storage and cache adapters, authentication
providers, provider integrations, and deployment topologies. The current labels remain
listed in [Compatibility Status](COMPATIBILITY_STATUS.md).

## Universal Stable Gate

A user-facing surface may be marked Stable only when all applicable requirements below
have checked-in evidence:

- an explicit supported behavior contract and documented exclusions
- real upstream clients with a maintained client/version matrix
- authenticated, unauthorized, expired-token, and repository-isolation flows
- concurrent same-resource and different-resource operations
- interrupted, retried, duplicated, and reordered operations
- malformed and adversarial input, including bounded parser and body limits
- resource ceilings for memory, disk, sessions, and in-flight work
- restart durability across every acknowledged persistence boundary
- multi-replica tests for shared mutable state
- backward-compatible configuration and an upgrade/rollback statement
- fuzz or property coverage for custom parsers and state machines
- operator diagnostics that distinguish transient, permanent, and integrity failures

Requirements that do not apply must be marked explicitly with a reason in the surface's
compatibility documentation. They must not be silently omitted.

## Surface-Specific Gates

### Protocol Frontends

- client-visible retry and idempotency behavior is documented
- digest, length, conditional-write, and partial-upload failures are tested
- mutable names such as refs, tags, and object keys have a concurrency contract
- protocol-specific deletion cannot bypass retention or reachability invariants

### Cache Adapters

- cache loss, corruption, timeout, and unavailability never change correctness
- slow cache operations have bounded latency and do not exhaust request workers
- cache stampedes remain bounded across one and multiple Shardline processes
- complete cache eviction during active traffic recovers without operator action

### Authentication Providers

- the operator CLI can mint tokens for providers that support local signing
- signing and verification-only modes interoperate
- supported key formats round-trip through the operator and server paths
- tampering, wrong keys, expiry, scope binding, and repository binding fail closed
- key rotation and overlapping verification windows are documented

### Provider Integrations

- webhook processing is idempotent under duplicate and replayed delivery
- out-of-order rename, transfer, visibility, access-revocation, and deletion events
  converge to the provider's authoritative state
- bad signatures and cross-repository events fail closed
- provider unavailability has bounded behavior and cannot silently expand access

### Deployment Topologies

- the topology has an explicit writer model: single writer, read replicas, or multiple
  concurrent writers
- every shared mutable resource has a database-enforced uniqueness, compare-and-set,
  transactional lock, or fenced-lease contract
- stale writers cannot commit after ownership changes
- rolling mixed-version operation is covered for the documented compatibility window
- destructive GC excludes concurrent writers across local processes and Postgres replicas
- backup, destructive loss, restore, integrity verification, and traffic return are
  exercised end to end

## Current Graduation Work

| Surface | Current tier | Principal evidence still required |
| --- | --- | --- |
| Git LFS | Beta | broader platform evidence; pinned Git LFS 3.7.1/3.6.1 client flows, Postgres cross-replica resume, retry/resume, mixed parallel operations, same-OID concurrency, and length/digest failures have checked-in coverage |
| Bazel HTTP cache | Beta | multi-replica evidence and broader platform evidence; pinned Bazel 8.7.0/9.2.0 client flows, same-digest concurrency, AC/CAS isolation, interrupted PUT, and large objects have checked-in coverage |
| S3 frontend | Stable | conditional writes have an adapter-level database CAS contract with concurrent SQLite and Postgres-handle evidence; unconditional overwrites remain documented last-writer-wins |
| Hugging Face Hub API | Beta | broader client matrix and remaining semantic compatibility; tenant-bound route extraction, stale-parent rejection, Postgres cross-replica ref serialization, and delete-vs-push row locking are covered |
| Redis reconstruction cache | Beta | broader production and provider-version evidence; partition, flush, kill/restart, bounded timeout, corruption repair, cross-replica cold-load serialization, cancellation takeover, and stale-owner fencing are covered |
| Provider integration | Beta | revocation timing and broader live-provider evidence; duplicate/replay handling, atomic monotonic reconciliation-state merging, one-transaction fenced rename/delete with atomic delivery claims, repository-scoped event serialization with persisted ownership epochs, terminated-owner detection, access/revision reordering, rename migration, signature checks, and fail-closed visibility parsing are covered |
| Ed25519 | Beta | broader deployment evidence; bounded overlapping multi-key verification, zero-downtime rotation, operator minting, CLI-to-provider verification, key formats, tampering, expiry, wrong-key, malformed-token, and algorithm-confusion paths are covered |
| Multi-replica Postgres/S3 writers | Stable | durable resumable sessions remove RWX state; persisted lock epochs, fenced publication, writer-excluding GC, tombstone reclamation, cross-replica protocol tests, deterministic chaos, Postgres-kill and MinIO-kill mid-LFS-PATCH, netem partition during overlapping LFS PATCH repair, Postgres-kill mid-OCI blob-upload, Postgres-kill mid-S3 multipart, terminated-owner takeover, migration serialization, and backup-destroy-restore-fsck-download drills are checked in |

## Promotion Review

Every promotion change must include:

1. links to the tests that satisfy each applicable gate
2. the supported client and deployment matrix
3. remaining exclusions and known limitations
4. failure-injection results for mutable or distributed state
5. updated compatibility, operations, security, and upgrade documentation

If any correctness requirement remains open, the surface remains Beta or Experimental.
Stable does not mean feature-complete; it means the documented contract is defensible.
