# Object Reachability Model

**Applies to:** garbage collection, fsck, repair, deletion, retention holds, and the
CAS upload lifecycle

GC's reference walk is implemented in `crates/shardline-gc/src/reachability.rs`.
It enumerates committed records and their physical references. The separate
`ObjectReachability` trait performs an object-presence point lookup through
`AsyncIndexStore::contains_object()`; it does not make the complete GC decision.

## Definition

An object is **reachable to GC** if a committed file record references it, directly
or through its storage representation. Active retention holds and live resumable
staging provide additional deletion protection.
Reachable objects must never be deleted by GC or any lifecycle tool.

## Object States

| State | Meaning | Visible to reads? | Collectible by GC? |
| --- | --- | --- | --- |
| **Registered** | The object's hash is recorded in the object-presence index. This alone is not a root in GC's full reference walk. | Depends on the frontend | Yes, if no live reference or other protection remains |
| **Referenced** | A committed file reconstruction lists this object in its reconstruction terms. | Yes (through the file record) | No |
| **Visible** | The object is referenced AND a latest-file record points to the reconstruction. | Yes | No |
| **Orphaned** | Managed object bytes exist but no committed record references them and no other deletion protection applies. | No committed-file visibility | Yes (after quarantine grace period) |

## Reachability Rules

1. **Committed records supply roots.** GC walks latest and immutable version
   records, resolves xorb member chunks, and preserves dedupe shards for live
   chunk hashes. A dedupe mapping alone is derived state, not an independent root.
   Upload-intent evidence alone is not an additional root in this walk.

2. **File record reference implies reachability.** An object referenced by any committed
   file record (version or latest) is reachable even if the index presence record is
   missing. GC must reconcile this during its mark phase.

3. **Retention holds override deletion.** An object with an active retention hold is
   preserved regardless of reachability.
   GC checks retention holds before deletion.

4. **Quarantine preserves before deletion.** Orphaned candidates enter quarantine for a
   configured grace period before physical deletion.
   During quarantine they are not reachable but are not yet deleted.

5. **Cache cannot create reachability.** Reconstruction cache entries are not
   authoritative. A missing cache entry does not imply unreachability, and a present
   cache entry does not make an unreachable object reachable.

## Reachability Function

`ObjectReachability::is_object_reachable()` answers whether the object-presence
index records a hash. GC instead enumerates the committed reference graph and
applies holds, quarantine, retention and clock guards. A positive point lookup
does not replace these checks.

Mutating requests hold the shared maintenance barrier; destructive GC holds its
exclusive side, preventing participating uploads from racing the final mark and
delete. Postgres resumable-session GC separately protects live part-map fragments.
An unresolved remote delete blocks conflicting protected publication until its
exact attempt is reconciled. Follow the [upgrade procedure](ROLLING_UPGRADE.md),
including its pause for older GC binaries.

## Scale-Specific Strategies

| Tool | Method | Why |
| --- | --- | --- |
| GC (orphan scan) | Walk records → collect referenced key set → list all objects → subtract | Enumeration — must find what the index doesn't know about |
| fsck (reconstruction check) | Validate expected records, references and bytes | Check integrity of committed content |
| Repair (integrity check) | Reconcile metadata with records, storage and verified evidence | Recover supported inconsistencies |
| Destructive sweep | Committed reference walk plus holds, quarantine, retention and clock checks under exclusive ownership | Decide which physical objects may be deleted |

See [garbage collection](GARBAGE_COLLECTION.md) and
[resumable sessions](RESUMABLE_SESSION_ARCHITECTURE.md) for the complete workflows.
