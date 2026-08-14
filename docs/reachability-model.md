# Object Reachability Model

**Applies to:** garbage collection, fsck, repair, deletion, retention holds, and the
CAS upload lifecycle

> **Internals note:** the single authority behind this model is the
> `ObjectReachability` trait in `shardline_cas::reachability`, whose implementation
> performs an object-presence lookup (`AsyncIndexStore::contains_object()`) against
> the stored-objects table (`shardline_stored_objects`). The durable upload-intent
> records live in the upload-intents table (`shardline_upload_intents`). The plain
> terms below describe the conceptual model; lifecycle tools must go through the
> trait, not the tables directly.

## Definition

An object is **reachable** if the index has a record of it as a committed
content-addressed storage entry.
Reachable objects must never be deleted by GC or any lifecycle tool.

## Object States

| State | Meaning | Visible to reads? | Collectible by GC? |
| --- | --- | --- | --- |
| **Registered** | The object's hash is recorded in the object-presence index (or an equivalent presence record). | No (needs a reconstruction record to be part of a file) | No |
| **Referenced** | A committed file reconstruction lists this object in its reconstruction terms. | Yes (through the file record) | No |
| **Visible** | The object is referenced AND a latest-file record points to the reconstruction. | Yes | No |
| **Orphaned** | Object bytes exist on disk but the object is neither registered nor referenced by any committed record. | No | Yes (after quarantine grace period) |

## Reachability Rules

1. **Index presence implies reachability.** An object whose hash is recorded in the
   object-presence index is registered and must not be collected.
   This covers objects referenced by:
   - Committed file reconstruction records
   - Pending upload-intent records (the durable upload-intent lifecycle now exists and
     is used by the CAS upload path; treating pending intents as GC roots is still
     pending — see P0.2 in `SHARDLINE_PRODUCTION_READINESS.md`)

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

The canonical reachability check is the object-presence lookup (the
`ObjectReachability::is_object_reachable()` contract). This is the single authority used
by all lifecycle tools.

## Scale-Specific Strategies

| Tool | Method | Why |
| --- | --- | --- |
| GC (orphan scan) | Walk records → collect referenced key set → list all objects → subtract | Enumeration — must find what the index doesn't know about |
| fsck (reconstruction check) | Object-presence lookup per term | Point query — checking specific expected objects |
| Repair (integrity check) | Object-presence lookup per reference | Point query — verifying specific references |
| Deletion (hold-aware) | Object-presence lookup + retention hold check | Point query — deciding if one object can be removed |

All tools use the same definition of reachability.
The implementation varies by scale (enumeration vs point query), but the logical model
is identical.
