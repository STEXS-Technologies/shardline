# shardline-gc

Garbage collection for orphaned chunks and unreferenced CAS objects.
Implements quarantine-based GC: chunks are first moved to a quarantine state,
then deleted after a configurable retention period if no new references appear.
GC also inventories serialized-xorb containers and retained shards, resolves
xorb containers to their constituent chunk hashes, and prunes xorb cache
sidecars when their parent xorb is swept.
Supports concurrent upload/GC safety and produces detailed GC reports.
Invoked via `shardline gc`.

See the [main Shardline README](../../README.md) for the project overview.
