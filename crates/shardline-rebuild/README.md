# shardline-rebuild

Index rebuild from stored metadata and CAS object inventory. Scans the CAS
object store and reconstructs index records for files, chunks, and shards when
the metadata database is lost or corrupted. Invoked via `shardline rebuild`.
Supports dry-run mode for impact assessment before actual rebuild.

See the [main Shardline README](../../README.md) for the project overview.
