# shardline-fsck

Filesystem consistency checker for Shardline metadata and object storage.
Scans index records, chunk references, and object storage to detect and report
inconsistencies such as missing chunks, orphaned metadata, and reference cycles.
Invoked via `shardline fsck` and also used programmatically as a library.

See the [main Shardline README](../../README.md) for the project overview.
