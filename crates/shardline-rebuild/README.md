# shardline-rebuild

Index rebuild from stored metadata and CAS object inventory. Scans the CAS
object store and reconstructs index records for files, chunks, and shards when
the metadata database is lost or corrupted. Invoked via `shardline index rebuild`.
Supports dry-run mode for impact assessment before actual rebuild.

Latest-record repairs require a complete, valid version-record scan. If a version
record is unreadable or invalid, the rebuild reports the issue and defers all
latest-record creation, replacement, and removal. A readable older version must
never replace an acknowledged head or be presented as latest while a newer
version may be hidden by corruption. This protection applies across files because
corrupt records can have opaque locators with no trustworthy file identity.
Repair the reported version records and rerun to resume latest-record repairs.

See the [main Shardline README](../../README.md) for the project overview.
