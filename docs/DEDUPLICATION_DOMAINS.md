# Chunk reuse domains

Chunk reuse follows the authorized repository scope across native Xet, S3,
basic Git LFS, Bazel, and OCI ingestion. The domain includes provider, owner,
repository, and optional revision. Different repositories, owners, providers,
or revisions do not share candidate visibility. Cross-repository and
cross-tenant candidate lookup is disabled. Independently packing identical bytes
can still produce the same content-addressed container. Domains constrain reuse
discovery and authorization; they do not create separate physical namespaces or
per-tenant encryption.

An upload without a repository scope uses the explicit global domain. Global
means global records only; it never means searching every repository. Deployments
serving independent customers must bind uploads to an authorized repository.

Protocol records keep their existing file identifiers, storage keys, and read
locations. A separate optional `deduplication_domain` records reuse ownership.
Native and legacy records without that field retain their original repository
scope, or the global domain if unscoped. Migration does not infer ownership of
legacy protocol records: those records remain global and cannot supply reuse to
new scoped uploads until their ownership is established through an authorized
rewrite.

The existing membership index remains a rebuildable candidate index. Publication
updates its domain in the same metadata transaction as the file record. Every
candidate still requires authoritative record membership and verified immutable
container members. Reuse publishes ordinary reconstruction references, preserving
garbage-collection reachability and durable-content-before-publication behavior.
Deleting a source record does not delete content referenced by another record.

Verified members are cached per ingestion with bounds on both container count
and total member count. Initial container verification still uses the existing
PostgreSQL remote-delete admission transaction. Its object-store reads and lock
duration need separate performance qualification; domain isolation does not
resolve that remaining cost.
