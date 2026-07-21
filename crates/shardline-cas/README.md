# shardline-cas

Content-addressed storage coordinator for chunk-level deduplication.
Provides the `CasCoordinator` composition struct that frontends use to store
and retrieve chunks and xorbs by content hash. Coordinates between the
index store and object store to implement dedup-aware upload and reconstruction
operations across all protocol frontends (Xet, LFS, OCI, Bazel, Hub).

See the [main Shardline README](../../README.md) for the project overview.
