# shardline-cas

Content-addressed storage coordinator for chunk-level deduplication.
Provides the `CasCoordinator` composition struct with CAS workflow methods
(`store_content_addressed_blob`, `begin_upload`, `transition_upload`,
`with_upload_intent`) that frontends use to store and retrieve chunks and
xorbs by content hash, coordinating between the index store and object store.
Native Xet ingest is handled by the xet adapter rather than this coordinator.

See the [main Shardline README](../../README.md) for the project overview.
