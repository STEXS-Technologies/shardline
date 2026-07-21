# shardline-index

Metadata indexing and record storage for files, chunks, and repository state.
Supports SQLite (local deployments) and Postgres (production deployments) backends.
Provides the `IndexStore` and `RecordStore` traits that the server uses to
persist file metadata, chunk references, repository scopes, lifecycle state,
provider events, and Hub API records. Database migrations are managed per-backend.

See the [main Shardline README](../../README.md) for the project overview.
