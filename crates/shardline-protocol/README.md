# shardline-protocol

Core protocol types shared across all Shardline crates. Provides the foundational
data types that every other crate depends on: token types (`TokenScope`,
`TokenClaims`, `RepositoryScope`, `TokenSigner`), content hashes
(`ShardlineHash`), byte range types (`ByteRange`), security primitives
(`SecretString`), timestamp handling, and repository provider types
(`RepositoryProvider`). This is the lowest-level crate in the workspace.

See the [main Shardline README](../../README.md) for the project overview.
