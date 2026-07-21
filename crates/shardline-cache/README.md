# shardline-cache

Reconstruction cache adapters supporting in-memory and Redis backends.
Provides a pluggable `AsyncReconstructionCache` trait used by the server to
cache reconstructed file metadata and chunk locations, reducing redundant
computation during repeated downloads. The in-memory backend is the default;
Redis is used in multi-node deployments for a shared cache.

See the [main Shardline README](../../README.md) for the project overview.
