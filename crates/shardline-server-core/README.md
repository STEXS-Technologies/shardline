# shardline-server-core

Shared server foundations used by the main server crate and protocol adapters.
Provides the `ServerObjectStore` abstraction over backend storage (local, S3),
the `ServerFrontend` dispatch system for routing requests to protocol-specific
handlers, `Validation` utilities for content hashes and paths, and protocol
support helpers. This is the common base that the server crate builds on.

See the [main Shardline README](../../README.md) for the project overview.
