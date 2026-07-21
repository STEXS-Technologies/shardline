# shardline-server

The Shardline server binary crate. Combines all frontends (Xet, Git LFS, OCI,
Bazel, Hub), backends (local filesystem, S3-compatible, Postgres), and services
(GC, fsck, rebuild, backup, provider integration, metrics, tracing) into a
single runnable server. Built on top of `shardline-server-core` and all protocol
adapter crates. Started via `shardline serve`.

See the [main Shardline README](../../README.md) for the project overview.
