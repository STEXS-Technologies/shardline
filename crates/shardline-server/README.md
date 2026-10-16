# shardline-server

The Shardline HTTP server library. Combines the Xet, Git LFS, OCI, Bazel, Hub,
and S3 frontends with local or S3-compatible object storage and SQLite or
Postgres metadata. Built on `shardline-server-core` and the protocol adapter
crates; started by `shardline serve` in the `shardline` binary crate.

Startup tracking runs inside the server process. GC, fsck, rebuild, and backup
remain explicit operator commands; starting the server does not schedule GC.
See the [maintenance guide](../../docs/MAINTENANCE_INTEGRATION_UX.md).

See the [main Shardline README](../../README.md) for the project overview.
