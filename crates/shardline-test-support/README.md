# shardline-test-support

Test utilities, Docker container lifecycle management, and test infrastructure
helpers for Shardline integration tests. Provides `DockerLocalStack` for managing
Docker-based test dependencies (Postgres, MinIO, Redis) with automatic cleanup,
port allocation, and health checks. Used by integration tests across multiple
crates that require real infrastructure.

See the [main Shardline README](../../README.md) for the project overview.
