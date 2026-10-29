# shardline-test-support

Test utilities, Docker container lifecycle management, and test infrastructure
helpers for Shardline integration tests. Provides `DockerLocalStack` for managing
Docker-based test dependencies (Postgres, MinIO, Redis) with automatic cleanup,
port allocation, and health checks. Used by integration tests across multiple
crates that require real infrastructure.

With the `docker` feature, `with_minio_tls()` provides HTTPS MinIO and its
disposable CA certificate. Storage preflight verifies actual object writes and
read-back over TLS. `HttpsDocumentFixture` serves static HTTPS documents for
authentication tests using OpenSSL, with an owned process and startup deadline.
Keep test CA trust isolated from other tests; neither fixture disables certificate
verification. OpenSSL must be installed for these TLS fixtures.

See the [main Shardline README](../../README.md) for the project overview.
