# shardline-storage

Content-addressed object storage adapters for local filesystem and S3-compatible
backends. Defines the `ObjectStore` trait that all storage operations go through,
with implementations for local disk (symlink-safe, with directory walk and
metadata caching) and S3 (multipart upload, streaming, with support for MinIO,
AWS S3, and any S3-compatible store). Also provides object key types and path
validation.

See the [main Shardline README](../../README.md) for the project overview.
