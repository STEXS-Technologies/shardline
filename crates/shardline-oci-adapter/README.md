# shardline-oci-adapter

OCI Distribution protocol adapter for container image and artifact storage.
Implements blob upload/download, manifest management, tag resolution, and
session management as specified by the OCI Distribution spec. Used by the
server when the OCI frontend is enabled. Reuses the shared CAS coordinator
for chunk-level deduplication across OCI blobs.

See the [main Shardline README](../../README.md) for the project overview.
