# Shardline Docs

Shardline is an open, self-hostable backend for the Xet protocol.

These docs describe how Shardline stores content-addressed data, coordinates Xet uploads
and downloads, verifies protocol objects, and runs in self-hosted deployments.

The default deployment model is a single process.
When traffic grows, the same `shardline serve` binary can be split into `api` and
`transfer` roles without changing the external protocol surface.

## Scope

Shardline covers:
- Xet protocol compatibility
- CAS coordinator behavior
- xorb and shard validation requirements
- reconstruction and range-download behavior
- storage adapter contracts
- metadata/index storage requirements
- GitHub, Gitea, GitLab, and other VCS integration boundaries
- Docker and self-hosted deployment expectations

Shardline does not cover:
- unrelated application-domain semantics
- product-specific ownership rules unless they are explicitly part of an integration
  profile

## Documentation

Setup and deployment:
- [Deployment](DEPLOYMENT.md), including
  [Providerless Direct Xet Backend](DEPLOYMENT.md#providerless-direct-xet-backend)
- [Provider Setup Guide](PROVIDER_QUICKSTART.md)
- [Client Configuration](CLIENT_CONFIGURATION.md)
- [CLI](CLI.md)
- [Architecture](ARCHITECTURE.md)
- [Compatibility Status](COMPATIBILITY_STATUS.md)
- [Protocol Conformance](PROTOCOL_CONFORMANCE.md)
- [Storage Adapters](STORAGE_ADAPTERS.md)
- [Cache Adapters](CACHE_ADAPTERS.md)
- [Provider Adapters](PROVIDER_ADAPTERS.md)
- [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md)
- [Database Migrations](DATABASE_MIGRATIONS.md)
- [Fsck](FSCK.md)
- [Lifecycle Repair](LIFECYCLE_REPAIR.md)
- [Index Rebuild](INDEX_REBUILD.md)
- [Garbage Collection](GARBAGE_COLLECTION.md)
- [Backup Manifest](BACKUP.md)
- [Storage Migration](STORAGE_MIGRATION.md)
- [Performance](PERFORMANCE.md)
- [Profiling](PROFILING.md)
- [Security and Invariants](SECURITY_AND_INVARIANTS.md)
- [Production Security Audit](PRODUCTION_SECURITY_AUDIT.md)
- [Operations](OPERATIONS.md)
- [systemd](SYSTEMD.md)
- [Kubernetes Manifests](k8s/README.md)
- [Providerless local bootstrap is built into `shardline serve` and `shardline providerless setup`](DEPLOYMENT.md#providerless-direct-xet-backend)
- [Provider Setup Guide](PROVIDER_QUICKSTART.md)
- [Client Configuration](CLIENT_CONFIGURATION.md)
- [CLI](CLI.md)
- [Architecture](ARCHITECTURE.md)
- [Compatibility Status](COMPATIBILITY_STATUS.md)
- [Protocol Conformance](PROTOCOL_CONFORMANCE.md)
- [Storage Adapters](STORAGE_ADAPTERS.md)
- [Cache Adapters](CACHE_ADAPTERS.md)
- [Provider Adapters](PROVIDER_ADAPTERS.md)
- [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md)
- [Database Migrations](DATABASE_MIGRATIONS.md)
- [Fsck](FSCK.md)
- [Lifecycle Repair](LIFECYCLE_REPAIR.md)
- [Index Rebuild](INDEX_REBUILD.md)
- [Garbage Collection](GARBAGE_COLLECTION.md)
- [Backup Manifest](BACKUP.md)
- [Storage Migration](STORAGE_MIGRATION.md)
- [Performance](PERFORMANCE.md)
- [Profiling](PROFILING.md)
- [Security and Invariants](SECURITY_AND_INVARIANTS.md)
- [Production Security Audit](PRODUCTION_SECURITY_AUDIT.md)
- [Operations](OPERATIONS.md)
- [systemd](SYSTEMD.md)
- [Kubernetes Manifests](k8s/README.md)
- [Local Kubernetes E2E](k8s/local-e2e/README.md)
