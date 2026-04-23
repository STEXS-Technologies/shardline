# Shardline Docs

Shardline is an open, self-hostable content-addressed storage backend with
Xet-compatible protocol support.

These docs describe how Shardline stores content-addressed data, exposes the current Xet
frontend, verifies protocol objects, and runs in self-hosted deployments.

`shardline serve` currently exposes the Xet frontend by default.
There is no runtime frontend selector yet; `--role` only splits API and transfer duties.

The default deployment model is a single process.
When traffic grows, the same `shardline serve` binary can be split into `api` and
`transfer` roles without changing the external protocol surface.

## Scope

Shardline covers:
- CAS coordinator behavior
- Xet protocol compatibility
- current Xet xorb and shard validation requirements
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
- [Xet Protocol Conformance](PROTOCOL_CONFORMANCE.md)
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
- [Operations](OPERATIONS.md)
- [systemd](SYSTEMD.md)
- [Kubernetes Manifests](k8s/README.md)
- [Providerless local bootstrap is built into `shardline serve` and `shardline providerless setup`](DEPLOYMENT.md#providerless-direct-xet-backend)
