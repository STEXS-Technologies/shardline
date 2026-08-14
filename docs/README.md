# Shardline Docs

Shardline is a protocol-neutral content-addressed storage engine, optimized for
deduplicated model, dataset, container and build-artifact distribution, with Xet, OCI,
Git LFS, S3, HuggingFace Hub and cache-compatible frontends.

Use this index to find the shortest path for your task.
If you are new to the project, start with deployment, then read the protocol or operator
docs that match your use case.

## Start Here

- [Deployment](DEPLOYMENT.md)
- [Operations](OPERATIONS.md)
- [Architecture](ARCHITECTURE.md)
- [Compatibility Status](COMPATIBILITY_STATUS.md)
- [CLI](CLI.md)

## Setup And Rollout

- [Provider Setup Guide](PROVIDER_QUICKSTART.md)
- [Client Configuration](CLIENT_CONFIGURATION.md)
- [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md)
- [Providerless Direct Xet Backend](DEPLOYMENT.md#providerless-direct-xet-backend)

## Runtime And Contracts

- [Protocol Frontends](PROTOCOLS.md)
- [S3 Frontend](S3_FRONTEND.md)
- [HuggingFace Hub API](HUGGINGFACE_HUB_API.md)
- [Xet-Native File Management CLI](XET_NATIVE_CLI.md)
- [Authentication](AUTHENTICATION.md)
- [Object Reachability Model](reachability-model.md)
- [CDC Chunking and Storage Representation](CDC_MIGRATION.md)
- [Xet Protocol Conformance](PROTOCOL_CONFORMANCE.md)
- [Storage Adapters](STORAGE_ADAPTERS.md)
- [Cache Adapters](CACHE_ADAPTERS.md)
- [Provider Adapters](PROVIDER_ADAPTERS.md)
- [Security and Invariants](SECURITY_AND_INVARIANTS.md)

## Operator Workflows

- [Database Migrations](DATABASE_MIGRATIONS.md)
- [Fsck](FSCK.md)
- [Lifecycle Repair](LIFECYCLE_REPAIR.md)
- [Index Rebuild](INDEX_REBUILD.md)
- [Garbage Collection](GARBAGE_COLLECTION.md)
- [Backup Manifest](BACKUP.md)
- [Storage Migration](STORAGE_MIGRATION.md)
- [systemd](SYSTEMD.md)
- [Kubernetes Manifests](k8s/README.md)

## Performance And Profiling

- [Performance](PERFORMANCE.md)
- [Profiling](PROFILING.md)

## Planning And Release

- [Production-Readiness Plan](SHARDLINE_PRODUCTION_READINESS.md)
- [SDX Client Plan](SDX_PLAN.md)
- [Coordinated crates.io Release](RELEASE.md)
