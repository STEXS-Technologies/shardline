# Shardline

[![Status](https://img.shields.io/badge/status-stable-1f6feb)](docs/COMPATIBILITY_STATUS.md)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

**Shardline is a protocol-neutral content-addressed storage engine, optimized for
deduplicated model, dataset, container and build-artifact distribution, with Xet, OCI,
Git LFS, S3, HuggingFace Hub and cache-compatible frontends.**

Shardline is a self-hostable content-addressed storage (CAS) server.
It accepts immutable object uploads, deduplicates content, and serves range-aware
downloads. Run it standalone or pair it with GitHub, GitLab, or Gitea for
repository-scoped storage.

## Surface Maturity

| Surface | Tier | Evidence |
| --- | --- | --- |
| Xet CAS frontend | **Stable** | Native Xet upload/download flows and checked-in `git-xet` push/clone/fetch/pull coverage |
| Git LFS frontend | **Beta** | LFS batch negotiation and direct object routes; checked-in `git-lfs` push/pull/fetch coverage |
| Bazel HTTP remote cache frontend | **Beta** | `ac`/`cas` read and write routes; `bazel`/`bazelisk` remote-cache flows in test matrix |
| OCI Distribution frontend | **Stable** | Blob/manifest/tag routes and checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Model/dataset create, upload, download, delete; `hf` CLI workflows in test matrix |
| S3 frontend (protocol) | **Beta** | S3-compatible object API — Put/Get(+Range)/Head/Delete, conditional requests (If-Match/If-None-Match), CopyObject, multipart upload, ListObjectsV2, DeleteObjects, ListBuckets, bucket stubs; SigV4 access-key=token auth; validated against real clients (`mc`, pyarrow 25) in the real-client e2e suite and security-audited (`feat/s3-frontend`) |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS and mTLS connectivity; cache hit/miss paths validated |
| Provider integration (GitHub/GitLab/Gitea/Codeberg/generic) | **Beta** | Checked-in token issuance, webhook, and repository-scoped authorization coverage |
| Ed25519 auth provider | **Experimental** | Signing and verification, verification-only mode, configuration, and authenticated HTTP flows have targeted tests |

## What it does

- **Store and deduplicate** any binary content — datasets, model weights, build
  artifacts, media
- **Multiple protocols** — Xet (default), Git LFS, Bazel HTTP remote cache, OCI
  Distribution, S3, and HuggingFace Hub
- **HuggingFace Hub API** — drop-in alternative for `huggingface-cli` uploads and
  downloads
- **Pluggable auth** — local HMAC, Ed25519, OIDC, JWKS, or passthrough provider adapters
- **Self-hosted or cloud** — local filesystem, S3-compatible storage, Postgres metadata
- **Operational tooling** — health checks, migrations, integrity verification, garbage
  collection, backups
- **Provider integration** — optional webhooks and token issuance for GitHub, GitLab,
  Gitea, Codeberg

## Use cases

| Use case | How Shardline helps |
| --- | --- |
| **AI model distribution** | Store model weights with automatic deduplication. Pull specific model versions by content hash. HuggingFace Hub API compatibility means existing `huggingface-cli` workflows work unchanged. |
| **Game asset pipelines** | Deduplicate textures, meshes, and builds across versions. Content-addressed storage prevents asset corruption and enables safe caching. Range downloads stream large assets efficiently. |
| **Binary/executable distribution** | Upload versioned executables and libraries. Each build gets a unique content hash — clients download exactly the version they need without recompilation. Deduplication shares unchanged binaries between releases. |
| **Container images** | OCI Distribution frontend accepts `docker push` / `skopeo copy` directly. Deduplicate layers across images and tags. |
| **Build artifact caching** | Bazel HTTP remote cache protocol speeds up CI builds. Deduplicate unchanged compilation outputs across branches. |

## Quick start

```bash
# Run with Docker
docker compose -f docker-compose.yml up --build

# Or build from source
cargo build --release
./target/release/shardline serve
```

The server bootstraps `.shardline/` automatically on first run.

For providerless setup without starting the server:

```bash
shardline providerless setup
```

Mint repository-scoped tokens:

```bash
shardline admin token
```

## Deployment options

| Profile | Description |
| --- | --- |
| **Local** | Single-node with local filesystem storage — `docker compose up` |
| **Production small** | Single process with S3 + Postgres |
| **Production scaled** | Split `api` and `transfer` roles with shared storage |

All profiles run providerless by default.
Provider integration is optional.

## Documentation

| Guide | Description |
| --- | --- |
| [Deployment](docs/DEPLOYMENT.md) | Installation and configuration |
| [Authentication](docs/AUTHENTICATION.md) | Pluggable auth providers (HMAC, Ed25519, OIDC, JWKS, passthrough) |
| [HuggingFace Hub API](docs/HUGGINGFACE_HUB_API.md) | Hub API compatibility for huggingface-cli |
| [S3 Frontend](docs/S3_FRONTEND.md) | S3-compatible object API for lakehouse and s3:// clients |
| [Operations](docs/OPERATIONS.md) | Day-to-day operations runbook |
| [CLI Reference](docs/CLI.md) | All commands and flags |
| [Compatibility Status](docs/COMPATIBILITY_STATUS.md) | Surface maturity tiers and validated route coverage |
| [Architecture](docs/ARCHITECTURE.md) | System design and runtime shape |
| [Provider Setup](docs/PROVIDER_QUICKSTART.md) | GitHub/GitLab/Gitea/Codeberg integration |
| [Client Configuration](docs/CLIENT_CONFIGURATION.md) | Configure git, LFS, and Xet clients |
| [Protocols](docs/PROTOCOLS.md) | Supported protocol frontends |
| [Fsck](docs/FSCK.md) | Verify object-store and metadata integrity |
| [Garbage Collection](docs/GARBAGE_COLLECTION.md) | Reclaim unreachable objects safely |
| [Backup Manifest](docs/BACKUP.md) | Export recovery artifacts |
| [systemd](docs/SYSTEMD.md) | Linux service templates |
| [Kubernetes](docs/k8s/README.md) | Production Kubernetes manifests |

## License

Dual licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
