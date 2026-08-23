# Shardline

[![Status](https://img.shields.io/badge/status-stable-1f6feb)](docs/COMPATIBILITY_STATUS.md)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

**Shardline is a self-hostable content-addressed storage backend for anyone’s storage
needs, with content deduplication at its core and version-aware workflows where the
frontend supports them.**

It accepts immutable object uploads, deduplicates content, and serves range-aware
downloads. It is especially useful for large-file versioning when the selected frontend
keeps revisions or commits, because unchanged content can be reused instead of uploaded
repeatedly. Use the frontend that fits your workflow: Xet, OCI, Git LFS, S3, Hugging Face
Hub, cache clients, or the native API—or run it standalone for your own storage needs.
Pair it with GitHub, GitLab, or Gitea when you want repository-scoped storage.

**Use only the protocols you need.** Run Shardline as a focused Xet backend, OCI
registry, S3-compatible service, or Hugging Face backend—or compose multiple
frontends over the same hardened storage core.

## Surface Maturity

| Surface | Tier | Evidence |
| --- | --- | --- |
| Xet CAS frontend | **Stable** | Native Xet upload/download flows and checked-in `git-xet` push/clone/fetch/pull coverage |
| Git LFS frontend | **Beta** | LFS batch negotiation and direct object routes; checked-in `git-lfs` push/pull/fetch coverage |
| Bazel HTTP remote cache frontend | **Beta** | `ac`/`cas` read and write routes; `bazel`/`bazelisk` remote-cache flows in test matrix |
| OCI Distribution frontend | **Stable** | Blob/manifest/tag routes and checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Model/dataset create, upload, download, delete; `hf` CLI workflows in test matrix |
| S3 frontend | **Stable** | S3-compatible object API — Put/Get(+Range)/Head/Delete, conditional requests (If-Match/If-None-Match), CopyObject, multipart upload, ListObjectsV2 + ListObjects v1, DeleteObjects, ListBuckets, bucket stubs; standard MD5 ETags and user metadata (`x-amz-meta-*`); SigV4 access-key=token auth; validated against real clients (`mc`, AWS CLI, boto3, `s3cmd`, `rclone`, pyarrow 25) in the CI-run real-client e2e suite and security-audited (`feat/s3-frontend`) |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS/mTLS, bounded operation latency, corrupt-value recovery, and fenced cross-replica cold-load deduplication are validated |
| Provider integration (GitHub/GitLab/Gitea/Codeberg/generic) | **Beta** | Checked-in token issuance, webhook, and repository-scoped authorization coverage |
| Ed25519 auth provider | **Beta** | Signing/verification, bounded overlapping key rotation, CLI minting, verification-only mode, configuration, and authenticated HTTP flows have targeted tests |

## What it does

- **Store and deduplicate** any binary content — datasets, model weights, build
  artifacts, media
- **Composable protocol frontends** — enable only the protocols you need: Xet
  (default), Git LFS, Bazel HTTP remote cache, OCI Distribution, S3, and
  Hugging Face Hub
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

The recommended first run is the development Compose profile.
It provisions the local Postgres and MinIO dependencies, applies migrations, and waits
for them before starting Shardline:

```bash
docker compose -f docker-compose.yml up --build
```

See [Getting Started](docs/GETTING_STARTED.md) for readiness checks, token creation,
state cleanup, and the boundary between the development and production profiles.

For a host-native binary:

```bash
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
| **Production scaled** | Disposable `api` and `transfer` replicas with shared Postgres/S3; no RWX volume required |

All profiles run providerless by default.
Provider integration is optional.

## Documentation

| Guide | Description |
| --- | --- |
| [Deployment](docs/DEPLOYMENT.md) | Installation and configuration |
| [Getting Started](docs/GETTING_STARTED.md) | One deterministic local path from checkout to a running server |
| [Authentication](docs/AUTHENTICATION.md) | Pluggable auth providers (HMAC, Ed25519, OIDC, JWKS, passthrough) |
| [HuggingFace Hub API](docs/HUGGINGFACE_HUB_API.md) | Hub API compatibility for huggingface-cli |
| [S3 Frontend](docs/S3_FRONTEND.md) | S3-compatible object API for lakehouse and s3:// clients |
| [Operations](docs/OPERATIONS.md) | Day-to-day operations runbook |
| [CLI Reference](docs/CLI.md) | All commands and flags |
| [Compatibility Status](docs/COMPATIBILITY_STATUS.md) | Surface maturity tiers and validated route coverage |
| [Architecture](docs/ARCHITECTURE.md) | System design and runtime shape |
| [Distributed Correctness](docs/DISTRIBUTED_CORRECTNESS.md) | Multi-writer contracts, lock ordering, fencing epochs, and supported deployment boundaries |
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
