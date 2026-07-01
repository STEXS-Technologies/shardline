# Shardline

[![Status](https://img.shields.io/badge/status-stable%201.0.0-1f6feb)](docs/COMPATIBILITY_STATUS.md)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

**The open-source storage backend for large binary assets.**

Shardline is a self-hostable content-addressed storage (CAS) server. It accepts immutable object uploads, deduplicates content, and serves range-aware downloads. Run it standalone or pair it with GitHub, GitLab, or Gitea for repository-scoped storage.

The first open-source, production-ready, multi-protocol CAS server with a Xet frontend. Other Xet-compatible implementations exist as proof-of-concepts or single-protocol servers. Shardline combines Xet with Git LFS, Bazel HTTP remote cache, and OCI Distribution in a self-hostable, Kubernetes-ready deployment with full operational tooling.

## What it does

- **Store and deduplicate** any binary content — datasets, model weights, build artifacts, media
- **Multiple protocols** — Xet (default), Git LFS, Bazel HTTP remote cache, OCI Distribution
- **HuggingFace Hub API** — drop-in alternative for `huggingface-cli` uploads and downloads
- **Pluggable auth** — local Ed25519, OIDC, JWKS, or passthrough provider adapters
- **Self-hosted or cloud** — local filesystem, S3-compatible storage, Postgres metadata
- **Production-ready** — health checks, migrations, integrity verification, garbage collection, backups
- **Provider integration** — optional webhooks and token issuance for GitHub, GitLab, Gitea, Codeberg

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
|---------|-------------|
| **Local** | Single-node with local filesystem storage — `docker compose up` |
| **Production small** | Single process with S3 + Postgres |
| **Production scaled** | Split `api` and `transfer` roles with shared storage |

All profiles run providerless by default. Provider integration is optional.

## Documentation

| Guide | Description |
|-------|-------------|
| [Deployment](docs/DEPLOYMENT.md) | Installation and configuration |
| [Authentication](docs/AUTHENTICATION.md) | Pluggable auth providers (Ed25519, OIDC, JWKS, passthrough) |
| [HuggingFace Hub API](docs/HUGGINGFACE_HUB_API.md) | Hub API compatibility for huggingface-cli |
| [Operations](docs/OPERATIONS.md) | Day-to-day operations runbook |
| [CLI Reference](docs/CLI.md) | All commands and flags |
| [Provider Setup](docs/PROVIDER_QUICKSTART.md) | GitHub/GitLab/Gitea/Codeberg integration |
| [Client Configuration](docs/CLIENT_CONFIGURATION.md) | Configure git, LFS, and Xet clients |
| [Protocols](docs/PROTOCOLS.md) | Supported protocol frontends |
| [Kubernetes](docs/k8s/README.md) | Production Kubernetes manifests |

## License

Dual licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
