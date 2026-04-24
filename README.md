# Shardline

[![Rust](https://img.shields.io/badge/rust-stable-orange?logo=rust)](../../rust-toolchain.toml)
[![Deployment](https://img.shields.io/badge/deployment-docker%20%7C%20kubernetes-blue)](docs/DEPLOYMENT.md)
[![Status](https://img.shields.io/badge/status-stable%201.0.0-1f6feb)](docs/COMPATIBILITY_STATUS.md)
[![Platform](https://img.shields.io/badge/platform-unix%20hardened%20%7C%20windows%20compile-0a7ea4)](docs/COMPATIBILITY_STATUS.md)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

Shardline is an open, self-hostable content-addressed storage backend with
pluggable protocol frontends.

It accepts immutable object uploads, verifies protocol objects, plans
reconstructions, and serves range-aware downloads. You can run it directly as a
providerless CAS backend or pair it with GitHub, GitLab, Gitea, or a generic Git
provider without baking provider-specific behavior into the CAS core.

Validated frontends in this repository today are Xet, Git LFS, Bazel HTTP remote
cache, and OCI Distribution.
`shardline serve` now accepts an explicit frontend set through `--frontend` or
`SHARDLINE_SERVER_FRONTENDS`, with `xet` enabled by default.
The core storage, indexing, and reconstruction boundaries stay separate from
frontend-specific protocol handling.

For small deployments, `shardline serve` runs the control plane and transfer plane in
one process. Larger deployments can split the same binary into `api` and `transfer`
roles. `--role` only changes deployment topology; the enabled frontend set controls the
protocol surface.

## Why Shardline

- self-hostable CAS backend with explicit protocol frontends
- production-oriented operator surface: health checks, migrations, fsck, repair, backup,
  storage migration, retention holds, and garbage collection
- storage and metadata adapters kept behind explicit boundaries
- provider integration kept outside the CAS core
- security posture centered on hostile-input handling, bounded work, and fail-closed
  local filesystem behavior

## Getting Started

Shardline is not a one-command quick-start project. Even the local profile still needs
storage, metadata, and token-signing configured correctly.

Start here:

- [Deployment](docs/DEPLOYMENT.md)
- [Operations](docs/OPERATIONS.md)
- [CLI](docs/CLI.md)
- [Database Migrations](docs/DATABASE_MIGRATIONS.md)

For the current providerless Xet-compatible backend:

- deploy the local SQLite + filesystem profile or Postgres + S3 profile
- from a source checkout, run `shardline serve`; it bootstraps `.shardline/`
  automatically
- if you want bootstrap without starting the server, run `shardline providerless setup`
- mint repository-scoped bearer tokens with `shardline admin token`
- point clients directly at the Shardline base URL

For provider-aware setup, token issuance, and stock `git` + `git-lfs` + `git-xet`
workflows, continue with:

- [Provider Setup Guide](docs/PROVIDER_QUICKSTART.md)
- [Client Configuration](docs/CLIENT_CONFIGURATION.md)
- [Repository Bootstrap](docs/REPOSITORY_BOOTSTRAP.md)

## Architecture

```mermaid
flowchart TD
  subgraph Canvas[ ]
    direction TD
    Client[Client]
    Provider[Optional Git provider]
    Router[Frontend router]
    Frontends["<b>Frontend set</b><br/>Xet<br/>Git LFS<br/>Bazel HTTP cache<br/>OCI Distribution"]
    Core["<b>Shared server core</b><br/>Auth and scope checks<br/>CAS coordinator<br/>Reconstruction planner<br/>GC and operator flows"]
    Adapters["<b>Adapters</b><br/>Index and record store<br/>Object store<br/>Reconstruction cache<br/>VCS and provider adapters"]
  end

  Client --> Router
  Provider --> Core
  Router --> Frontends
  Frontends --> Core
  Core --> Adapters

  classDef neutral fill:#f6efe8,stroke:#c7b8a3,color:#1f2937;
  classDef frontend fill:#dcecf8,stroke:#8db7d8,color:#1f2937;
  classDef core fill:#dff3e4,stroke:#90c6a0,color:#1f2937;
  classDef adapter fill:#efe3f8,stroke:#b89bd6,color:#1f2937;
  style Canvas fill:#f8f4ec,stroke:#d7c9b2,color:#1f2937;
  class Client,Provider neutral;
  class Router,Frontends frontend;
  class Core core;
  class Adapters adapter;
  linkStyle default stroke:#111827,stroke-width:1.5px;
```

- The router enables one or more frontends.
- Optional Git providers interact through the server/core path for token issuance and webhooks.
- The shared core stays protocol-neutral where possible.
- Storage, metadata, cache, and provider logic stay behind adapter boundaries.

## Deployment Profiles

```mermaid
flowchart TD
  subgraph Canvas[ ]
    direction TD
    Profiles[Deployment profiles]
    Profiles --> Local[Local single-node]
    Profiles --> Small[Production small]
    Profiles --> Scaled[Production scaled]

    Local --> LocalStore[Local object storage and local metadata]
    Small --> SmallStore[S3-compatible objects and Postgres metadata]
    Scaled --> Split[Separate api and transfer deployments]
    Split --> Shared[(Shared S3, Postgres, Redis cache)]
  end

  style Canvas fill:#f8f4ec,stroke:#d7c9b2,color:#1f2937;
  classDef root fill:#f6efe8,stroke:#c7b8a3,color:#1f2937;
  classDef profile fill:#dcecf8,stroke:#8db7d8,color:#1f2937;
  classDef target fill:#dff3e4,stroke:#90c6a0,color:#1f2937;
  class Profiles root;
  class Local,Small,Scaled,Split profile;
  class LocalStore,SmallStore,Shared target;
  linkStyle default stroke:#111827,stroke-width:1.5px;
```

- Local single-node: `docker compose -f docker-compose.yml up --build`
  By default, Compose keeps a development signing key in the container volume. If you
  want host-minted tokens, pass the same key with `SHARDLINE_TOKEN_SIGNING_KEY=...`
  and mint with `shardline admin token --key-env SHARDLINE_TOKEN_SIGNING_KEY`.
- Production small: one `shardline serve` process with durable object and metadata
  stores
- Production scaled: split `shardline serve --role api` and
  `shardline serve --role transfer`

All three profiles can run providerless.
Provider integration is optional and only needed when a forge or bridge service must
mint scoped CAS tokens on behalf of users.
The exact validated local providerless Xet steps are in
[Providerless Direct Xet Backend](docs/DEPLOYMENT.md#providerless-direct-xet-backend).

Start with [Deployment](docs/DEPLOYMENT.md), then use
[Shardline Kubernetes](docs/k8s/README.md) for the production-scaled manifest set.

## Production Readiness

Shardline is released as `1.0.0` for the protocol and operator surface documented in
this repo. Before a production rollout, read:

- [Deployment](docs/DEPLOYMENT.md)
- [Operations](docs/OPERATIONS.md)
- [Compatibility Status](docs/COMPATIBILITY_STATUS.md)
- [Security and Invariants](docs/SECURITY_AND_INVARIANTS.md)

## Crate Map

| Crate | Purpose |
| --- | --- |
| `protocol` | Shared protocol-facing types, hash and byte-range parsing, scoped token types, and small security/time/text helpers |
| `cache` | Reconstruction-cache traits and adapters |
| `storage` | Immutable object-storage contracts and adapters |
| `index` | Reconstruction and deduplication metadata contracts and adapters |
| `cas` | Protocol-neutral CAS coordinator domain and composition |
| `vcs` | Provider adapters and authorization boundaries |
| `server` | HTTP routes, runtime wiring, migrations, fsck, GC, repair, and rollout logic |
| `cli` | `shardline` operator binary |

## Documentation

- [Docs Index](docs/README.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Operations](docs/OPERATIONS.md)
- [Provider Setup Guide](docs/PROVIDER_QUICKSTART.md)
- [Client Configuration](docs/CLIENT_CONFIGURATION.md)
- [Contributing](CONTRIBUTING.md)
- [CLI](docs/CLI.md)
- [Protocol Frontends](docs/PROTOCOLS.md)
- [Protocol Conformance](docs/PROTOCOL_CONFORMANCE.md)
- [Compatibility Status](docs/COMPATIBILITY_STATUS.md)
- [Performance](docs/PERFORMANCE.md)

## License

Shardline is dual licensed under either of these, at your option:

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)
