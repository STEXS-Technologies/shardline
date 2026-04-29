# Deployment

Shardline runs as a single Docker container by default, with external object storage and
index storage selected by configuration.

For production operating guidance after deployment, including backup, restore,
high-availability layout, GC suspension during incident response, and operator recovery,
see [Operations](OPERATIONS.md).
For single-node Linux service templates, see [systemd](SYSTEMD.md).

## Runtime Shape

`shardline serve` accepts an explicit frontend set through repeated `--frontend` flags
or `SHARDLINE_SERVER_FRONTENDS=xet,...`. The default frontend is `xet`. `--role api` and
`--role transfer` only split that frontend set across processes for scaling; they do not
choose different protocols.

## Deployment Profiles

### Local Single-Node

Use this profile for development and small private installs.

```mermaid
flowchart TD
  subgraph Canvas[ ]
    direction TD
    Server[server: shardline]
    Server --> Object[object store: local filesystem]
    Server --> Metadata[index + record storage: local SQLite metadata]
    Server --> Transfer[transfer: server-mediated]
  end

  style Canvas fill:#f8f4ec,stroke:#d7c9b2,color:#1f2937;
  classDef server fill:#f6efe8,stroke:#c7b8a3,color:#1f2937;
  classDef storage fill:#dff3e4,stroke:#90c6a0,color:#1f2937;
  classDef data fill:#efe3f8,stroke:#b89bd6,color:#1f2937;
  classDef flow fill:#dcecf8,stroke:#8db7d8,color:#1f2937;
  class Server server;
  class Object storage;
  class Metadata data;
  class Transfer flow;
  linkStyle default stroke:#111827,stroke-width:1.5px;
```

The bundled compose file starts the local profile by default:

```text
docker compose -f docker-compose.yml up --build
```

That compose profile is Docker-native by default: it generates and persists its
development signing key inside the named data volume.
To mint an authenticated local token against that running container, execute the CLI
inside the service:

```bash
TOKEN="$(docker compose -f docker-compose.yml exec -T shardline \
  shardline admin token \
  --issuer local \
  --subject operator-1 \
  --scope write \
  --provider generic \
  --owner team \
  --repo assets \
  --revision main \
  --key-file /var/lib/shardline/secrets/token-signing-key)"

curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:18080/v1/stats
```

If you want to mint tokens on the host for the Compose server, pass the same signing key
as an environment variable:

```bash
SHARDLINE_TOKEN_SIGNING_KEY=dev-signing-key docker compose -f docker-compose.yml up --build

TOKEN="$(SHARDLINE_TOKEN_SIGNING_KEY=dev-signing-key shardline admin token \
  --issuer local \
  --subject operator-1 \
  --scope write \
  --provider generic \
  --owner team \
  --repo assets \
  --revision main \
  --key-env SHARDLINE_TOKEN_SIGNING_KEY)"

curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:18080/v1/stats
```

For a host-native server process, run `shardline serve` directly and provide the runtime
configuration through normal environment variables or files.
Provider integration is independent of whether the process runs on the host or in a
container.

For local S3-compatible testing, start the MinIO profile and point Shardline at the
bucket it creates:

```text
docker compose -f docker-compose.yml --profile s3 up minio minio-init
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_S3_BUCKET=shardline
SHARDLINE_S3_REGION=us-east-1
SHARDLINE_S3_ENDPOINT=http://127.0.0.1:19000
SHARDLINE_S3_ACCESS_KEY_ID=shardline
SHARDLINE_S3_SECRET_ACCESS_KEY=shardline-dev-password
SHARDLINE_S3_ALLOW_HTTP=true
```

For a direct, providerless Xet-compatible backend, follow
[Providerless Direct Xet Backend](#providerless-direct-xet-backend).

### Production Small

Use this profile for self-hosted teams.

```mermaid
flowchart TD
  subgraph Canvas[ ]
    direction TD
    Server[server: shardline]
    Server --> Object[object store: S3-compatible]
    Server --> Index[index store: Postgres-compatible SQL]
    Server --> Transfer[transfer: presigned object-store URLs when supported]
  end

  style Canvas fill:#f8f4ec,stroke:#d7c9b2,color:#1f2937;
  classDef server fill:#f6efe8,stroke:#c7b8a3,color:#1f2937;
  classDef storage fill:#dff3e4,stroke:#90c6a0,color:#1f2937;
  classDef data fill:#efe3f8,stroke:#b89bd6,color:#1f2937;
  classDef flow fill:#dcecf8,stroke:#8db7d8,color:#1f2937;
  class Server server;
  class Object storage;
  class Index data;
  class Transfer flow;
  linkStyle default stroke:#111827,stroke-width:1.5px;
```

### Production Scaled

Use this profile when API traffic and transfer traffic need independent scaling.

```mermaid
flowchart TD
  subgraph Canvas[ ]
    direction TD
    Api[api servers: shardline serve --role api]
    Transfer[transfer servers: shardline serve --role transfer]
    Shared[shared backend services<br/>object store: S3-compatible or custom adapter<br/>index store: managed Postgres-compatible SQL]
    Api --> Shared
    Transfer --> Shared
  end

  style Canvas fill:#f8f4ec,stroke:#d7c9b2,color:#1f2937;
  classDef role fill:#f6efe8,stroke:#c7b8a3,color:#1f2937;
  classDef shared fill:#dff3e4,stroke:#90c6a0,color:#1f2937;
  class Api,Transfer role;
  class Shared shared;
  linkStyle default stroke:#111827,stroke-width:1.5px;
```

The scaled role split lets API traffic and transfer traffic grow independently while
keeping the same protocol handlers.

Shardline now ships a production-scaled Kubernetes package under
`docs/k8s/production-scaled/`. It includes:

- separate API and transfer `Deployment` objects
- separate API and transfer `Service` objects
- separate API and transfer `HorizontalPodAutoscaler` objects
- separate API and transfer `PodDisruptionBudget` objects
- a dedicated garbage-collection `CronJob`
- a concrete ingress example for path-based API versus transfer routing

Those manifests target the durable production profile:

```text
object store: S3-compatible
index and record storage: Postgres-compatible SQL
reconstruction cache: Redis-compatible
state root: ephemeral pod-local runtime state
```

Use them with:

```text
kubectl apply -f docs/k8s/production-scaled/runtime-secret.template.yaml
kubectl apply -f docs/k8s/production-scaled/provider-catalog-secret.template.yaml
kubectl apply -k docs/k8s/production-scaled
```

The Kubernetes package assumes the same public hostname for both roles and requires
path-aware routing in front of the Services.
The included ingress maps these route families:

- API: `/healthz`, `/v1/providers`, `/api`, `/reconstructions`, `/v1/reconstructions`,
  `/v2/reconstructions`, `/shards`, `/v1/shards`, `/v1/stats`
- transfer: `/v1/chunks`, `/v1/xorbs`, `/transfer/xorb`

Provider integration remains optional in this profile.
A scaled deployment can run as a direct Xet-compatible backend with only the CAS and
operator routes enabled.

The server also exposes `/metrics` in Prometheus text format.
The production ingress example keeps that route internal; scrape it from the API Service
or an internal-only monitoring entrypoint.
Set `SHARDLINE_METRICS_TOKEN_FILE` in production so the route requires
`Authorization: Bearer <metrics-token>`. `/readyz` is for Kubernetes probes and
service-local diagnostics, not public ingress.

## Container Requirements

The container image:
- run as a non-root user
- include only the `shardline` binary and required runtime assets
- expose one HTTP port
- support graceful shutdown
- write logs to stdout/stderr
- avoid shell scripts as the primary entrypoint
- include a health endpoint

Suggested commands:

```text
shardline serve
shardline config check
shardline db migrate up
shardline fsck
shardline index rebuild
shardline hold list --active-only
```

From a source checkout without a globally installed binary, run the same commands
through Cargo:

```text
cargo run -p shardline --bin shardline -- health --server http://127.0.0.1:18080
```

## Configuration

Configuration supports a file plus environment overrides.

Initial environment variables:

```text
SHARDLINE_BIND_ADDR=0.0.0.0:8080
SHARDLINE_SERVER_ROLE=all
SHARDLINE_PUBLIC_BASE_URL=https://cas.example.com
SHARDLINE_OBJECT_STORAGE_ADAPTER=local
SHARDLINE_INDEX_POSTGRES_URL=postgres://shardline:change-me@postgres:5432/shardline
SHARDLINE_MAX_REQUEST_BODY_BYTES=67108864
SHARDLINE_MAX_SHARD_FILES=16384
SHARDLINE_MAX_SHARD_XORBS=16384
SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS=65536
SHARDLINE_MAX_SHARD_XORB_CHUNKS=65536
SHARDLINE_TOKEN_SIGNING_KEY_FILE=/run/secrets/shardline-token-key
SHARDLINE_METRICS_TOKEN_FILE=/run/secrets/shardline-metrics-token
SHARDLINE_PROVIDER_CONFIG_FILE=/etc/shardline/providers.json
SHARDLINE_PROVIDER_API_KEY_FILE=/run/secrets/shardline-provider-key
SHARDLINE_PROVIDER_TOKEN_ISSUER=shardline-provider
SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS=300
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory
SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=30
SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=4096
SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://default:dev_password@garnet:6379
SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=64
SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=64
SHARDLINE_LOG_LEVEL=info
```

Set exactly one signing-key source for a server process that exposes CAS routes:
`SHARDLINE_TOKEN_SIGNING_KEY` for a direct environment value, or
`SHARDLINE_TOKEN_SIGNING_KEY_FILE` for a file or mounted secret.
If both are set, startup fails with a configuration error instead of guessing
precedence. CAS routes require bearer tokens with a valid Shardline signature, issuer,
repository scope, and the required read or write scope.
The stats endpoint follows the same rule and requires a valid bearer token.
`SHARDLINE_PROVIDER_CONFIG_FILE` and `SHARDLINE_PROVIDER_API_KEY_FILE` are optional.
Leave them unset for a providerless deployment that serves clients directly.

### Providerless Direct Xet Backend

Use this mode when Shardline is the only backend you need and no provider bridge is
minting tokens for clients.

Required configuration:

```text
SHARDLINE_BIND_ADDR=0.0.0.0:8080
SHARDLINE_PUBLIC_BASE_URL=https://cas.example.com
SHARDLINE_TOKEN_SIGNING_KEY=change-me-for-local-only
SHARDLINE_OBJECT_STORAGE_ADAPTER=local
```

For production, prefer
`SHARDLINE_TOKEN_SIGNING_KEY_FILE=/run/secrets/shardline-token-key` or a platform secret
mount instead of putting the signing key directly in the process environment.

For local single-node, keep the default SQLite metadata store under
`.shardline/data/metadata.sqlite3`. For durable production installs, switch to:

```text
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_INDEX_POSTGRES_URL=postgres://user:password@db.example.com:5432/shardline
```

Do not set:

```text
SHARDLINE_PROVIDER_CONFIG_FILE
SHARDLINE_PROVIDER_API_KEY_FILE
```

Validated local source-checkout quickstart:

```bash
shardline serve
```

`shardline serve` bootstraps missing `.shardline/` providerless state automatically when
you run it from a fresh source checkout.
If you want to materialize the local state without starting the server, run:

```bash
shardline providerless setup
```

In another shell, mint a repository-scoped token and verify the authenticated stats
route:

```bash
TOKEN="$(shardline admin token \
  --issuer local \
  --subject operator-1 \
  --scope write \
  --provider generic \
  --owner team \
  --repo assets \
  --revision main \
  --key-file .shardline/token-signing-key)"

curl -H "Authorization: Bearer ${TOKEN}" http://127.0.0.1:8080/v1/stats
```

Validate the bootstrapped local profile:

```bash
shardline config check
```

The providerless bootstrap writes:

- `.shardline/token-signing-key`
- `.shardline/providerless.env`
- `.shardline/data/`

The validated response shape for a clean local deployment is:

```json
{"chunks":0,"chunk_bytes":0,"files":0}
```

The same route returns `401` without a bearer token.

For plain `docker run`, pass the signing key directly with the environment like any
other runtime setting:

```bash
docker run --rm -it \
  -p 8080:8080 \
  -e SHARDLINE_BIND_ADDR=0.0.0.0:8080 \
  -e SHARDLINE_SERVER_ROLE=all \
  -e SHARDLINE_PUBLIC_BASE_URL=http://127.0.0.1:8080 \
  -e SHARDLINE_ROOT_DIR=/data/shardline \
  -e SHARDLINE_OBJECT_STORAGE_ADAPTER=local \
  -e SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory \
  -e SHARDLINE_TOKEN_SIGNING_KEY=dev-signing-key \
  -v shardline-data:/data/shardline \
  shardline:local serve
```

If you prefer a file or mounted secret, set `SHARDLINE_TOKEN_SIGNING_KEY_FILE` instead.
Do not set both signing-key variables; the server rejects ambiguous signing-key
configuration.

If port `8080` is already in use on the host, change the host-side mapping and update
`SHARDLINE_PUBLIC_BASE_URL` to match.

In providerless mode:

- clients talk directly to Shardline CAS routes
- scoped bearer tokens come from `shardline admin token` or your own trusted signing
  workflow
- provider token issuance and webhook ingestion routes remain disabled
- provider repository state tables stay empty unless you later enable provider
  integration

Operator commands resolve the deployment root in this order:

1. explicit `--root <path>`
2. `SHARDLINE_ROOT_DIR`
3. nearest parent containing `.shardline`, using `.shardline/data`
4. current directory when it already contains a Shardline data layout
5. current working directory plus `.shardline/data`

For local use, run commands from the project or deployment directory and Shardline will
use that directory's `.shardline/data` state automatically.
`SHARDLINE_ROOT_DIR` is for service deployments that need an explicit mounted state
path.

Postgres-compatible metadata deployments should apply the bundled schema before starting
traffic-serving pods:

```text
shardline db migrate up
```

The command uses `SHARDLINE_INDEX_POSTGRES_URL` by default and manages the bundled
Shardline migration set in the correct order.
For details and rollback behavior, see [Database Migrations](DATABASE_MIGRATIONS.md).

When `SHARDLINE_PROVIDER_CONFIG_FILE` and `SHARDLINE_PROVIDER_API_KEY_FILE` are set,
Shardline exposes a provider-facing issuance endpoint for trusted connectors:

```text
POST /v1/providers/github/tokens
X-Shardline-Provider-Key: <bootstrap-key>
{
  "subject": "github-user-1",
  "owner": "team",
  "repo": "assets",
  "revision": null,
  "scope": "Write"
}
```

The provider catalog file is JSON. Initial shape:

```json
{
  "providers": [
    {
      "kind": "github",
      "integration_subject": "github-app",
      "webhook_secret": "replace-me",
      "repositories": [
        {
          "owner": "team",
          "name": "assets",
          "visibility": "private",
          "default_revision": "main",
          "clone_url": "https://github.example/team/assets.git",
          "read_subjects": ["github-user-1"],
          "write_subjects": ["github-user-1"]
        }
      ]
    }
  ]
}
```

The bootstrap key is not a user token.
It is for trusted provider-side services that need to exchange provider authorization
results for scoped CAS tokens.
Each configured provider entry must include a non-empty `webhook_secret`. Shardline
rejects provider catalogs that omit webhook authentication.

With the same provider catalog enabled, Shardline also exposes a provider webhook
ingestion endpoint:

```text
POST /v1/providers/github/webhooks
X-GitHub-Event: repository
X-GitHub-Delivery: <delivery-id>
X-Hub-Signature-256: sha256=<hmac>
```

Repository deletion webhooks create time-bounded retention holds for the affected chunk
objects. That gives operators a recovery window before a later garbage-collection sweep
can reclaim storage.

S3-compatible object storage:

```text
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_S3_BUCKET=asset-cas
SHARDLINE_S3_REGION=us-east-1
SHARDLINE_S3_ENDPOINT=https://s3.example.com
SHARDLINE_S3_ACCESS_KEY_ID=<access-key>
SHARDLINE_S3_SECRET_ACCESS_KEY=<secret-key>
SHARDLINE_S3_SESSION_TOKEN=<optional-session-token>
SHARDLINE_S3_ACCESS_KEY_ID_FILE=/run/secrets/shardline/s3-access-key-id
SHARDLINE_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/shardline/s3-secret-access-key
SHARDLINE_S3_SESSION_TOKEN_FILE=/run/secrets/shardline/s3-session-token
SHARDLINE_S3_KEY_PREFIX=<optional-prefix>
SHARDLINE_S3_ALLOW_HTTP=false
SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=false
```

Use either direct S3 credential variables or the matching `_FILE` variables, not both.
Production deployments should prefer `_FILE` variables with regular non-symlink secret
files mounted owner/group-readable only, for example Kubernetes `defaultMode: 0440` with
the Shardline `fsGroup`, or `0640`/`0600` on a systemd host.

Postgres-compatible index storage:

```text
SHARDLINE_INDEX_POSTGRES_URL=postgres://user:password@db.example.com:5432/shardline
```

Secrets must not be printed by `config check`.

`SHARDLINE_MAX_REQUEST_BODY_BYTES` caps the maximum accepted HTTP request body size.
Transfer routes for xorb and shard uploads enforce this limit before Xet normalization
and validation. Upload bodies are not staged to local `incoming` files; immutable bytes
are persisted through the configured object-storage adapter.
S3 deployments therefore do not require local disk for upload-body persistence.
Ranged xorb and shard downloads stream stored object bytes through the response path.
Control-plane JSON and webhook routes still buffer bounded request bodies before
parsing. The default is `67108864` bytes.
Raise it to match expected asset sizes and keep the transfer role behind an upstream
limit that is at least as strict.

Shard metadata limits cap per-upload metadata fanout, not logical asset size.
They prevent a shard body from forcing unbounded file sections, xorb sections,
reconstruction terms, or xorb chunk records before the server can reject it.
Large-media and scientific deployments can raise
`SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS` and `SHARDLINE_MAX_SHARD_XORB_CHUNKS`
together with the request-body cap when their Xet client emits very large shard metadata
records. These limits should be sized from the expected shard metadata shape and
available RAM, not from the raw file byte size.

`SHARDLINE_SERVER_ROLE` selects which route family the current process exposes.
Use `all` for the default single-process deployment, `api` for control-plane nodes, and
`transfer` for large upload/download nodes.

`SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS` caps the per-upload chunk processing window.
Complete aligned chunks can be hashed and persisted concurrently, but the server keeps
the window bounded so a large file cannot create unbounded tasks or retained request
frames. Raise this value only after measuring storage-adapter latency, CPU saturation,
and memory pressure under the expected concurrent upload count.

`SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS` caps concurrent transfer work using a
chunk-equivalent budget.
A chunk read or small file download consumes one permit.
Larger response frames consume more permits, up to the configured budget.
The limiter is applied per streamed response frame, so one large download can use the
lane while it is actively sending a frame without monopolizing capacity for the full
lifetime of the response.

`SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER` selects the reconstruction-cache backend.
Use `memory` for the default bounded process-local cache, `redis` for a shared cache, or
`disabled` to skip caching.
Transfer-only nodes do not use the reconstruction cache and report the effective cache
backend as `disabled` in readiness and config checks.

In a split deployment behind a reverse proxy, route ownership is:

- `api`: `/v1/reconstructions/*`, `/v1/providers/*`, `/v1/shards`, `/v1/stats`
- `transfer`: `/v1/chunks/*`, `/v1/xorbs/*`, `/transfer/xorb/*`

## Health Checks

The server exposes:

- liveness: process is running and can respond
- readiness: index store and required object store operations are available
- version: build version, protocol compatibility version, enabled adapters

Current probe endpoints:

- `GET /healthz`
- `GET /readyz`

The published container image also includes a built-in Docker `HEALTHCHECK` that runs:

```text
shardline health --server http://127.0.0.1:8080
```

Health endpoints must not expose secrets, token claims, presigned URLs, or object keys
for private data.

## Metrics

`GET /metrics` emits Prometheus text format.
When `SHARDLINE_METRICS_TOKEN_FILE` is set, the endpoint rejects unauthenticated scrape
requests and requires `Authorization: Bearer <metrics-token>`.

Initial metrics:

- request count by route, method, status
- request duration
- upload bytes accepted
- upload bytes stored
- download bytes served
- logical bytes registered
- dedupe hits and misses
- xorb validation failures
- shard validation failures
- reconstruction lookup failures
- transfer range failures
- object-store latency
- index-store latency

Metric labels must be bounded.
Do not use raw hashes, tokens, user IDs, repository names, or object keys as labels.

## Backup and Restore

Production backup requires both stores:

- index database backup
- object storage retention or backup

The index alone is not enough to restore data.
Object bytes alone are not enough to serve reconstructions efficiently without
rebuilding metadata.

Restore tooling includes:

```text
shardline fsck
shardline gc
shardline gc --mark
shardline gc --sweep
shardline gc --mark --sweep --retention-seconds 0
shardline gc --mark --retention-report reports/gc-retention.json --orphan-inventory reports/gc-orphans.json
shardline index rebuild
```

For `gc`, `fsck`, `repair lifecycle`, and `hold`, `--root` is only an override for the
Shardline state root, not a switch that forces local object storage.
When `SHARDLINE_INDEX_POSTGRES_URL` is set, those commands read lifecycle and record
metadata from Postgres.
When the S3 adapter is configured, they inventory and delete payload objects through S3.

Current local rebuild mode is:

```text
shardline index rebuild
```

Local rebuild requires retained immutable version rows in
`.shardline/data/metadata.sqlite3`.
