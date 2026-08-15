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

For deployments using a `shardline.toml` config file, mount it and use the `--config`
flag:

```bash
docker run -v /path/to/shardline.toml:/etc/shardline/shardline.toml:ro \
  registry.example.com/shardline:latest \
  --config /etc/shardline/shardline.toml serve
```

For Docker Compose, bind-mount the config file and add `--config` to the command in your
`docker-compose.yml`:

```yaml
services:
  shardline:
    image: registry.example.com/shardline:latest
    command: ["--config", "/etc/shardline/shardline.toml", "serve"]
    volumes:
      - ./shardline.toml:/etc/shardline/shardline.toml:ro
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
- hub: `/api/*`, `/{type}/{ns}/{repo}/info/refs`, `/{type}/{ns}/{repo}/HEAD`,
  `/{type}/{ns}/{repo}/git-upload-pack`, `/{type}/{ns}/{repo}/git-receive-pack`,
  `/objects/batch`, `/lfs/objects/{oid}`
- s3: `GET /` (ListBuckets), `/{bucket}` bucket stubs, `/{bucket}/{*key}` object
  data routes — served on the API role, so they also require path-based routing on
  the ingress when S3 is enabled

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

Configuration supports a TOML file with environment variable overrides.
Secrets and credentials belong in a `.env` file, not in the TOML config or in
environment variables.

### Configuration file (shardline.toml)

Server settings can be declared in a `shardline.toml` file.
The file is auto-detected from these locations (first found wins):

1. `./shardline.toml` (current directory)
2. `~/.config/shardline/shardline.toml` (user config)
3. `/etc/shardline/shardline.toml` (global config)

Use the `--config` flag to point at a specific path:

```bash
shardline serve --config /etc/shardline/shardline.toml
```

Values with `${VAR}` syntax are resolved from the process environment or from a `.env`
file loaded with `--env-file`:

```toml
[server]
bind_addr = "0.0.0.0:8080"
public_base_url = "https://cas.example.com"
server_role = "all"
frontends = ["xet", "oci", "hub"]
root_dir = "/var/lib/shardline"
chunk_size_bytes = 65536

[storage]
adapter = "s3"

[storage.s3]
endpoint = "https://s3.example.com"
region = "us-east-1"
bucket = "shardline-data"

[index]
postgres_url = "${DATABASE_URL}"

[cache]
adapter = "memory"
ttl_seconds = 30

[auth]
provider = "local"
provider_token_issuer = "shardline"
provider_token_ttl_seconds = 300
```

Place credentials in a separate `.env` file:

```bash
SHARDLINE_S3_ACCESS_KEY_ID=shardline
SHARDLINE_S3_SECRET_ACCESS_KEY=shardline-dev-password
DATABASE_URL=postgres://shardline:change-me@postgres:5432/shardline
SHARDLINE_TOKEN_SIGNING_KEY=change-me-for-local-only
```

Load both together:

```bash
shardline serve --env-file .env.production --config shardline.toml
```

Environment variables already set in the shell take precedence over values in
`shardline.toml`. The `.env` file is loaded before the TOML file is parsed, so `${VAR}`
references resolve correctly.

### Environment variables

Configuration through environment variables remains fully supported and always takes
precedence over TOML file values.

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
# Redis cache is opt-in: it requires your own Redis server. The shipped
# docker-compose profile uses the memory adapter and runs no Redis service.
# SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis
# SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://default:change-me@redis.example.com:6379
SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=64
SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=64
# S3 frontend upload limits (multipart sessions)
SHARDLINE_S3_MAX_PART_BYTES=1073741824       # 1 GiB; minimum accepted value 1 MiB
SHARDLINE_S3_MIN_PART_BYTES=5242880          # 5 MiB, S3's minimum non-final part size (checked at Complete only)
SHARDLINE_S3_UPLOAD_SESSION_MAX_BYTES=1099511627776   # 1 TiB per upload session
SHARDLINE_S3_UPLOAD_TOTAL_MAX_BYTES=4398046511104     # 4 TiB across active sessions
SHARDLINE_S3_UPLOAD_MAX_ACTIVE_PART_FILES=200000      # global cap on part files across active sessions
SHARDLINE_S3_UPLOAD_SESSION_TTL_SECONDS=3600          # 1 hour
SHARDLINE_S3_UPLOAD_MAX_ACTIVE_SESSIONS=1024
RUST_LOG=info
```

For a TLS-enabled Redis cache, use a `rediss://` URL. Private-CA and mTLS deployments
can supply PEM files with `SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CA_FILE`,
`SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE`, and
`SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_KEY_FILE`; see
[Cache adapters](CACHE_ADAPTERS.md#tls-and-mtls).

Set exactly one signing-key source for a server process that exposes CAS routes:
`SHARDLINE_TOKEN_SIGNING_KEY` for a direct environment value, or
`SHARDLINE_TOKEN_SIGNING_KEY_FILE` for a file or mounted secret.
If both are set, startup fails with a configuration error instead of guessing
precedence. CAS routes require bearer tokens with a valid Shardline signature, issuer,
repository scope, and the required read or write scope.
The stats endpoint follows the same rule and requires a valid bearer token.
`SHARDLINE_PROVIDER_CONFIG_FILE` and `SHARDLINE_PROVIDER_API_KEY_FILE` are optional.
Leave them unset for a providerless deployment that serves clients directly.

### Secret encryption at rest

`Strict` and `Authenticated` deployment modes require at-rest encryption keys for
every persistent-secret surface that is enabled:

- Hub webhook signing secrets — set `SHARDLINE_HUB_WEBHOOK_SECRET_KEY` (32-byte
  AES-256 key) when the hub frontend is enabled.
- Provider-config webhook secrets — set `SHARDLINE_CONFIG_SECRET_KEY` (32-byte
  AES-256 key) when a provider config or API key is configured.

Startup **fails** in these modes when a persistent-secret surface is present
without its key, instead of silently storing secrets in plaintext. Insecure mode
(local development) is exempt.

`SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION=true` is an explicit insecure
override that re-enables plaintext storage. It is intended **only** for
migrating an existing deployment to at-rest encryption and must not be used in
production.

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

- `api`: `/v1/reconstructions/*`, `/v1/providers/*`, `/v1/shards`, `/v1/stats`,
  plus the S3 frontend (`/`, `/{bucket}`, `/{bucket}/{*key}`) when enabled
- `transfer`: `/v1/chunks/*`, `/v1/xorbs/*`, `/transfer/xorb/*`
- `hub`: `/api/*`, `/{type}/{ns}/{repo}/info/refs`, `/{type}/{ns}/{repo}/HEAD`,
  `/{type}/{ns}/{repo}/git-upload-pack`, `/{type}/{ns}/{repo}/git-receive-pack`,
  `/objects/batch`, `/lfs/objects/{oid}`

## Hub API

The Hub API provides HuggingFace Hub compatibility.
Enable it by adding `hub` to the frontend set:

```text
SHARDLINE_SERVER_FRONTENDS=xet,hub
```

Or pass `--frontend hub` to `shardline serve`.

### REST Endpoints

Once enabled, these routes are available:

```text
GET    /health                                       — health check
GET    /api/whoami-v2                                — current user identity
GET    /api/{type}/{ns}/{repo}/xet-read-token/{rev}  — Xet read token exchange
GET    /api/{type}/{ns}/{repo}/xet-write-token/{rev} — Xet write token exchange
POST   /api/repos/create                             — create a repository
DELETE /api/repos/delete                             — delete a repository
GET    /api/repos                                    — list repositories
GET    /api/{type}/search                            — search repositories
POST   /api/{type}/{ns}/{repo}                       — create repository (typed)
GET    /api/{type}/{ns}/{repo}                       — get repository info
DELETE /api/{type}/{ns}/{repo}                       — delete repository
GET    /api/{type}/{ns}/{repo}/revisions             — list revisions
GET    /api/{type}/{ns}/{repo}/revision/{rev}        — revision metadata and siblings
GET    /api/{type}/{ns}/{repo}/modelcard             — fetch the model card
POST   /api/validate-yaml                            — validate YAML
POST   /api/{type}/{ns}/{repo}/preupload/{rev}       — pre-upload check
POST   /api/{type}/{ns}/{repo}/commit/{rev}          — commit file changes
GET    /api/{type}/{ns}/{repo}/tree/{rev}            — browse repository root
GET    /api/{type}/{ns}/{repo}/tree/{rev}/{*path}    — browse a file tree
GET    /{type}/{ns}/{repo}/resolve/{rev}/{*path}     — resolve and download a typed file
GET    /{ns}/{repo}/resolve/{rev}/{*path}            — resolve and download a model file
POST   /objects/batch                                — LFS batch request
PUT    /lfs/objects/{oid}                            — upload an LFS object
GET    /lfs/objects/{oid}                            — download an LFS object
GET    /api/datasets/{ns}/{repo}/parquet             — dataset parquet preview
GET    /api/datasets/{ns}/{repo}/first-rows          — dataset first rows
GET    /api/datasets/{ns}/{repo}/viewer/{split}      — dataset viewer
POST   /api/{type}/{ns}/{repo}/webhooks              — create a webhook
GET    /api/{type}/{ns}/{repo}/webhooks              — list webhooks
DELETE /api/{type}/{ns}/{repo}/webhooks/{id}         — delete a webhook
```

### Git Smart HTTP Endpoints

Direct `git clone` and `git push` via the Git Smart HTTP protocol:

```text
GET  /{type}/{ns}/{repo}/info/refs          — discovery (service=upload-pack|receive-pack)
GET  /{type}/{ns}/{repo}/HEAD               — HEAD reference
POST /{type}/{ns}/{repo}/git-upload-pack    — clone/fetch pack generation
POST /{type}/{ns}/{repo}/git-receive-pack   — push pack acceptance
```

Clone a Hub repository:

```bash
git clone http://127.0.0.1:8080/models/my-org/my-model
```

Push to a Hub repository:

```bash
cd my-model
git remote add hub http://127.0.0.1:8080/models/my-org/my-model
git push hub main
```

Pack files are generated from stored file entries.
LFS pointer blobs are created for files with LFS metadata.
The `.gitattributes` file is auto-generated when LFS files are present.

### Hub API Metadata Storage

Hub metadata (repos, revisions, file entries, LFS objects) is stored separately from the
main CAS index:

- **SQLite**: `{root_dir}/hub/` directory (default for local deployments)
- **Postgres**: same connection as the main index (`SHARDLINE_INDEX_POSTGRES_URL`)

For Postgres deployments, apply the Hub API migration:

```text
shardline db migrate up
```

The Hub API migration is migration 7 in the bundled set.

### Hub API with huggingface-cli

Point the CLI at your Shardline server:

```bash
export HF_ENDPOINT=http://127.0.0.1:8080
huggingface-cli upload my-org/my-model ./model-files
huggingface-cli download my-org/my-model
```

### Hub API Authentication

Hub API routes use the same `AuthProvider` trait as CAS routes.
When an auth provider is configured (Ed25519, OIDC, JWKS, or Local HMAC), Hub API bearer
tokens are validated against it.

When no auth provider is configured (providerless mode), Hub API routes accept all
requests anonymously.

## Authentication Providers

Shardline supports pluggable authentication via the `SHARDLINE_AUTH_PROVIDER` variable:

```text
SHARDLINE_AUTH_PROVIDER=local          # HMAC-SHA256 signing key (default)
SHARDLINE_AUTH_PROVIDER=ed25519        # Ed25519 signing or verification
SHARDLINE_AUTH_PROVIDER=oidc           # OpenID Connect
SHARDLINE_AUTH_PROVIDER=jwks           # JSON Web Key Set
SHARDLINE_AUTH_PROVIDER=passthrough    # Trust upstream proxy
```

### Local (HMAC)

Default for providerless deployments.
Tokens are signed with a local HMAC-SHA256 signing key.

```text
SHARDLINE_TOKEN_SIGNING_KEY=change-me-for-local-only
# or
SHARDLINE_TOKEN_SIGNING_KEY_FILE=/run/secrets/shardline-token-key
```

### Ed25519

Use a private key for signing and verification:

```text
SHARDLINE_AUTH_PROVIDER=ed25519
SHARDLINE_ED25519_PRIVATE_KEY_FILE=/run/secrets/shardline-ed25519-private-key
```

For verification-only operation, use
`SHARDLINE_ED25519_PUBLIC_KEY_FILE=/run/secrets/shardline-ed25519-public-key` instead.
TOML deployments can set `auth.provider = "ed25519"` and `auth.ed25519.private_key_path`
or `auth.ed25519.public_key_path`. See [Authentication](AUTHENTICATION.md#ed25519) for
supported key formats, token-format limitations, and minting behavior.

### OIDC

Validate tokens against an OpenID Connect issuer:

```text
SHARDLINE_AUTH_PROVIDER=oidc
SHARDLINE_AUTH_OIDC_ISSUER=https://accounts.google.com
```

### JWKS

Validate tokens against a JSON Web Key Set endpoint:

```text
SHARDLINE_AUTH_PROVIDER=jwks
SHARDLINE_AUTH_JWKS_URL=https://example.com/.well-known/jwks.json
```

### Passthrough

Trust an upstream reverse proxy that handles authentication.
The server reads the `Authorization` header directly without validation:

```text
SHARDLINE_AUTH_PROVIDER=passthrough
```

Use this behind a trusted proxy (e.g., Cloudflare Access, oauth2-proxy).

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

### Rolling Upgrades

To upgrade a running deployment without a full outage, upgrade one role class at a
time — `api` and `transfer` separately in the scaled profile, one process for
`--role all` — and poll `/healthz` and `/readyz` after each class. The readiness
check reports the server role and configured backends, so each class can be confirmed
ready before the next one moves. See [Rolling Upgrade](ROLLING_UPGRADE.md) for the
full procedure, recommended order, and rollback steps.

## Metrics

`GET /metrics` emits Prometheus text format.
When `SHARDLINE_METRICS_TOKEN_FILE` is set, the endpoint rejects unauthenticated scrape
requests and requires `Authorization: Bearer <metrics-token>`.

50+ metrics across 9 categories:

- **Storage**: objects stored/retrieved, bytes transferred, dedup hits/misses
- **Transfer**: upload/download counts, bytes, chunks processed, range failures
- **Xet**: xorbs/shards processed, reconstruction terms, validation failures
- **Protocol**: LFS, Bazel, OCI request counts and latencies
- **Reconstruction**: lookups, cache hits/misses, failures
- **GC/FSCK**: runs, duration, chunks quarantined/swept, orphans found
- **Backend**: local/S3 operation counts and latency histograms
- **Provider**: token issuance, webhook events processed
- **System**: active connections, memory usage, cache eviction counts

Metric labels must be bounded.
Do not use raw hashes, tokens, user IDs, repository names, or object keys as labels.

## Backup and Restore

Production backup requires both stores:

- index database backup
- object storage retention or backup

For scenario-by-scenario recovery runbooks (node loss, metadata loss, object-store
loss, crash mid-upload, cross-node moves), see
[Disaster Recovery](DISASTER_RECOVERY.md).

The index alone is not enough to restore data.
Object bytes alone are not enough to serve reconstructions efficiently without
rebuilding metadata.

Restore tooling includes:

```text
shardline fsck
shardline gc
shardline gc --mark
shardline gc --sweep
shardline gc --mark --sweep --retention-seconds 3600
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
