# Shardline

## A content-addressed storage server for large, changing files

Shardline stores each piece of data once and represents new versions as small
metadata changes. That makes it a practical backend for model checkpoints,
datasets, container layers, game assets, build outputs, and CI caches that are
large, frequently revised, and expensive to copy repeatedly.

Shardline runs on infrastructure you control. It exposes the protocols your
tools already understand, keeps metadata durable, and streams data through the
entire request path so file size does not become server memory usage.

## Why teams use it

- **Deduplication across versions**. Shared chunks are stored once, even when
  many revisions contain the same content.
- **Streaming by default**. Uploads, downloads, previews, range reads, and
  processing use bounded memory. Large files are never treated as one giant
  in-memory value.
- **Resumable transfers**. Interrupted Git LFS, OCI, S3, Xet, and other uploads
  can continue without starting over.
- **One storage core, many clients**. Use Git LFS, OCI, Bazel, S3, Hugging Face
  clients, Xet, or the native CLI against the same content-addressed store.
- **Local or cloud storage**. Run with a local filesystem for a small
  installation or use S3-compatible object storage with PostgreSQL metadata for
  a durable deployment.
- **Operational guardrails**. Request limits, bounded concurrency, cancellation,
  integrity checks, garbage collection, repair tools, metrics, and fault drills
  are part of the server rather than an afterthought.

## Supported frontends

| Frontend | Typical use |
| --- | --- |
| Git LFS | Large files in Git repositories |
| Hugging Face Hub API | Models, datasets, and revisions |
| OCI Distribution | Container images and artifacts |
| S3-compatible API | Applications, analytics, and object tooling |
| Bazel HTTP cache | Remote build and action cache |
| Xet | Chunked repository storage and reconstruction |
| Native CLI | Administration, maintenance, migration, and benchmarks |

The frontends share authorization, deduplication, storage limits, lifecycle
metadata, and observability. A client can use the protocol that fits its
workflow without creating a separate copy of the data.

## Start locally

The development Compose stack starts Shardline with PostgreSQL metadata and
MinIO object storage. It is intended for local evaluation, not production.

Requirements are Docker Engine with the Compose plugin and at least 4 GiB of
available memory.

```bash
git clone https://github.com/STEXS-Technologies/shardline.git
cd shardline
docker compose up --build
```

The server listens on `http://127.0.0.1:18080`. The first start builds the
image, applies database migrations, creates the development bucket, and waits
for the durable services before starting the API.

Check the process and backend readiness from another terminal:

```bash
curl -fsS http://127.0.0.1:18080/healthz
curl -fsS http://127.0.0.1:18080/readyz
```

The Compose file contains development credentials. Replace them before sharing
the stack or exposing it beyond your machine.

Stop the stack while keeping its volumes:

```bash
docker compose down
```

Delete the local development data only when you want a fresh installation:

```bash
docker compose down --volumes
```

## First authenticated request

The local Compose profile uses the local HMAC authentication provider. Mint a
repository-scoped token inside the running container:

```bash
TOKEN="$(docker compose exec -T shardline \
  shardline admin token \
  --issuer local \
  --subject local-operator \
  --scope write \
  --provider generic \
  --owner example \
  --repo assets \
  --revision main \
  --key-env SHARDLINE_TOKEN_SIGNING_KEY)"

curl -fsS \
  -H "Authorization: Bearer ${TOKEN}" \
  http://127.0.0.1:18080/v1/stats
```

For production, use a mounted signing key or an external identity provider.
Read [Client Configuration](docs/CLIENT_CONFIGURATION.md) and
[Authentication](docs/AUTHENTICATION.md) before issuing client credentials.

## Hugging Face and dataset workflows

Shardline provides a Hub-compatible API for repository revisions, file upload,
file download, range reads, and bounded dataset previews. Point compatible
clients at the server instead of moving the data through a second storage
system.

```bash
export HF_ENDPOINT=http://127.0.0.1:18080
export HF_TOKEN="${TOKEN}"

hf upload example/assets ./weights
hf download example/assets model.safetensors --revision main
```

Dataset previews support bounded CSV, JSONL, and Parquet reads. The server
streams only the ranges needed for the requested page and enforces scan, row,
result, and deadline limits.

## DuckDB and Parquet analytics

The recommended full-SQL workflow is DuckDB as an external S3 client. This
keeps analytical execution and its resource usage outside the main API process
while still allowing predicate pushdown, projection, aggregation, joins, and
Parquet export.

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET shardline (
  TYPE S3,
  KEY_ID 'REPOSITORY_SCOPED_TOKEN',
  SECRET 'unused',
  ENDPOINT 'shardline.example:443',
  REGION 'us-east-1',
  URL_STYLE 'path',
  USE_SSL true
);

SELECT label, count(*) AS rows
FROM read_parquet('s3://example.assets/data/train/*.parquet')
WHERE label = 'positive'
GROUP BY label;
```

The Hub also exposes a bounded native query endpoint for authorized dataset
previews. It supports schema discovery, selected columns, validated filters,
bounded aggregates, stable ordering, and pagination. Requests are pinned to an
authorized immutable revision and file identity. Scan bytes, result bytes,
concurrency, execution time, and cancellation are bounded and observable.

Read [DuckDB integration](docs/DUCKDB.md) for the query contract, S3 setup,
limits, benchmark procedure, and the decision boundary between external DuckDB
and the native preview path.

## Production architecture

A small installation can run one Shardline process with local storage. A
durable production installation normally uses:

- Shardline API and transfer roles
- PostgreSQL-compatible metadata storage
- S3-compatible object storage
- Optional Redis reconstruction cache
- TLS termination and an identity provider at the edge

API and transfer roles can scale independently while sharing the same metadata
and object stores. All roles enforce the same content, authorization, and
resource limits.

Use [Deployment](docs/DEPLOYMENT.md) for environment variables, TLS, secrets,
storage adapters, role separation, Kubernetes, and scaling. Use
[Operations](docs/OPERATIONS.md) for backups, restore order, garbage
collection, repair, and incident response.

## The command line tool

The `shardline` binary manages the server and its operational workflows.

```text
shardline serve       Run the server
shardline config      Validate effective configuration
shardline db          Apply or inspect PostgreSQL migrations
shardline health      Probe server health
shardline fsck        Verify object and metadata integrity
shardline index       Rebuild mutable indexes
shardline repair      Repair lifecycle metadata and webhook state
shardline backup      Export recovery artifacts
shardline storage     Migrate objects between storage adapters
shardline gc          Run garbage collection or install a schedule
shardline hold        Manage retention holds
shardline bench       Run performance benchmarks
```

Generate shell completion or a man page with `shardline completion` and
`shardline manpage`. Run `shardline help <command>` for command-specific
configuration and safety details.

## Security and data safety

Shardline is designed around explicit bounds and recoverable state.

- Authentication and repository scope are checked at protocol boundaries.
- Secrets can be supplied through mounted files instead of process arguments.
- Object keys, identifiers, ranges, and serialized metadata are validated.
- Storage publication is content addressed and idempotent.
- Garbage collection quarantines candidates before deletion and can be
  interrupted and resumed safely.
- Metrics and errors avoid exposing credentials, SQL, internal paths, or row
  data.

See [Security and Invariants](docs/SECURITY_AND_INVARIANTS.md),
[Compatibility Status](docs/COMPATIBILITY_STATUS.md), and
[Chaos Engineering](docs/CHAOS_ENGINEERING.md).

## Build from source

The workspace uses Rust 2024. Install Rust and the project quality tools, then
run:

```bash
cargo check --workspace
cargo make ci
```

The CI task runs formatting, clippy, dependency policy checks, workspace tests,
release packaging, and migration synchronization. Docker-backed and end to end
tests are documented in [Contributing](CONTRIBUTING.md).

Useful focused commands include:

```bash
cargo test -p shardline-hub-api
cargo test -p shardline-server --test fault_drills
cargo make shardline-bench-duckdb
```

## Documentation

Start with the [documentation index](docs/README.md), then choose the path that
matches your role.

- [Getting Started](docs/GETTING_STARTED.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Client Configuration](docs/CLIENT_CONFIGURATION.md)
- [Protocol Frontends](docs/PROTOCOLS.md)
- [DuckDB integration](docs/DUCKDB.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Read-only Administration API](docs/ADMIN_READ_API.md)
- [Security and Invariants](docs/SECURITY_AND_INVARIANTS.md)
- [Protocol Conformance](docs/PROTOCOL_CONFORMANCE.md)
- [Coordinated Release](docs/RELEASE.md)

## License

Shardline is dual-licensed under the [MIT License](LICENSE-MIT) and the
[Apache License 2.0](LICENSE-APACHE).
