# Getting Started

This is the shortest supported path from a checkout to a working Shardline instance.
It uses the development Docker Compose profile: Postgres and MinIO provide the durable
backends, and the Compose graph waits for the database migration job before starting the
server.

For a production deployment, use [Deployment](DEPLOYMENT.md) and the
[Kubernetes package](k8s/README.md) instead.
The Compose file uses development credentials and is not a production topology.

## Prerequisites

- Docker Engine with the Compose plugin
- at least 4 GiB of available memory

## Start the local server

From the repository root:

```bash
docker compose up --build
```

The first run builds the image, starts Postgres and MinIO, applies all pending database
migrations, creates the `shardline` bucket, and then starts the server on
`http://127.0.0.1:18080`.

The default profile is intentionally local and development-only.
Its credentials are visible in `docker-compose.yml`; replace them before using any
shared environment.

## Check readiness

In another terminal:

```bash
curl -fsS http://127.0.0.1:18080/healthz
curl -fsS http://127.0.0.1:18080/readyz
```

`/healthz` confirms that the process is answering.
`/readyz` also checks the configured durable backends and is the useful gate before
sending traffic.

## Create a local token

The Compose profile uses the local HMAC provider.
Mint a repository-scoped token inside the running container:

```bash
TOKEN="$(docker compose exec -T shardline \
  shardline admin token \
  --issuer local \
  --subject operator-1 \
  --scope write \
  --provider generic \
  --owner team \
  --repo assets \
  --revision main \
  --key-env SHARDLINE_TOKEN_SIGNING_KEY)"

curl -fsS \
  -H "Authorization: Bearer ${TOKEN}" \
  http://127.0.0.1:18080/v1/stats
```

For client-specific setup, continue with
[Client Configuration](CLIENT_CONFIGURATION.md).

## Stop and remove local state

Stop the services while keeping the named volumes:

```bash
docker compose down
```

Remove the local development data as well only when you intentionally want a fresh
instance:

```bash
docker compose down --volumes
```

## Where to go next

- [Deployment profiles](DEPLOYMENT.md) for a small or scaled production layout
- [Operations](OPERATIONS.md) for backups, restore order, scaling, and incident work
- [Compatibility status](COMPATIBILITY_STATUS.md) for protocol tiers and explicit limits
- [Performance](PERFORMANCE.md) for reproducible benchmark commands
- [Disaster recovery](DISASTER_RECOVERY.md) for failure-specific recovery procedures
