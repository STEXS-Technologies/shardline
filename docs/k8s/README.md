# Shardline Kubernetes

These manifests target the scaled production deployment profile:

- API pods run `shardline serve --role api`
- transfer pods run `shardline serve --role transfer`
- garbage collection runs as a separate `CronJob`
- immutable object bytes live in S3-compatible storage
- durable metadata lives in Postgres-compatible SQL
- reconstruction cache uses the Redis protocol; the manifests reference an external
  Redis-compatible endpoint that you must provide (the runtime secret carries a
  placeholder URL at `garnet.example.com` — no cache service is deployed here)

This package does not assume local object storage or local metadata.
The pod state mount is only local runtime state in this profile, not the durable source
of truth.

## Layout

```text
docs/k8s/production-scaled/
  namespace.yaml
  serviceaccount.yaml
  configmap.yaml
  runtime-secret.template.yaml
  provider-catalog-secret.template.yaml
  networkpolicy-default-deny-ingress.yaml
  networkpolicy-allow-ingress-nginx.yaml
  networkpolicy-allow-monitoring.yaml
  networkpolicy-allow-runtime-egress.template.yaml
  api-deployment.yaml
  api-service.yaml
  api-hpa.yaml
  api-pdb.yaml
  transfer-deployment.yaml
  transfer-service.yaml
  transfer-hpa.yaml
  transfer-pdb.yaml
  gc-cronjob.yaml
  ingress-nginx.yaml
  kustomization.yaml
```

## Configuration

Server settings are defined in `configmap.yaml` as a `shardline.toml` file mounted at
`/etc/shardline/shardline.toml`. Shardline automatically detects this path, so no
`--config` flag is needed.
Credentials and secrets remain in the runtime secret as env vars or mounted files.

```bash
kubectl apply -f docs/k8s/production-scaled/configmap.yaml
```

Use `--config` and `--env-file` with the CLI locally to match the production
configuration during development:

```bash
shardline --config docs/k8s/production-scaled/configmap.yaml serve
```

## Prerequisites

- a Kubernetes cluster with the `autoscaling/v2` API enabled
- an ingress controller that supports `Ingress`
- a TLS secret for the public Shardline hostname
- a reachable S3-compatible object store
- a reachable Postgres-compatible metadata database
- a reachable Redis-compatible cache
- a monitoring namespace labeled `shardline.io/monitoring=true` if Prometheus-style
  scraping runs outside the Shardline namespace

## Secret Material

Fill these templates before applying the manifests:

- `runtime-secret.template.yaml`
- `provider-catalog-secret.template.yaml`

The runtime secret carries:

- S3 credentials
- Postgres URL
- Redis cache URL
- Shardline token-signing key
- metrics scrape bearer token
- provider bootstrap API key
- provider-config at-rest encryption key

The provider catalog secret carries the provider repository catalog JSON, including
webhook secrets.

The provider-config webhook secrets are encrypted at rest with
`SHARDLINE_CONFIG_SECRET_KEY`, a 32-byte AES-256 key read from the runtime
secret's `config-secret-key` entry. Generate one before applying the manifests:

```bash
openssl rand -base64 24
```

24 random bytes base64-encode to exactly 32 characters, and the trailing newline
the command prints is stripped on load. Without this key, the fail-loud
plaintext-secrets gate rejects the deployment because the provider runtime is
enabled (`SHARDLINE_PROVIDER_API_KEY_FILE` + `SHARDLINE_PROVIDER_CONFIG_FILE`),
so the API pods would CrashLoopBackOff in their `config-check` init container.

The manifests use the placeholder image `registry.example.com/shardline:1.0.1` (the
`1.0.1` tag is an example, not the current workspace version). Replace
it with the image tag you build or publish for your environment.

The runtime egress policy is environment-specific and is therefore shipped as a
template:

- `networkpolicy-allow-runtime-egress.template.yaml`

Fill its placeholder CIDRs or replace them with namespace and pod selectors that match
your Postgres, Redis-cache, and object-storage endpoints before applying it.

## Apply

```bash
kubectl apply -f docs/k8s/production-scaled/runtime-secret.template.yaml
kubectl apply -f docs/k8s/production-scaled/provider-catalog-secret.template.yaml
kubectl apply -f docs/k8s/production-scaled/networkpolicy-allow-runtime-egress.template.yaml
kubectl apply -k docs/k8s/production-scaled
```

## Public Routing

The scaled profile needs path-aware routing in front of the two Services.

API routes:

- `/healthz`
- `/v1/providers`
- `/api`
- `/reconstructions`
- `/v1/reconstructions`
- `/v2/reconstructions`
- `/shards`
- `/v1/shards`
- `/v1/stats`
- S3 route family (API tier): `GET /` (`ListBuckets`), `/{bucket}` and `/{bucket}/`
  (bucket-level operations), `/{bucket}/{*key}` (object data path)
- Hub LFS objects: `/objects/batch`, `/lfs/objects/{oid}`
- Hub Git Smart HTTP: `/{type}/{ns}/{repo}/info/refs`, `/{type}/{ns}/{repo}/HEAD`,
  `/{type}/{ns}/{repo}/git-upload-pack`, `/{type}/{ns}/{repo}/git-receive-pack`

Transfer routes:

- `/v1/chunks`
- `/v1/xorbs`
- `/transfer/xorb`

Internal scrape route:

- `/metrics`

`ingress-nginx.yaml` provides one concrete `Ingress` example with the Xet route
family splits. The S3 bucket/object paths (`/{bucket}`, `/{bucket}/{*key}`) and the
Hub path-based routes are **API-tier** and must be sent to the API Service; the
shipped example does not encode them (the S3 catch-all in particular needs explicit
path mapping per bucket when both roles are exposed on one hostname).
Clusters that use another gateway should keep the same path ownership while translating
the object shape to their controller.
The example also disables NGINX request and response buffering and sets the body-size
limit to the same 64 MiB ceiling the default Shardline configuration accepts.
`/readyz` stays internal to Kubernetes probes and service-local diagnostics because it
reports runtime backend topology.
The example does not expose `/metrics` on the public ingress.
Scrape it through the API Service or an internal monitoring ingress.
The production manifests configure `SHARDLINE_METRICS_TOKEN_FILE`, so scrapers must send
`Authorization: Bearer <metrics-token>` when reading `/metrics`.

## Scaling

API and transfer pods scale independently.

- API pods are sized for token issuance, reconstruction planning, webhook ingestion, and
  shard registration.
- transfer pods are sized for chunk reads, xorb uploads, and xorb range transfer.

Horizontal Pod Autoscalers are intentionally separate so control-plane latency and
transfer throughput can grow without forcing the same replica count.

## Garbage Collection

Garbage collection is not an in-process background worker.
The Kubernetes package runs it through `CronJob`.

The default job executes:

```text
shardline gc --mark --sweep --retention-seconds 86400
```

That means:

- the run marks current orphans
- sweep deletes only quarantine candidates whose retention already expired
- new candidates receive a one-day retention window unless they were already quarantined

Adjust the schedule and retention to match the recovery window your operators want.

During incident response or restore work, suspend the CronJob before mutating metadata
or restoring storage:

```bash
kubectl patch cronjob shardline-gc -n shardline -p '{"spec":{"suspend":true}}'
```

Resume it only after validation:

```bash
kubectl patch cronjob shardline-gc -n shardline -p '{"spec":{"suspend":false}}'
```

For the broader production operating model, see [Operations](../OPERATIONS.md).

## Disposable `kind` Smoke Test

The repository ships a CI-grade smoke test for the scaled deployment profile.
It builds the current Docker image, creates a temporary `kind` cluster, starts Postgres,
Redis, and MinIO inside the cluster, creates the S3 bucket and metadata schema, and
deploys the production manifests through a small test overlay.

Run it from the repository root when `docker`, `kind`, `kubectl`, `cargo`, and `python3`
are available:

```bash
scripts/k8s/kind-smoke.sh
```

The test validates both role-specific readiness responses, provider-token issuance, an
authenticated xorb upload through the transfer Service, shard registration through the
API Service, a ranged transfer download, API statistics, metrics authentication, and
that API and transfer routes remain separated.
It uses one low-resource replica of each Shardline role and does not require an ingress
controller; the script port-forwards each Service directly.

The cluster name is unique by default and is deleted on success, failure, or
interruption. The locally built smoke image and port-forward processes are also removed.
On failure, the script preserves Kubernetes diagnostics in a temporary directory (or in
`SHARDLINE_KIND_LOG_DIR` when set) before deleting the cluster.
Set `SHARDLINE_KIND_CLUSTER_NAME` only when a predictable, still-disposable name is
required; the script refuses to reuse an existing cluster.

> **Test-only:** `SHARDLINE_KIND_LOG_DIR` and `SHARDLINE_KIND_CLUSTER_NAME` are
> environment overrides for this disposable `kind` smoke test only. They are not
> Shardline server configuration and have no effect on a deployed server.

## Runtime hardening

The production-scaled manifests are designed for the Kubernetes `restricted` Pod
Security Standard: containers run as non-root, drop every Linux capability, disallow
privilege escalation, use a read-only root filesystem, and select the node's
`RuntimeDefault` seccomp profile.
Keep an equivalent AppArmor profile enabled where the cluster supports it; clusters
without AppArmor support must enforce an equivalent runtime policy through their
admission controller.
