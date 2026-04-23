# Shardline Kubernetes

These manifests target the scaled production deployment profile:

- API pods run `shardline serve --role api`
- transfer pods run `shardline serve --role transfer`
- garbage collection runs as a separate `CronJob`
- immutable object bytes live in S3-compatible storage
- durable metadata lives in Postgres-compatible SQL
- reconstruction cache uses the Redis protocol; the shipped manifests deploy Garnet

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

The provider catalog secret carries the provider repository catalog JSON, including
webhook secrets.

The manifests use the placeholder image `registry.example.com/shardline:1.0.0`. Replace
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

Transfer routes:

- `/v1/chunks`
- `/v1/xorbs`
- `/transfer/xorb`

Internal scrape route:

- `/metrics`

`ingress-nginx.yaml` provides one concrete `Ingress` example with those path splits.
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
