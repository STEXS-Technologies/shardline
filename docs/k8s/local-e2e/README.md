# Shardline Local Kubernetes E2E

This overlay stands up a local end-to-end Shardline deployment inside Kubernetes with:

- API and transfer roles split
- ingress-nginx path routing
- MinIO as the S3-compatible object store
- Postgres as the durable metadata store
- Garnet as the deployed Redis-compatible shared reconstruction cache
- a migration job that runs `shardline db migrate up`
- a bucket-init job for MinIO
- NetworkPolicy rules for local namespace isolation

The local policies allow same-namespace pod traffic and include kind's default Service
CIDR (`10.96.0.0/12`) for the Postgres, MinIO, and Redis-cache ports because the test
jobs reach dependencies through ClusterIP Services.

For repeated local runs, rebuild `shardline:e2e`, load it into the kind node, rerun the
one-shot setup jobs, and restart the API and transfer deployments so they pick up the
rebuilt image:

```bash
docker build -t shardline:e2e .
kind load docker-image shardline:e2e --name shardline-e2e
kubectl --context kind-shardline-e2e delete job -n shardline-e2e shardline-migrate minio-bucket-init --ignore-not-found
kubectl --context kind-shardline-e2e apply -k docs/k8s/local-e2e
kubectl --context kind-shardline-e2e rollout restart deployment/shardline-api deployment/shardline-transfer -n shardline-e2e
```

The live cluster protocol test verifies:

- native Xet sparse update upload against the ingress endpoint
- MinIO object growth is smaller for the sparse update than for the initial upload
- Postgres metadata records advance across the upload and reconstruction flow
- Redis-cache keys are populated by repeated versioned reconstruction requests in the
  deployed Garnet instance
- a native Xet client downloads the updated version as a sparse transfer instead of a
  full-file transfer
- the API `/metrics` endpoint is reachable through an authenticated internal Service
  port-forward

After the protocol checks pass, drive CPU load against the ingress endpoint to confirm
that the API and transfer HPAs can scale the split deployment.
