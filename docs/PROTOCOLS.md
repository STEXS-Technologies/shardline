# Protocol Frontends

Shardline has a protocol-neutral CAS core and explicit frontend adapters.

That means the storage, indexing, authorization, reconstruction, and operator workflows
are shared, while each client-facing protocol lives behind a bounded frontend surface.

## Supported Today

- Xet
- Git LFS
- Bazel HTTP remote cache
- OCI Distribution

## Starting the Server

Shardline enables protocol frontends through the `--frontends` CLI flag.
The default is `xet`.

Start all currently implemented frontends:

```bash
shardline serve --role all --frontends xet,lfs,bazel-http,oci
```

Start only the new non-Xet frontends:

```bash
shardline serve --role all --frontends lfs,bazel-http,oci
```

When bearer-token auth is enabled, all enabled frontends use the same repository-scoped
token model. The environment override is `SHARDLINE_SERVER_FRONTENDS`, but the CLI flag
is the primary operator-facing entry point.

OCI-specific knobs:

- `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS`
- `SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS`
- `SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS`
- `SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS`

Current route families:

- Xet: reconstructions, shard upload, xorb upload, chunk transfer, xorb transfer
- Git LFS: `POST /v1/lfs/objects/batch`, `GET|HEAD|PUT /v1/lfs/objects/{oid}`
- Bazel HTTP remote cache: `GET|PUT /v1/bazel/cache/ac/{hash}`,
  `GET|PUT /v1/bazel/cache/cas/{hash}`
- OCI Distribution: `GET /v2/`, blob upload and download routes, manifest
  `PUT|GET|HEAD|DELETE`, `GET /v2/{repository}/tags/list`, and `GET /v2/token`

## Client Entry Points

- Xet: native Xet CAS routes plus the provider-aware Git/LFS bridge documented elsewhere
- Git LFS: point the client at `.../v1/lfs` as the LFS URL
- Bazel HTTP remote cache: point Bazel at `.../v1/bazel/cache`
- OCI Distribution: use the server as a registry at `http(s)://<host>/v2/` and
  authenticate either with direct bearer tokens or the registry token flow at
  `GET /v2/token`

## Quick Start By Frontend

These are the shortest validated client entry points for the implemented frontends.

Git LFS:

```bash
git config lfs.url http://127.0.0.1:8080/v1/lfs
git config http.extraHeader "Authorization: Bearer $SHARDLINE_TOKEN"
git lfs push origin main
```

Bazel HTTP remote cache:

```bash
bazel build //... \
  --remote_cache=http://127.0.0.1:8080/v1/bazel/cache \
  --remote_header=Authorization=Bearer\ $SHARDLINE_TOKEN
```

OCI Distribution:

```bash
skopeo login --username shardline --password "$SHARDLINE_TOKEN" http://127.0.0.1:8080
skopeo copy oci:./artifact:latest docker://127.0.0.1:8080/team/assets:latest
```

For local plain-HTTP registries, OCI clients may require their own insecure-registry
flags or local client configuration.

## CAS-Based Interface Protocols

These are the current CAS-facing protocols implemented in the server.

| Protocol Name | Frontend Flag | Primary Use Case | Addressing Method | Transport Type | Typical Strength |
| --- | --- | --- | --- | --- | --- |
| Xet Protocol | `xet` | ML and large-file repositories | Pointer hashes | Custom and HTTP | Reconstruction-oriented large-file backend. |
| Git LFS | `lfs` | Large file versioning in standard Git workflows | SHA-256 digest | HTTP and JSON | Native interoperability with standard Git LFS clients. |
| Remote HTTP (Bazel) | `bazel-http` | Developer and CI remote caching | SHA-256 digest paths today | HTTP `GET` and `PUT` | Straightforward shared cache protocol for build artifacts. |
| OCI Distribution | `oci` | Container images and OCI artifacts | SHA-256 digest | HTTP and REST | Standard registry protocol for Docker, Kubernetes, and OCI artifacts. |
