# Protocol Frontends

Shardline has a protocol-neutral CAS core and explicit frontend adapters.

That means the storage, indexing, authorization, reconstruction, and operator workflows
are shared, while each client-facing protocol lives behind a bounded frontend surface.

## Supported Today

- Xet
- Git LFS
- Bazel HTTP remote cache
- OCI Distribution
- HuggingFace Hub API
- S3 (S3-compatible object API)

## Starting the Server

Shardline enables protocol frontends through the `--frontend` CLI flag.
The default is `xet`.

Start all currently implemented frontends:

```bash
shardline serve --role all --frontend xet,lfs,bazel-http,oci,hub,s3
```

Start only the new non-Xet frontends:

```bash
shardline serve --role all --frontend lfs,bazel-http,oci,hub,s3
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
- Git LFS: `POST /v1/lfs/objects/batch`, `GET|HEAD|PUT /v1/lfs/objects/{oid}`,
  lock creation/listing, lock verification and unlock under `/v1/lfs/locks`
- Bazel HTTP remote cache: `GET|PUT /v1/bazel/cache/ac/{hash}`,
  `GET|PUT /v1/bazel/cache/cas/{hash}`
- OCI Distribution: `GET /v2/`, blob upload and download routes, manifest
  `PUT|GET|HEAD|DELETE`, `GET /v2/{repository}/tags/list`,
  `GET /v2/{repository}/referrers/{digest}`, and `GET /v2/token`
- HuggingFace Hub API: `/api/whoami-v2`, `/api/repos/create`, `/api/{type}/{ns}/{repo}`,
  `/api/{type}/{ns}/{repo}/commit/{rev}`, `/objects/batch`, `/lfs/objects/{oid}`
- S3: `GET /` (the caller's ListBuckets result), bare `GET /{bucket}`
  (ListObjects v1), `?list-type=2`, bucket probes (`?location=`), batch delete
  (`?delete=`), and `/{bucket}/{*key}` object routes (`PUT`/`GET`/`HEAD`/`DELETE`,
  `?uploads` and `?uploadId` multipart sub-resources, `x-amz-copy-source`). An
  explicit `?list-type=1` is unsupported; see [S3 compatibility and scope](S3_FRONTEND.md).

## Client Entry Points

- Xet: native Xet CAS routes plus the provider-aware Git/LFS bridge documented elsewhere
- Git LFS: point the client at `.../v1/lfs` as the LFS URL
- Bazel HTTP remote cache: point Bazel at `.../v1/bazel/cache`
- OCI Distribution: use the server as a registry at `http(s)://<host>/v2/` and
  authenticate either with direct bearer tokens or the registry token flow at
  `GET /v2/token`
- HuggingFace Hub API: set `HF_ENDPOINT=http://<host>` for `huggingface-cli`, or point
  Git at the Smart HTTP endpoints
- S3: point an S3 client at `http://<host>`. Existing clients can keep using a
  repository-scoped Shardline token as the access key. For verified SigV4 and
  presigned URLs, configure signing identities as described in [S3_FRONTEND.md](S3_FRONTEND.md).
  Use TLS in production for either credential mode.

## Quick Start By Frontend

These are the shortest validated client entry points for the implemented frontends.

Git LFS:

```bash
git config lfs.url http://127.0.0.1:8080/v1/lfs
git config http.extraHeader "Authorization: Bearer $SHARDLINE_TOKEN"
git lfs push origin main
```

The same LFS URL supports `git lfs lock`, `git lfs locks`, and `git lfs unlock`.
Locks are repository-wide and survive restart. SQLite serializes changes in a
transaction; PostgreSQL coordinates owners across replicas. A write token is
required to create, verify or remove locks. Forced unlock is available to other
writers. Locks guide cooperating Git LFS clients; direct object uploads remain
content-addressed and do not enforce Git working-tree locks.

With the Hub frontend enabled, locking is also available at
`/{owner}/{repo}.git/info/lfs/locks` and the corresponding `/datasets/` and
`/spaces/` URLs. These routes use the same durable lock store and authenticated
repository namespace as standalone LFS. Native Git LFS locking and transfers are
checked together, including downloads after clearing the client's local cache.
Hub LFS batch requests may omit `ref`. Upload action URLs remain object-scoped;
when a client also sends Authorization, both credentials must verify and agree
on issuer and repository. LFS object routes accept repeated Authorization values
only when they are byte-identical; conflicting values are rejected.

SQLite installs the new lock table when opening metadata. For PostgreSQL, run
the normal `shardline db migrate up` step before starting the updated servers.
This adds metadata tables and leaves existing object bytes unchanged. Finish
upgrading API replicas before relying on locking: an older server does not serve
the locking API, and clients configured to skip verification can still push.

Chunked `PATCH` uploads in a Postgres deployment keep their range map and completion
fence in Postgres and immutable fragments in object storage, so any replica can accept
the next range without shared filesystem state. Local SQLite deployments retain
bounded staging files under `SHARDLINE_ROOT_DIR` as a single-node implementation.

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

OCI manifest and blob bytes are immutable by digest. Mutable tag pointers are
linearized in the metadata database, so replicas share one authoritative value and a
manifest deletion only removes a tag if it still points to that manifest. On first
access after upgrading, legacy object-store-only tag pointers are imported with
create-if-absent semantics. Manifest creation and manifest/blob deletion are serialized
per repository, preventing a tag or manifest from committing against a concurrently
deleted dependency.

OCI `DELETE` is a logical operation. Shardline atomically records a durable tombstone
and removes manifest tags, while retaining immutable payload bytes. Republishing the
same digest clears the tombstone. This prevents a stale database-fence owner from
deleting content made visible by a newer replica; retained deleted bytes are not
currently reclaimed by online GC.

PostgreSQL visibility reads verify the tombstone and its lifecycle evidence in
one read-only repeatable snapshot. A concurrent delete or republish cannot mix
metadata from different commits into a false integrity failure. This does not
change logical deletion or add online reclamation of retained OCI bytes.

Incomplete OCI upload sessions use the same shared API staging root and a
cross-process session lock. Completed blobs remain authoritative in the configured
object store.

Referrers are discovered from verified, visible manifests, so artifacts uploaded
before upgrading are discoverable too. Results support `artifactType`, `n` and
`Link` pagination. Discovery currently scans the repository's manifests; large
repositories therefore incur more backend reads even for a small result page.

Run `bash scripts/verify-protocol-conformance.sh` to check native Git LFS locking
and transfers plus the pinned OCI Distribution v1.1.1 upstream suite. Set
`DATABASE_URL` to a disposable PostgreSQL database to include replica lock races.
Before the PostgreSQL phase, initialize that database with
`shardline db migrate up --database-url "$DATABASE_URL"`. Set `TEST_S3_BUCKET`,
`TEST_S3_ENDPOINT`, `TEST_S3_ACCESS_KEY`, and `TEST_S3_SECRET_KEY` to a disposable
MinIO bucket to include the PostgreSQL/object-store phase.
The upstream suite has mutually exclusive setup and cross-mount cases, so a
successful run includes explicitly skipped alternative cases.
The separate pre-populated-repository run exercises the environment-supplied tag
and tag-list setup paths, then verifies pulls and tag pagination. A direct HTTP
check verifies missing-blob mount fallback (`202`), upload completion, and later
automatic mounting (`201`). The upstream automatic-discovery-disabled branch is
not executed against the current automatic-discovery behavior; skipped branches
are never counted as passed checks.

The script also builds the pinned MinIO Go v7.0.95 signing fixture (Go 1.23 or
newer) and verifies signed chunk/trailer uploads and multipart parts. You can set
`MINIO_SIGNER_BIN` to an already-built fixture. Missing required tools fail the
check; PostgreSQL coverage requires `DATABASE_URL`. The remote phase also tests
this producer with PostgreSQL and MinIO storage using `TEST_S3_*`, including
CRC32, CRC32C, CRC64NVME, SHA-1 and SHA-256 trailers, rejected corrupt replacements,
validly signed incorrect checksums, mid-body TCP disconnects, and exact downloads
after a graceful restart. Client disconnects are distinct from server process
termination and lost storage acknowledgements, which need separate qualification.
This producer version does
not establish compatibility with every MinIO release.

The native protocol E2E target uses `SHARDLINE_REQUIRE_NATIVE_CLIENTS=1` in CI:
missing Git LFS, Skopeo, Helm, Podman, Docker or Bazel clients fail qualification.
The git-xet E2E target applies the same policy to Git, Git LFS and git-xet.
The MinIO-backed case also requires a working Docker runtime. Local runs without
this flag retain optional-tool behavior; a passing local run alone therefore
does not prove every native client was exercised.

For local plain-HTTP registries, OCI clients may require their own insecure-registry
flags or local client configuration.

S3:

```bash
export MC_HOST_shardline=http://127.0.0.1:8080
mc alias set shardline http://127.0.0.1:8080 "$SHARDLINE_TOKEN" unused-secret
mc cp ./model.pt shardline/ac.assets/model.pt
mc ls shardline/ac.assets/
```

In the legacy credential mode, the S3 `access_key` is a Shardline bearer token
scoped to `{owner}.{name}` and the `secret_key` is ignored; the SigV4 signature
is not verified. This works with the documented clients when configured for
that credential mode. Operators can instead configure signing identities to
verify SigV4 and presigned requests. Neither mode expands the operations Shardline
implements; see [S3 compatibility and scope](S3_FRONTEND.md) before choosing a client.
Use TLS in production.

## CAS-Based Interface Protocols

These are the current CAS-facing protocols implemented in the server.
The maturity tiers are defined in [Compatibility Status](COMPATIBILITY_STATUS.md).

| Protocol Name | Frontend Flag | Maturity | Primary Use Case | Addressing Method | Transport Type | Typical Strength |
| --- | --- | --- | --- | --- | --- | --- |
| Xet Protocol | `xet` | **Stable** | ML and large-file repositories | Pointer hashes | Custom and HTTP | Reconstruction-oriented large-file backend. |
| Git LFS | `lfs` | **Beta** | Large file versioning in standard Git workflows | SHA-256 digest | HTTP and JSON | Native interoperability with standard Git LFS clients. |
| Remote HTTP (Bazel) | `bazel-http` | **Beta** | Developer and CI remote caching | SHA-256 digest paths today | HTTP `GET` and `PUT` | Straightforward shared cache protocol for build artifacts. |
| OCI Distribution | `oci` | **Stable** | Container images and OCI artifacts | SHA-256 digest | HTTP and REST | Standard registry protocol for Docker, Kubernetes, and OCI artifacts. |
| Hugging Face Hub API | `hub` | **Beta** | ML model and dataset distribution | Repository paths | HTTP and REST | Tested `hf` CLI uploads, downloads, and `hf_xet` CAS transfer with Hub/Xet frontends enabled together. |
| S3-compatible object API | `s3` | **Stable** | Lakehouse and `s3://` object workflows | Content-addressed storage with standard S3-style MD5 ETags | HTTP with optional verified SigV4 | Validated real-client surface with database-linearized conditional writes. Configured signing identities verify signatures; the legacy access-key-as-token mode does not. See [S3 credential modes and scope](S3_FRONTEND.md). |
