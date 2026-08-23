# Rolling Upgrade

This document describes how to upgrade a running Shardline deployment without a full
service outage: one role class at a time, verifying readiness after each step.

Shardline processes are stateless in the request path. The durable boundaries are the
object-store adapter and the index and record adapters, which are external to the
process. That makes a live upgrade safe as long as each process is drained, restarted
on the new version, and confirmed ready before the next process moves.

## Roles And Upgrade Units

`shardline serve` pins a process to a role:

- `shardline serve --role api` — control-plane traffic: reconstruction planning,
  provider token issuance, provider webhook ingestion, shard registration, health,
  readiness, and metrics
- `shardline serve --role transfer` — data-plane traffic: xorb upload, chunk
  download, ranged xorb transfer
- `shardline serve --role all` — both roles in one process; the default when
  `SHARDLINE_SERVER_ROLE` is unset

The role split only partitions the frontend set across processes; it does not select
different protocols (see [Deployment](DEPLOYMENT.md#runtime-shape)).

The upgrade unit is a role class, not an individual process:

- in the [Production Scaled](DEPLOYMENT.md#production-scaled) profile, `api` and
  `transfer` are separate Deployments; each Deployment is one role class
- in the [Local Single-Node](DEPLOYMENT.md#local-single-node) profile there is a
  single `all`-role process; the procedure reduces to one drain, restart, and
  readiness check

## Recommended Order

Upgrade one role class at a time, and verify the class is fully ready before starting
the next. Recommended order: `transfer` first, then `api`.

Why:

- while one class is mid-rollout, the other stays at the previous version and keeps
  serving, so there is always a known-good version to roll back to
- transfer carries the long-lived, high-throughput upload and download connections;
  upgrading it first means the data plane moves to the new version before the control
  plane changes, and API token issuance, webhook ingestion, and reconstruction
  planning are unaffected throughout the transfer rollout
- API handles readiness for the deployment; upgrading it last means readiness
  semantics change only after the data plane is already validated on the new version

Do not upgrade both classes concurrently. If the new version contains a metadata
schema change, apply it before the process rollout with `shardline db migrate up`
(see [Database Migrations](DATABASE_MIGRATIONS.md)); the schema must be compatible
with the previous version's processes for the duration of the rollout.

The OCI tag-index migration is additive and old processes continue to read their
object-store tag pointers. During the API-class rollout, drain OCI manifest `PUT` and
`DELETE` traffic, OCI blob deletion, and provider webhook traffic, or route those
mutations exclusively to the new API version. Reads
remain available and legacy tags are imported lazily. Resume OCI mutations after all
API replicas run the new version; this avoids two software versions using different
authoritative tag-pointer stores or bypassing repository-scoped locks during the brief
mixed-version window.

The durable resumable-session migration is also additive, but session formats are not
shared across versions. Before replacing the API class, stop admitting new Git LFS
PATCH, OCI blob-upload, and S3 multipart sessions to the old replicas and allow their
in-flight sessions to finish or expire. Keep the legacy RWX mount attached only while
old API replicas remain. New replicas persist new sessions in Postgres/object storage;
after every API replica is upgraded and legacy sessions are drained, the RWX claim can
be removed permanently. Do not route one legacy session between old and new replicas.

Suspend destructive GC (`--mark` and/or `--sweep`) for the entire mixed-version
rollout. The new release coordinates GC against writers with a shared/exclusive
barrier, but an older server does not participate in that barrier. Dry-run GC remains
safe. Resume scheduled destructive GC only after every API and transfer replica runs
the barrier-aware version.

## Procedure

The example uses the Production Scaled profile (`kubectl`). For systemd or host-native
processes, replace the drain and restart steps with the equivalent service control
(`systemctl stop`, replace the binary, `systemctl start`).

### 1. Snapshot Before Upgrade

```bash
cd /srv/assets

shardline backup manifest \
  --output reports/pre-upgrade-manifest.json

shardline db migrate status
```

Keep the previous container image tag or binary so rollback is one restart away.

### 2. Upgrade The Transfer Class

Drain the class so in-flight connections finish and new traffic moves to other
replicas:

```bash
kubectl -n shardline rollout restart deployment/shardline-transfer
kubectl -n shardline rollout status deployment/shardline-transfer --timeout=10m
```

With a single transfer replica, the restart is a brief availability dip for transfer
traffic. The API class keeps serving throughout.

Confirm the process is up:

```bash
curl -fsS http://127.0.0.1:8080/healthz
```

Confirm readiness. `/readyz` returns JSON; verify the role and backends:

```bash
curl -fsS http://127.0.0.1:8080/readyz
```

```json
{
  "server_role": "transfer",
  "server_frontends": ["xet"],
  "object_backend": "s3",
  "cache_backend": "memory"
}
```

The transfer class is upgraded only when `/healthz` answers `200` and `/readyz`
reports the expected `server_role`, `object_backend`, and `cache_backend`.

### 3. Upgrade The API Class

Repeat the same steps for the API Deployment:

```bash
kubectl -n shardline rollout restart deployment/shardline-api
kubectl -n shardline rollout status deployment/shardline-api --timeout=10m

curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

`/readyz` must report `server_role: "api"` with the expected backends before the
upgrade is considered complete.

For a single-node `--role all` deployment, run the same drain, restart, and readiness
checks on the single process.

### 4. Post-Upgrade Verification

```bash
cd /srv/assets

shardline fsck

shardline backup manifest \
  --output reports/post-upgrade-manifest.json
```

Compare the post-upgrade manifest counts and byte totals with
`reports/pre-upgrade-manifest.json`. Return full traffic after `fsck` exits `0`.

## Readiness Polling

- `GET /healthz` — liveness; always open and answers `200` while the process is up
- `GET /readyz` — readiness; reports the index store and required object-store
  operations are available, with fields `server_role`, `server_frontends`,
  `object_backend`, and `cache_backend`

Poll `/readyz` in a loop until it reports the expected role and backends, or fails
the rollback condition below. `/readyz` is for probes and service-local diagnostics,
not public ingress (see [Deployment](DEPLOYMENT.md#health-checks)).

## Rollback

To revert one class to the previous version:

1. restart that class with the previous image tag or binary (drain first, exactly as
   in the upgrade steps)
2. poll `/healthz` then `/readyz` until the class reports ready
3. verify with `shardline fsck` and the backup manifest

If a schema migration was part of the upgrade and the downgrade needs the previous
schema, revert it with `shardline db migrate down` before restarting the old
processes (see [Database Migrations](DATABASE_MIGRATIONS.md)). Because only one class
is mid-rollout at any time, rollback always has a known-good version to return to.

## Automated Evidence

`e2e/rolling_upgrade_e2e.rs` replaces each role independently, polls `/healthz` and
`/readyz`, and confirms the other role keeps serving. Role separation itself is covered
by `e2e/role_split_e2e.rs`.

The `mixed-version-upgrade` reliability job adds the binary compatibility evidence that
an in-process test cannot provide. It automatically selects the greatest preceding
SemVer tag reachable from the current commit, builds that release and the current commit
separately, starts two N-1 processes against shared Postgres and S3, then performs:

```text
N-1 + N-1 -> N + N-1 -> N + N -> N-1 + N
```

At every mixed-version stage, each binary reads exact bytes written by the other. The
rollback stage also publishes through N-1 and reconstructs through N. Its transcript
and exact N/N-1 commit identities are retained as CI artifacts. This evidence applies
to the tested adjacent release pair; every release reruns the automatically advanced
pair rather than assuming transitive compatibility.
