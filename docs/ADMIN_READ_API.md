# Read-Only Administration API

Shardline exposes a versioned, read-only operations API for dashboards,
monitoring systems, and Kubernetes operators. It is deliberately separate from
the repository-scoped data API and does not expose mutation methods.

## Enable and authenticate

The API is disabled by default. Configure exactly one of:

```text
SHARDLINE_ADMIN_READ_TOKEN=<token>
SHARDLINE_ADMIN_READ_TOKEN_FILE=/run/secrets/shardline-admin-read-token
```

The file form is recommended in production. Tokens must contain only visible
ASCII without whitespace, be non-empty, and be at most 4096 bytes. Supplying
both forms is a configuration error. The configured secret is redacted from
debug output.

The equivalent `shardline.toml` setting references the secret file without
placing the secret itself in configuration:

```toml
[server]
admin_read_token_path = "/run/secrets/shardline-admin-read-token"
```

An explicitly set environment variable takes precedence over TOML, consistently
with the rest of Shardline's configuration.

Every request uses the dedicated token:

```bash
curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  https://cas.example.com/api/v1/status
```

The comparison is constant-time. Repository tokens and the independently
configured `/metrics` token do not grant access. When no administration token
is configured, these paths return `404`; when configured but authentication is
missing or invalid, they return `401`.

Do not expose these routes through a public ingress. They reveal aggregate
capacity, topology, and process activity even though they do not reveal object
keys, repositories, users, credentials, or presigned URLs.

## Endpoints

| Endpoint | Scope |
| --- | --- |
| `GET /api/v1/status` | Current process durable/cache readiness, role, enabled frontends, and backend kinds |
| `GET /api/v1/storage` | Authoritative CAS counts plus explicitly process-lifetime write counters |
| `GET /api/v1/gc` | GC counters observed by this process and execution ownership |
| `GET /api/v1/integrity` | Fsck counters observed by this process and execution ownership |
| `GET /api/v1/nodes` | Current process health; cluster discovery capability |
| `GET /api/v1/tasks` | Background scheduler capability and known tasks |
| `GET /api/v1/replication` | Replication-controller capability and known replicas |
| `GET /api/v1/metrics` | A bounded JSON snapshot of high-level process metrics |
| `GET /api/v1/plugins` | Reserved typed plugin registry/health surface |

All successful responses contain `api_version: "v1"` and
`observed_at_unix_seconds`. They return `Cache-Control: no-store`. `GET` and
the normal body-free `HEAD` behavior are read-only; mutation methods return
`405 Method Not Allowed`.

The routes are mounted on `all`, `api`, and `transfer` roles. A dashboard must
poll each process or place an explicitly configured aggregation layer in front
of them when it needs per-process visibility.

## State and consistency semantics

The API distinguishes facts Shardline can authoritatively report from facts it
does not own:

- `ready` means the current process completed its backend readiness check.
- `degraded` means that check failed. Internal backend errors are not returned.
- `external` means execution or coordination is owned outside the server
  process, such as CLI/CronJob GC and fsck or storage-provider replication.
- `unsupported` means this version has no registry or discovery mechanism for
  that field.

`storage.authoritative` comes from an admission-controlled inventory of the durable backend
and includes total object count/physical bytes plus CAS chunk and file counts.
Fields under `process_lifetime` are Prometheus counters for only the serving
process and can reset on restart. Shardline does not yet retain authoritative
logical input bytes across every replica, so `deduplication_ratio_state` is
`unsupported` and `deduplication_ratio` is `null` rather than a misleading
value derived from process-local counters.

GC and fsck are currently operator-run commands, not server-owned background
workers. Their endpoints therefore report `state: "external"`,
`execution: "external"`, and expose only counters observed in the current process.
Likewise, Shardline coordinates
multiple writers over shared Postgres/object storage but does not own the
storage provider's replication controller; `/api/v1/replication` reports that
boundary explicitly.

The plugin endpoint is part of the stable shape anticipated by the plugin
architecture. Until a plugin registry exists, it returns `registry:
"unsupported"` and an empty list. Future plugin entries will expose bounded
IDs, versions, health, lifecycle state, and declared capabilities without
granting plugins raw access to the administration API or durable stores.

## Compatibility contract

Within `/api/v1`, existing field meanings and enum values are not changed or
removed in a patch or minor release. New fields and endpoints may be added, so
clients must ignore unknown object fields. A semantic break requires a new API
version. Numeric counters are JSON integers and may increase between polls or
reset when explicitly documented as process-lifetime values.

Responses are bounded: there are no caller-selected page sizes, unbounded
queries, raw labels, or arbitrary object identifiers. `/api/v1/storage` uses
the same weighted admission control as the authenticated stats route, so
monitoring pressure cannot bypass normal request limits.

## Failure behavior

- A dependency outage changes readiness-bearing responses to `degraded`; it
  does not weaken authentication.
- A saturated storage-stat request returns `503` rather than waiting without a
  bound.
- A failed authoritative storage query returns a sanitized server error rather
  than stale or fabricated data.
- Process counters and unsupported/external capability responses remain
  available when they do not require the failed dependency.
- Every response is a snapshot. No endpoint triggers GC, repair, fsck,
  replication, plugin lifecycle, or another state transition.

The deterministic deployment-chaos gate polls this API before, during, and
after a Postgres kill. It requires `ready -> degraded -> ready`, bounded HTTP
completion, secret-free responses, `no-store` cache policy, and continued
rejection of an invalid token throughout the outage.

For Prometheus/OpenMetrics scraping, continue to use `GET /metrics` and its
separate `SHARDLINE_METRICS_TOKEN[_FILE]` secret. The JSON metrics endpoint is
intended for dashboard summaries, not as a replacement for time-series
collection.
