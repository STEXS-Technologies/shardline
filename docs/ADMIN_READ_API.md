# Read-Only Administration API

Shardline exposes a versioned, read-only operations API for dashboards,
monitoring systems, and Kubernetes operators. It is separate from the
repository-scoped data API, accepts only a dedicated administration token, and
has no mutation handlers.

## Enable and authenticate

The API is disabled by default. Configure exactly one of:

```text
SHARDLINE_ADMIN_READ_TOKEN=<token>
SHARDLINE_ADMIN_READ_TOKEN_FILE=/run/secrets/shardline-admin-read-token
```

The file form is recommended in production. The equivalent `shardline.toml`
setting references the file without placing the secret in TOML:

```toml
[server]
admin_read_token_path = "/run/secrets/shardline-admin-read-token"
```

An explicitly set environment variable takes precedence over TOML, consistently
with the rest of Shardline's configuration. Supplying both token environment
forms is a configuration error. Tokens must be non-empty visible ASCII without
whitespace and at most 4096 bytes. Secret values are redacted from debug output.

Every request uses the dedicated bearer token:

```bash
curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  https://cas.example.com/api/v1/status
```

The comparison is constant-time. Duplicate or malformed `Authorization`
headers are rejected. Repository tokens and the separately configured
`/metrics` token do not grant access. Disabled routes return `404`; enabled
routes with missing or invalid authentication return `401`. Query parsing
happens only after this concealment/authentication decision.

Keep these routes on an internal ingress. They reveal aggregate capacity,
topology, backend kinds, and process activity, but never object keys,
repositories, users, credentials, connection strings, or presigned URLs.

## Common response contract

The server models these payloads as dedicated DTOs in an explicit `v1` module;
they are not aliases of internal backend, database, or metrics structures. The
`api_version` discriminator and the Rust module boundary therefore advance
together. Pagination inputs are also distinct validated newtypes for page
limits, prefixes, capabilities, cursor tokens, and cursor item keys. Cursor
state uses the `OperationalState` enum rather than string comparison.

Every successful response is JSON and contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `api_version` | string | API schema version. It is `"v1"` for these routes. |
| `observed_at_unix_seconds` | unsigned integer | Wall-clock Unix second at which this process produced the snapshot. It is not a transaction or cluster timestamp. |

Responses include `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`,
and a restrictive Content Security Policy. `GET` and the normal body-free
`HEAD` behavior are read-only. `POST`, `PUT`, `PATCH`, `DELETE`, `CONNECT`, and
`TRACE` do not reach mutation code and return `405`. Browser preflight may
return `200`, but credentialed CORS is not enabled.

Operational-state fields use exactly these values:

| Value | Meaning |
| --- | --- |
| `ready` | The current process successfully checked the named dependency or capability. |
| `degraded` | The current process checked it and the check failed. Internal failure details are withheld. |
| `external` | Execution or coordination belongs to an operator or external system, not this server process. |
| `unsupported` | This API version has no authoritative registry or discovery mechanism for it. |

The routes are mounted on `all`, `api`, and `transfer` runtime roles. Poll each
process, or deploy an explicit aggregation layer, when a dashboard needs
per-process or cluster-wide visibility.

## Endpoints and fields

### `GET /api/v1/status`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version` | string | Common field. |
| `shardline_version` | string | Package version of this running binary. |
| `observed_at_unix_seconds` | unsigned integer | Common field. |
| `state` | operational state | Overall durable-backend readiness of this process. |
| `durable_storage_state` | operational state | Readiness of the configured metadata and object backend combination. |
| `cache_state` | operational state | Reconstruction-cache readiness when this role uses it; otherwise ready because it is outside this role's dependency set. |
| `server_role` | string | Runtime role: `all`, `api`, or `transfer`. |
| `server_frontends` | array of strings | Frontends enabled in this process, such as `xet`, `oci`, `s3`, `lfs`, `bazel`, or `hub`. |
| `metadata_backend` | string | Configured metadata implementation name. No address or credentials are included. |
| `object_backend` | string | Configured object-storage implementation name. No bucket, endpoint, or credentials are included. |
| `cache_backend` | string | Configured reconstruction-cache implementation name. |
| `plugin_registry` | operational state | `unsupported` until a server plugin registry exists. |

### `GET /api/v1/storage`

The authoritative inventory is admission-controlled with the same weighted
limit as the authenticated statistics route.

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version` | string | Common field. |
| `observed_at_unix_seconds` | unsigned integer | Common field. |
| `authoritative.objects` | unsigned integer | Objects in the configured durable object namespace. |
| `authoritative.object_bytes` | unsigned integer | Physical bytes occupied by those objects. |
| `authoritative.chunks` | unsigned integer | Stored CAS chunk objects. |
| `authoritative.chunk_bytes` | unsigned integer | Physical bytes occupied by CAS chunks. |
| `authoritative.files` | unsigned integer | Durable file records. |
| `process_lifetime.objects_written` | unsigned integer | Successful object writes observed by this process since start. |
| `process_lifetime.object_bytes_written` | unsigned integer | Object bytes written by this process since start. |
| `process_lifetime.xorbs_written` | unsigned integer | Xorbs written by this process since start. |
| `process_lifetime.xorb_bytes_written` | unsigned integer | Xorb bytes written by this process since start. |
| `process_lifetime.shards_written` | signed integer | Shards written as reported by the process metric. |
| `process_lifetime.deduplicated_bytes` | unsigned integer | Bytes avoided through deduplication by this process since start. |
| `process_lifetime.compression_saved_bytes` | unsigned integer | Bytes avoided through compression by this process since start. |
| `deduplication_ratio_state` | operational state | `unsupported` until logical input bytes are retained authoritatively across replicas. |
| `deduplication_ratio` | object or null | Null while unsupported. A future object has `numerator_bytes`, `denominator_bytes`, and integer `basis_points`; clients must not synthesize it from process counters. |

Authoritative fields describe current durable state. `process_lifetime` fields
reset on restart and are not cluster totals.

### `GET /api/v1/gc`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `state` | operational state | `external`: GC is currently operator-run. |
| `execution` | operational state | `external`: this endpoint never starts or owns a GC run. |
| `runs_observed_by_process` | unsigned integer | GC runs recorded in this process's lifetime metrics. |
| `objects_collected_by_process` | unsigned integer | Objects reclaimed by those observed runs. |
| `bytes_collected_by_process` | unsigned integer | Bytes reclaimed by those observed runs. |

### `GET /api/v1/integrity`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `state` | operational state | `external`: integrity checking is operator-run. |
| `execution` | operational state | `external`: this endpoint never starts or owns fsck. |
| `fsck_runs_observed_by_process` | unsigned integer | Fsck runs recorded in this process's lifetime metrics. |
| `errors_observed_by_process` | unsigned integer | Integrity errors recorded by those observed runs; it is not an unbounded error listing. |

### `GET /api/v1/nodes`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `discovery` | operational state | `unsupported`: Shardline does not currently maintain a cluster member registry. |
| `nodes` | array | The current process entry after filtering. |
| `nodes[].scope` | string | Stable pagination key; currently `current_process`. It is not a hostname or secret-bearing pod identity. |
| `nodes[].state` | operational state | Current process durable-backend readiness. |
| `nodes[].server_role` | string | This entry's runtime role. |
| `nodes[].server_frontends` | array of strings | Frontends enabled on this entry. |
| `page` | page object | Cursor metadata described below. |

### `GET /api/v1/tasks`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `scheduler` | operational state | `external`: scheduled maintenance belongs to an operator/CronJob. |
| `tasks` | array | Empty until an authoritative bounded task registry exists. |
| `tasks[].id` | string | Stable opaque task identifier and pagination key. |
| `tasks[].state` | operational state | Task lifecycle/health state. |
| `page` | page object | Cursor metadata described below. |

### `GET /api/v1/metrics`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `prometheus_path` | string | Always `/metrics`; use it for full time-series scraping. |
| `active_connections` | signed integer | Connections active in this process at observation time. |
| `admitted_requests` | unsigned integer | Requests admitted during this process lifetime. |
| `queued_requests` | unsigned integer | Requests queued during this process lifetime. |
| `rejected_requests` | unsigned integer | Requests rejected by admission control during this process lifetime. |
| `upload_requests` | unsigned integer | Upload requests observed during this process lifetime. |
| `upload_bytes` | unsigned integer | Upload bytes observed during this process lifetime. |
| `download_requests` | unsigned integer | Download requests observed during this process lifetime. |
| `download_bytes` | unsigned integer | Download bytes observed during this process lifetime. |
| `range_requests` | unsigned integer | Range requests observed during this process lifetime. |

### `GET /api/v1/plugins`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `registry` | operational state | `unsupported` until the plugin registry exists. |
| `plugins` | array | Empty while the registry is unsupported. |
| `plugins[].id` | string | Stable bounded plugin identifier and pagination key. |
| `plugins[].version` | string | Plugin implementation version. |
| `plugins[].state` | operational state | Plugin lifecycle/health state. |
| `plugins[].capabilities` | array of strings | Declared bounded capabilities, used by the `capability` exact-match filter. |
| `page` | page object | Cursor metadata described below. |

### `GET /api/v1/replication`

| Field | Type | Scope and meaning |
| --- | --- | --- |
| `api_version`, `observed_at_unix_seconds` | common | Common fields. |
| `state` | operational state | `external`: replication belongs to the configured storage provider. |
| `coordinator` | operational state | `external`: Shardline has no asynchronous replication controller to report. |
| `replicas` | array | Empty until an authoritative replication registry exists. |
| `replicas[].id` | string | Stable opaque replica identifier and pagination key. |
| `replicas[].state` | operational state | Replica health/lifecycle state. |
| `page` | page object | Cursor metadata described below. |

## Cursor pagination and filtering

`nodes`, `tasks`, `plugins`, and `replication` are keyset-paginated. Other
endpoints reject all query parameters.

| Parameter | Applicable endpoints | Contract |
| --- | --- | --- |
| `limit` | all collections | Optional integer `1..=1000`; default `100`. |
| `cursor` | all collections | Optional opaque URL-safe base64 cursor returned as `next_cursor`; maximum 1024 bytes. |
| `state` | all collections | Exact operational-state match. |
| `prefix` | all collections | Case-sensitive prefix of the stable item key; maximum 128 decoded bytes. |
| `capability` | `plugins` only | Exact declared-capability match; non-empty and at most 128 decoded bytes. |

Every collection contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `page.limit` | unsigned integer | Effective requested/default page size. |
| `page.returned` | unsigned integer | Number of entries in this response. |
| `page.next_cursor` | string or null | Pointer after the final returned key, or null when no later matching entry exists. |

Pass the cursor back with the same filters:

```bash
curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  'https://cas.example.com/api/v1/plugins?limit=100&state=ready&capability=storage.read'

curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  'https://cas.example.com/api/v1/plugins?limit=100&state=ready&capability=storage.read&cursor=...'
```

Cursors are versioned, integrity-checked structurally, and bound to `state`,
`prefix`, and `capability`. Changing a bound filter, corrupting a cursor,
repeating a parameter, using an unknown parameter, using invalid percent
encoding/control characters, or exceeding the 4096-byte raw-query bound fails
closed. Treat cursors as opaque, short-lived pointers, not durable bookmarks.

Ordering is ascending by stable item key. Each request is a fresh snapshot;
there is no transaction spanning pages. If a collection changes between page
requests, deleted entries disappear and newly inserted entries whose keys sort
at or before the cursor are not revisited. The current node collection is a
single process entry, and the other registries are presently empty, but this
contract applies when they gain entries.

## Status and failure behavior

| Status | Meaning |
| --- | --- |
| `200` | Authorized snapshot or browser preflight. |
| `400` | Authorized request has an invalid, duplicate, unknown, polluted, or filter-incompatible query/cursor. |
| `401` | API is enabled but bearer authentication is missing, malformed, duplicated, or wrong. |
| `404` | API is disabled; route availability is concealed. |
| `405` | The route does not support that method. |
| `414` | Authorized raw query exceeds 4096 bytes. |
| `503` | Weighted admission is saturated or a dependency is temporarily unavailable through a mapped retryable error. |
| `500` | Sanitized unexpected backend failure; internal errors and secrets are not serialized. |

A dependency outage changes readiness-bearing fields to `degraded`; it never
weakens authentication. Authoritative storage inventory fails rather than
returning stale or fabricated data. Process counters and external/unsupported
capability responses remain available when they do not need the failed
dependency. No endpoint triggers GC, repair, fsck, replication, plugin
lifecycle, publication, or another durable transition.

## Security threat model and verification

The claim is coverage of vulnerability classes applicable to this API, with
non-applicable classes stated explicitly—not a claim that testing can prove the
absence of every future vulnerability.

| Class | Control and test evidence |
| --- | --- |
| Broken authentication / authorization | Dedicated token, constant-time comparison, duplicate-header rejection, token-domain separation, disabled-route concealment, every endpoint and runtime role tested. |
| Broken object/property authorization | No repository/object/user identifiers or caller-selected fields exist. Responses are one fixed operator-level projection behind one operator boundary. |
| Injection (SQL/command/template/header) | Filters operate only on typed in-memory values; no query text reaches SQL, shells, templates, or response headers. SQL/XSS/CRLF payload regressions and arbitrary-string property/fuzz tests are included. |
| XSS/content sniffing | JSON serialization, no reflection of filters, `nosniff`, and `default-src 'none'` CSP are tested on success and error paths. |
| CSRF | Authentication is an explicit bearer header, not an ambient cookie, and no mutation method exists. Credentialed CORS is disabled. |
| CORS abuse | Preflight is body-free and does not grant credentials. Operators must still restrict ingress and origins because a bearer explicitly supplied by application code remains a bearer. |
| SSRF | No endpoint accepts URLs, hosts, callbacks, paths, or other outbound-request targets. |
| Path traversal / file disclosure | No endpoint accepts filesystem or object paths, and backend names are fixed implementation identifiers. |
| Request smuggling / method confusion | Duplicate authorization fails; ambiguous duplicate query fields fail; mutation and uncommon methods cannot reach handlers. HTTP framing remains the responsibility of the HTTP stack and ingress. |
| Resource exhaustion | Query/filter/cursor/page bounds, bounded fixed projections, weighted storage admission, and request timeouts are exercised. No endpoint returns raw labels or unbounded identifiers. |
| Sensitive-data exposure / caching | Fixed response DTOs omit keys, tenants, credentials, URLs, and backend errors. `no-store` and sanitized-outage regressions cover success and failure. |
| Replay | Reads are idempotent snapshots; replay cannot mutate state. Bearer replay remains possible until token rotation, so protect transport and token files. |
| Unsafe API consumption | The API consumes no third-party response or attacker-selected remote data. Backend failures are converted to bounded states or sanitized errors. |
| Security misconfiguration / inventory | Disabled by default, explicit internal-ingress guidance, versioned `/api/v1`, strict TOML schema, Kubernetes secret projection, and manifest regression tests. |

Use TLS at the ingress or service mesh. Token rotation is an operator action;
the API intentionally does not expose a token-minting or rotation endpoint.

## Durability, chaos, and fault evidence

| Scenario | Required invariant |
| --- | --- |
| Concurrent polling and request cancellation | Bounded completion; no panic; durable inventory unchanged. |
| Router/process restart after polling | Authoritative inventory is identical because reads create no durable API state. Process-lifetime counters may reset as documented. |
| Postgres killed during an active upload | Status transitions `ready -> degraded -> ready`; invalid tokens remain rejected; malformed queries remain rejected; storage errors reveal no token, password, or connection string; acknowledged object bytes survive recovery. |
| Dependency failure during storage inventory | Return a sanitized error, never fabricated or partial authoritative counts. |
| Arbitrary query bytes | Deterministic parser/newtype result, no panic, bounded decode; covered by proptest and the `shardline_admin_api_query` libFuzzer target. |
| Arbitrary versioned cursor DTO bytes | Deterministic deserialization and validation of typed state, prefix, capability, and item-key fields; covered by the `shardline_admin_api_v1_cursor` libFuzzer target. |
| Authentication and query failure ordering | Disabled malformed requests remain `404`; unauthorized malformed requests remain `401`; only an authorized malformed request receives `400`. |
| Security headers across failure states | `no-store`, `nosniff`, and restrictive CSP remain present on success, authentication failure, and disabled-route responses. |

The deployment-chaos test is deterministic and part of the project's permanent
chaos gate. Every discovered failure should be reduced to a replayable
regression. Chaos and fuzzing provide strong evidence; they do not
mathematically prove correctness.

## Compatibility contract

Within `/api/v1`, existing field meanings and enum values are not changed or
removed in a patch or minor release. New fields and endpoints may be added, so
clients must ignore unknown object fields. A semantic break requires a new API
version and a new DTO module rather than reinterpreting the v1 types. Numeric
counters are JSON integers and can increase between polls or reset only where
documented as process-lifetime values.

For Prometheus/OpenMetrics scraping, continue to use `GET /metrics` and its
separate `SHARDLINE_METRICS_TOKEN[_FILE]` secret. The JSON endpoint is a bounded
dashboard summary, not a replacement for time-series collection.
