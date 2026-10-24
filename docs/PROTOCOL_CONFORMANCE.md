# Xet Protocol Conformance

Shardline targets the public Xet protocol as a strict compatibility goal, not as a loose
inspiration.

This document is intentionally Xet-specific.
It describes the current frontend contract, not the protocol-neutral CAS core.

Implementation status is tracked in [Compatibility Status](COMPATIBILITY_STATUS.md).

Primary external references:
- Xet Protocol Specification: `https://huggingface.co/docs/xet/index`
- CAS API Documentation: `https://huggingface.co/docs/xet/api`
- Upload Protocol: `https://huggingface.co/docs/xet/upload-protocol`
- Download Protocol: `https://huggingface.co/docs/xet/download-protocol`
- Reference client implementation: `https://github.com/huggingface/xet-core`

## Required CAS Endpoints

### `POST /v1/reconstructions`

Accepts the Xet CAS batch-reconstruction JSON request (`[{"prefix":"default","hash":"…"}]`)
and returns the same `files` response shape as the legacy query-string batch route. Unknown
file IDs are omitted. The request is bounded by the configured body and item-count limits.
The existing `GET /v1/reconstructions?...` form remains available for older clients.

Requirements:
- Require read scope.
- Validate every prefix and file hash before lookup.
- Deduplicate repeated file IDs within one request.
- Preserve the same repository scoping and reconstruction response as the single-file route.

### `GET /v1/reconstructions/{file_id}`

Returns reconstruction terms and fetch information for a file.

Requirements:
- Validate `file_id` before lookup.
- Require read scope.
- Support optional `Range: bytes=start-end`.
- Return `416 Range Not Satisfiable` when the requested start exceeds file length.
- Return fetch URLs that require the requested byte ranges. In authenticated
  deployments with a token-signing key, each URL carries a short-lived,
  cryptographically signed capability bound to the repository, file, Xorb, and
  exact serialized byte range. The transfer request can therefore work without
  forwarding the CAS bearer token, as required by clients that fetch the URL
  directly.
- Send `Cache-Control: private, no-store` on reconstruction responses because
  their URLs contain bearer capabilities. Treat those URLs as secrets; do not
  log or share them.
- Keep existing bearer-authenticated transfer URLs working for older clients.
  A bearer client can still read its repository's exact authorized range after
  a URL capability expires; a client without a bearer token must request fresh
  reconstruction data.
- Reject a missing, altered, or widened range for a capability URL. A capability
  cannot authorize another repository, file, or Xorb.
- Preserve term ordering.
- Never return reconstruction terms for unauthorized repository or revision scopes.

### `GET /v1/chunks/default/{hash}`

Returns shard bytes used for global deduplication.

Requirements:
- Validate prefix and hash.
- Require read scope.
- Return `404 Not Found` for dedupe misses.
- Under authenticated operation, require repository-scoped read access.
- Avoid exposing cross-scope dedupe information unless the deployment explicitly enables
  that policy.

### `POST /v1/xorbs/default/{hash}`

Uploads serialized xorb bytes.

Requirements:
- Validate prefix and hash.
- Require write scope.
- Enforce maximum accepted body size.
- Parse the xorb defensively.
- Accept footer-less xorb uploads and normalize them before validation.
- Recompute and verify the xorb hash.
- Store idempotently.
- Return `was_inserted: false` when the xorb already exists.

### `POST /v1/shards`

Uploads shard metadata and registers files as uploaded.

Requirements:
- Require write scope.
- Enforce maximum accepted body size.
- Parse the shard defensively.
- Accept minimal shard uploads and rebuild the normalized shard layout before indexing.
- Verify shard contents before committing index state.
- Reject shards that reference missing xorbs.
- Register file reconstructions atomically.
- Populate dedupe metadata only after validation succeeds.
- Treat retry of the same valid shard as success.

## Hash Handling

Hashes used in API paths use the protocol's string conversion rules.
A hash string is not treated as a direct byte-for-byte hexadecimal dump unless the
protocol says so for that specific hash.

Required tests:
- byte array to path string conversion
- path string to Shardline hash conversion
- malformed length rejection
- non-lowercase or non-hex rejection, if required by the endpoint
- round-trip behavior for known reference vectors

## Range Semantics

Range handling appears in two places:

- reconstruction requests select a byte range of the logical file
- fetch info describes byte ranges over serialized xorb data

Rules:
- Reconstruction range ends are inclusive.
- Chunk index ranges are end-exclusive.
- Xorb URL byte ranges are inclusive.
- Clients must download whole serialized xorb ranges needed to decode requested chunks.
- The first reconstruction term may include an offset into the first decoded chunk
  range.
- The server must not return fetch info that lacks an authorized range for a returned
  term.

## Error Semantics

The server preserves the status-code behavior expected by clients:

- `400 Bad Request`: malformed input, invalid hash, invalid serialized object
- `401 Unauthorized`: missing or invalid token
- `403 Forbidden`: token is valid but lacks required scope
- `404 Not Found`: missing file, xorb, chunk, or dedupe entry
- `416 Range Not Satisfiable`: reconstruction range cannot be served
- `429 Too Many Requests`: rate limit or overload protection
- `500 Internal Server Error`: unexpected server failure
- `503 Service Unavailable`: dependency or overload failure
- `504 Gateway Timeout`: dependency timeout

Transient failures are retryable.
Permanent client failures are explicit.

## Local Token Verification

Shardline verifies locally signed bearer tokens for development and self-hosted
operation.

With the supported server configuration:

- CAS routes reject missing bearer tokens with `401 Unauthorized`
- invalid or expired bearer tokens return `401 Unauthorized`
- bearer tokens carry issuer, repository, and optional revision scope
- file reconstruction records are namespaced by token repository scope
- reconstruction responses pin authenticated chunk URLs to a concrete file version
- read tokens cannot access write routes
- write tokens are allowed to perform read requests required by upload flows

## Compatibility Tests

Conformance is validated with:

- golden hash conversion vectors
- xorb parse and hash verification tests
- shard parse and validation tests
- upload idempotency tests
- missing-xorb shard rejection tests
- full-file reconstruction tests
- range reconstruction tests
- dedupe hit and miss tests
- unauthorized scope rejection tests
- authenticated native-client tests with provider-issued repository-scoped bearer tokens
- native-client token-refresh route tests
- authenticated native-client concurrent transfer tests
- long-lived authenticated native-client refresh-cycle tests
- native-client ranged download tests
- integration tests against an existing Xet-compatible client

A conforming release can upload and download through a local server with an existing
Xet-compatible client and no client patches.
