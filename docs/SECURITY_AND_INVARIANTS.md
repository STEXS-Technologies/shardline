# Security and Invariants

Shardline treats every network request, serialized object, token, and storage response
as untrusted until validated.
A malformed upload must never cause a panic, unbounded memory growth, unbounded CPU
work, or partial metadata publication.

## Threat Model

Expected hostile inputs:
- malformed hashes
- oversized request bodies
- truncated xorb and shard bodies
- valid-looking objects with incorrect content hashes
- shards that reference missing xorbs
- shards that attempt excessive metadata growth
- range requests with overflow edges
- oversized reconstruction query strings
- replayed uploads
- tokens with wrong scope, repository, or revision
- oversized or whitespace-injected bearer tokens
- oversized file identifiers or object-storage keys
- oversized provider names, subjects, repository refs, webhook delivery identifiers, and
  webhook authentication headers
- oversized provider bootstrap keys
- missing, malformed, oversized, or incorrect metrics scrape bearer tokens
- object-store responses that are missing, stale, or inconsistent

## Hard Invariants

These invariants must hold in all builds:

- A xorb is stored only if its body parses and its hash matches its content-addressed
  key.
- A shard is registered only if its body parses and all referenced xorbs exist.
- Shard file reconstruction terms may reference no more unique xorbs than the configured
  per-shard xorb limit, including xorbs already known from earlier uploads.
- A file reconstruction is visible only after its shard registration commits.
- Failed multi-file shard publication must not leave a partial visible latest-file set.
- Failed local shard publication must roll back dedupe-shard index mappings created
  during that upload attempt.
- A reconstruction response never returns terms outside the caller's authorization
  scope.
- Range arithmetic never overflows.
- Large transfer request bodies are capped by deployment configuration before validation
  can perform expensive work.
- Native xorb and shard upload bodies are never staged to server-side `incoming` files.
- Native shard parsing enforces bounded section counts before materializing file or xorb
  metadata records.
- Optional content-hash query parameters are validated as Xet hashes before cache-key
  construction or metadata lookup.
- Batch reconstruction query strings are capped before parameter scanning.
- Control-plane request body buffering is bounded by deployment configuration.
- Streamed object writes become visible only through idempotent content-addressed
  installation after integrity validation.
- Local object-store reads, writes, metadata probes, and deletes must reject symlinked
  or non-regular object paths, including symlinked parent directories, instead of
  following them outside the configured object root.
- Local index-store and record-store metadata reads and writes must reject symlinked or
  non-regular files, including symlinked parent directories, instead of following them
  outside the configured state root.
- File downloads must not materialize full reconstructed files before sending response
  bytes.
- No parser allocates based on unbounded client-controlled lengths.
- Global dedupe never leaks cross-scope object existence unless explicitly enabled.
- A retry of the same valid upload is idempotent.
- A conflicting object under an existing content-addressed key is a corruption event.
- Tokens and presigned URLs are never logged.

## Parser Requirements

Parsers must:
- reject unknown or unsupported protocol versions where required
- enforce maximum object size before expensive work
- enforce maximum item counts for shard sections
- bound the number of unique xorbs referenced by file terms before probing object
  storage
- reject trailing garbage when the format requires full consumption
- return explicit typed errors
- avoid panics on malformed input
- avoid recursive parsing patterns that can blow the stack

Shard uploads have both a byte limit and metadata-count limits.
The default bounded shard reader rejects uploads with more than 16,384 file sections,
16,384 xorb sections, 65,536 file reconstruction terms, or 65,536 xorb chunk records.
Operators can raise these per-shard metadata limits for large-media or scientific
workloads without changing the logical file-size model.
Exceeding the configured limits returns a payload-too-large error before the server
commits shard metadata.

## Authorization Requirements

Tokens must carry:
- read or write scope
- repository or namespace scope
- revision or ref scope where applicable
- expiration
- issuer identity

File reconstruction state must be stored under the same repository scope that granted
the write token.
Two repositories with the same logical file path must not overwrite each
other's visible reconstruction state.
Bearer tokens are capped before decoding and must not contain whitespace inside the
token value. Provider identity fields that become token claims are bounded before token
issuance. Provider path names are bounded before registry lookup.
Provider bootstrap keys are bounded at startup and incoming key headers are rejected
before constant-time comparison when their length differs.
Metrics scrape bearer tokens are bounded at startup and incoming token headers are
rejected before constant-time comparison when their length differs.
Webhook HMAC signatures are rejected unless they have the exact SHA-256 hexadecimal
length, so malformed headers cannot force unbounded decode work.
File identifiers and object-storage keys are metadata, not payload size, and are bounded
independently from supported asset size.

Write scope includes read behavior only when explicitly intended by the token model.
Read-only tokens must never upload xorbs or shards.

## Dedupe Privacy

Global deduplication can expose whether a chunk already exists.
That is useful for performance, but it can also be an information leak.

Default policy:
- dedupe lookup is scoped to the caller's authorized repository or tenant
- cross-repository dedupe is disabled by default
- cross-tenant dedupe requires an explicit deployment policy
- dedupe misses return the same externally visible shape regardless of storage topology
- authenticated chunk fetches must prove reachability through an authorized file
  version, not only possession of a readable chunk hash

## Transfer Security

Transfer URLs must:
- expire quickly
- bind to exact xorb hash and byte range
- require `Range` when the protocol expects range-limited access
- avoid granting full-object access when only a range is authorized
- avoid embedding secrets in logs or metrics labels

When using server-mediated transfer, the server must enforce range and scope checks
before opening the object stream.
Authenticated chunk reads must validate file-version context before probing global chunk
metadata so missing context cannot reveal whether a chunk exists.
Transfer streams must validate the indexed length against stored object metadata before
sending bytes. A mismatch is treated as storage or index corruption, not as a partial
successful response.

## Integrity Tooling

`shardline fsck` verifies:
- local metadata records are readable and stored under the expected logical locators
- every referenced chunk exists in the configured object-storage adapter
- every referenced chunk hash matches its body
- every referenced chunk length matches the recorded metadata
- native Xet records still have the unpacked chunk objects implied by their referenced
  xorbs
- reconstruction terms stay contiguous and sum to the recorded logical byte count
- visible latest records still have matching immutable version records
- quarantine metadata still points at existing objects with matching observed lengths
  and does not target reachable live objects
- active retention holds still point at existing objects and do not coexist with
  quarantine state for the same object
- processed webhook delivery claims do not carry implausibly future timestamps
- reconstruction rows reference registered xorbs
- provider repository lifecycle state uses valid repository identity and plausible
  timestamps

`shardline repair lifecycle` removes stale lifecycle metadata without deleting payload
bytes.
It prunes quarantine candidates that became missing, reachable again, or protected
by an active hold; drops expired or missing-object retention holds; and trims stale or
future-dated webhook delivery claims.

`shardline index rebuild` also removes stale reconstruction rows after deriving the
valid file surface from immutable version records.
If version-record validation finds any issue, reconstruction cleanup is skipped so
operators can inspect the metadata before any derived-state deletion.

`shardline gc` is conservative.
It must not delete objects that are reachable from any live reconstruction or pending
upload state.

## Fuzzing Targets

Required fuzz targets:
- hash path parser
- HTTP range parser
- xorb parser
- shard parser
- reconstruction range planner
- object key derivation
- token parser

Fuzz targets must assert logical correctness, not only absence of crashes.
Shardline keeps its fuzz package and corpora under `crates/fuzz/` so protocol and
storage invariants move with the service code.
Run Shardline targets from the repository root with
`cargo +nightly fuzz run --fuzz-dir crates/fuzz <target>` or use
`cargo make shardline-fuzz <target>`.

## Regression Policy

Every security or correctness bug must leave behind at least one permanent check:

- unit test for deterministic bugs
- integration test for API behavior bugs
- fuzz seed for parser crashes
- invariant assertion in production code when runtime enforcement is required
- `debug_assert!` only for implementation invariants that cannot be triggered by user
  input
