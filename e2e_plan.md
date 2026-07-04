# E2E test plan — full coverage

Every path, pipeline, flag, and edge case across the entire project.

## 1. Upload → Store → Reconstruct (content round trip)

- [ ] Upload single chunk xorb, reconstruct file, verify bytes match
- [ ] Upload multi chunk shard, reconstruct file, verify bytes match
- [ ] Upload file that fits exactly in one chunk boundary
- [ ] Upload file that spans exactly two chunk boundaries
- [ ] Upload file smaller than chunk size (partial chunk)
- [ ] Upload empty file (zero bytes)
- [ ] Upload file with binary content (null bytes, random bytes)
- [ ] Upload file with text content
- [ ] Upload file with unicode filename
- [ ] Upload same file through every frontend, verify bytes match across all of them
- [ ] Upload different files, verify each reconstructs independently
- [ ] Upload 1000 small files, verify all reconstruct
- [ ] Upload file exactly 1 byte smaller than chunk size
- [ ] Upload file exactly chunk size
- [ ] Upload file exactly 1 byte larger than chunk size
- [ ] Upload file 2^32 bytes (4GB+) to verify large file support
- [ ] Upload file with all possible byte values (0x00-0xFF)
- [ ] Upload file with sequential pattern (0,1,2,3...)
- [ ] Upload same logical file through two different frontends simultaneously (race)

## 2. Metadata round trip

- [ ] Upload with content type header, verify preserved on GetObject
- [ ] Upload with custom headers, verify preserved on GetObject
- [ ] Upload and set content disposition, verify on download
- [ ] Upload and set content encoding, verify on download
- [ ] Upload and set cache control, verify on download
- [ ] Upload and set content language, verify on download
- [ ] Reconstruct file, verify ETag (BLAKE3 root) matches stored hash
- [ ] Upload with every standard HTTP header, verify preserved
- [ ] Upload with header name containing special characters
- [ ] Upload with empty header value
- [ ] Upload with header value exceeding reasonable length (32KB+)
- [ ] Upload with duplicate headers (last wins or merge)
- [ ] Upload with extremely long content type (256+ characters)

## 3. Dedup and storage efficiency

- [ ] Upload same content twice, verify only one physical chunk stored
- [ ] Upload same content 10 times, verify only one chunk stored
- [ ] Upload similar content (append 1 byte), verify only that chunk changed
- [ ] Upload similar content (prepend 1 byte), verify only that chunk changed
- [ ] Upload similar content (modify first byte), verify only that chunk changed
- [ ] Upload similar content (modify last byte), verify only that chunk changed
- [ ] Upload similar content (modify middle byte of each chunk), verify only affected chunks changed
- [ ] Upload file A → delete A → upload same content as B → verify B reconstructable
- [ ] Upload 3 files sharing chunks → delete middle file → verify outer two still reconstruct
- [ ] Upload 10 files all sharing chunks → delete 9 → verify last still reconstructs
- [ ] Verify storage accounting reports correct dedup ratio
- [ ] Upload identical file to two different frontends, verify single storage

## 4. Range requests and partial reads

- [ ] Request first 100 bytes, verify exact bytes returned
- [ ] Request middle 100 bytes, verify exact bytes returned
- [ ] Request last 100 bytes, verify exact bytes returned
- [ ] Request suffix range (-500 bytes)
- [ ] Request range beyond file end, verify 416 status
- [ ] Request open ended range (100-), verify returns from offset to end
- [ ] Request range on multi chunk file, verify chunk boundaries transparent to client
- [ ] Request overlapping ranges, verify each produces correct output
- [ ] Request range of length 1 (single byte)
- [ ] Request range of length 0 (empty range, 416)
- [ ] Request multiple ranges in single request (multipart/byteranges)
- [ ] Request range across chunk boundary (spans two chunks)
- [ ] Request range exactly aligning with chunk boundary
- [ ] Request range starting at file offset 0 with length equal to file size
- [ ] Request range with negative start value (invalid, 416)

## 5. Frontend specific operations

### Xet
- [ ] Issue read token, use it to download
- [ ] Issue write token, use it to upload
- [ ] Batch reconstruction request with single file ID
- [ ] Batch reconstruction request with multiple file IDs
- [ ] Batch reconstruction request with 0 file IDs (empty)
- [ ] Batch reconstruction request with 1000 file IDs
- [ ] Batch reconstruction with duplicate file IDs
- [ ] Shard upload with single chunk
- [ ] Shard upload with multiple chunks
- [ ] Shard upload with 0 chunks (empty shard)
- [ ] Xet routes disabled when role is api only
- [ ] Xet routes disabled when role is transfer only
- [ ] Token with xet frontend scope cannot access LFS
- [ ] Token with LFS scope cannot access Xet

### Git LFS
- [ ] LFS batch request for single object
- [ ] LFS batch request for multiple objects (2, 10, 100)
- [ ] LFS batch request with operations (download, upload)
- [ ] LFS batch request with transfers (basic, other)
- [ ] LFS batch request with unknown transfer adapter (rejected)
- [ ] LFS batch request with non standard transfer adapter (rejected)
- [ ] LFS batch request with excessive cardinality (rejected)
- [ ] LFS batch request with empty objects array
- [ ] LFS batch request with missing required fields (rejected)
- [ ] LFS batch request with invalid JSON body (rejected)
- [ ] LFS batch request with oversized body (rejected)
- [ ] LFS upload object with valid content
- [ ] LFS upload object with empty content
- [ ] LFS upload object with content hash mismatch (rejected)
- [ ] LFS download object that exists
- [ ] LFS download object that does not exist (404)
- [ ] LFS head object that exists
- [ ] LFS head object that does not exist
- [ ] LFS objects survive GC when referenced

### OCI
- [ ] Push image manifest (by tag)
- [ ] Push image manifest (by digest)
- [ ] Push image index (multi arch manifest)
- [ ] Pull manifest by tag
- [ ] Pull manifest by digest
- [ ] Pull manifest with non existent tag (404)
- [ ] Pull manifest with non existent digest (404)
- [ ] Pull manifest with corrupt body (digest mismatch)
- [ ] Push blob
- [ ] Push blob with digest mismatch (rejected)
- [ ] Push blob with empty body
- [ ] Pull blob by digest
- [ ] Pull blob with non existent digest (404)
- [ ] Head manifest (exists)
- [ ] Head manifest (not exists)
- [ ] Head blob (exists)
- [ ] Head blob (not exists)
- [ ] Delete manifest
- [ ] Delete blob still referenced by manifest (rejected)
- [ ] Delete blob not referenced by any manifest
- [ ] Delete non existent manifest (404 or success)
- [ ] Delete non existent blob (404 or success)
- [ ] Tag listing (list all tags for repository)
- [ ] Tag listing with empty repository
- [ ] Tag listing with 1000+ tags (pagination)
- [ ] Referrers API (list referrers for manifest)
- [ ] Referrers API with no referrers
- [ ] Cross repository blob mount
- [ ] Cross repository blob mount with non existent source (404)
- [ ] Multi arch index push and pull
- [ ] Docker schema2 manifest compat
- [ ] OCI upload session TTL expiration (session expires, upload fails)
- [ ] OCI upload max active sessions limit (rejects when full)
- [ ] OCI registry token flow (authenticated pull with token)
- [ ] OCI anonymous pull (no auth required)
- [ ] Push manifest with invalid JSON
- [ ] Push manifest with missing required fields
- [ ] Push blob exceeding max body size

### Bazel HTTP remote cache
- [ ] Put AC cache entry
- [ ] Get AC cache entry (exists)
- [ ] Get AC cache entry (not exists, 404)
- [ ] Put CAS cache entry
- [ ] Get CAS cache entry (exists)
- [ ] Get CAS cache entry (not exists, 404)
- [ ] Head on cache entry (exists)
- [ ] Head on cache entry (not exists)
- [ ] Bazel remote cache with empty hash (rejected)
- [ ] Bazel remote cache with invalid hash characters
- [ ] Bazel remote cache with oversized body
- [ ] Put then Get, verify bytes match
- [ ] Put same entry twice, verify no error (idempotent)

### Hub (HuggingFace API)
- [ ] Dataset upload (single file)
- [ ] Dataset upload (directory)
- [ ] Dataset download
- [ ] Dataset download with range
- [ ] Dataset download non existent (404)
- [ ] Model upload
- [ ] Model download
- [ ] Model upload with metadata (card, tags)
- [ ] Webhook events (repository create, update, delete)
- [ ] Repository listing (list all repos)
- [ ] Repository listing for specific user/org
- [ ] Repository creation
- [ ] Repository deletion

### Metrics
- [ ] Metrics endpoint returns prometheus text format
- [ ] Metrics endpoint includes all registered metrics
- [ ] Metrics counter increment on each request
- [ ] Metrics histogram recorded for request duration
- [ ] Metrics gauge reports current state (active connections, etc.)
- [ ] Metrics endpoint accessible without auth
- [ ] Metrics endpoint with auth required (if configured)
- [ ] Metrics scrape endpoint | grep returns expected metric names
- [ ] Metrics cardinality does not explode with unique label values
- [ ] Metrics after 0 requests (empty counters)

## 6. Health and readiness

- [ ] /healthz returns 200 when server is running
- [ ] /readyz returns 200 when storage backend is initialized
- [ ] /readyz returns non 200 when storage backend is missing
- [ ] /readyz returns non 200 when database is unavailable
- [ ] Health check works without auth
- [ ] Readiness check works without auth
- [ ] Health check body is valid JSON
- [ ] Readiness check body includes component status
- [ ] Health check during GC operation (still healthy)
- [ ] Health check during storage migration (degraded or healthy)

## 7. Auth providers (every path)

### Local Ed25519
- [ ] Mint token with valid signing key
- [ ] Mint token with randomly generated key (32+ bytes)
- [ ] Mint token with key exactly 32 bytes
- [ ] Mint token with key exceeding max bytes (rejected)
- [ ] Verify token, reconstruct file (success)
- [ ] Verify token with wrong signing key (rejected)
- [ ] Verify expired token (rejected)
- [ ] Verify token at exact expiration time (boundary)
- [ ] Verify token 1 second before expiration (accepted)
- [ ] Verify token 1 second after expiration (rejected)
- [ ] Verify token with insufficient scope (rejected)
- [ ] Verify token with missing authorization header (rejected)
- [ ] Verify token with malformed bearer header (rejected)
- [ ] Verify token with oversized token (rejected)
- [ ] Token scope: read only can read, cannot write
- [ ] Token scope: write only can write, cannot read
- [ ] Token scope: admin can do everything
- [ ] Token scope: repository scoped token cannot access other repository
- [ ] Token scope: multiple scopes OR (has any of these)
- [ ] Token scope: all scopes AND (must have all of these)
- [ ] Empty signing key file (rejected)
- [ ] Signing key file with valid content, then file grows (rejected)
- [ ] Two signing key sources (env var + file, rejected)

### OIDC
- [ ] Configure OIDC provider, verify token issued by OIDC works
- [ ] Expired OIDC token rejected
- [ ] OIDC token with wrong audience (rejected)
- [ ] OIDC token with wrong issuer (rejected)
- [ ] OIDC token with wrong subject (if configured)
- [ ] Missing OIDC issuer config returns error at startup
- [ ] OIDC provider unreachable (degraded auth, fallback or fail)
- [ ] OIDC token with `nbf` claim in the future (rejected)
- [ ] OIDC token with `iat` claim in the future (rejected)
- [ ] OIDC provider returns malformed JWKS (rejected)

### JWKS
- [ ] Configure JWKS endpoint, verify token works
- [ ] JWKS token signed by roteted key (old key works until cache expires)
- [ ] Expired JWKS token rejected
- [ ] JWKS token with wrong `kid` (rejected)
- [ ] JWKS endpoint returns 500 (fallback to cached keys)
- [ ] JWKS endpoint returns empty keys (rejected)
- [ ] Missing JWKS URL config returns error at startup
- [ ] JWKS background refresh respects Cache-Control max-age
- [ ] JWKS background refresh respects ETag
- [ ] JWKS background refresh falls back to 300s interval

### Passthrough
- [ ] Any token accepted (dev mode)
- [ ] Missing token still rejected (if configured)
- [ ] Passthrough with arbitrary claims (verify claims reach downstream)

## 8. Config flags (every combination)

### Frontend flags
- [ ] `--frontends xet` serves Xet only, rejects LFS/OCI/Bazel/Hub/Metrics
- [ ] `--frontends lfs` serves LFS only, rejects others
- [ ] `--frontends oci` serves OCI only, rejects others
- [ ] `--frontends bazel-http` serves Bazel only, rejects others
- [ ] `--frontends hub` serves Hub only, rejects others
- [ ] `--frontends metrics` serves Metrics only, rejects others
- [ ] `--frontends xet,lfs,oci,bazel-http,hub,metrics` serves all
- [ ] `--frontends xet,lfs` serves two, rejects others
- [ ] `--frontends xet,oci` serves two, rejects others
- [ ] `--frontends lfs,oci` serves two, rejects others
- [ ] `--frontends xet,lfs,oci` serves three, rejects others
- [ ] Every single frontend flag works in isolation (6 combinations)
- [ ] Every pair of frontend flags works (15 combinations)
- [ ] Every triple of frontend flags works (20 combinations)
- [ ] Invalid frontend name rejected
- [ ] Empty frontend flag rejected (or defaults)
- [ ] Frontend flag with extra whitespace (trimmed or rejected)
- [ ] Frontend flag with uppercase characters (case sensitive, rejected)

### Role flags
- [ ] `--role api` serves API routes only (tokens, webhooks, reconstructions)
- [ ] `--role transfer` serves transfer routes only (upload/download chunks)
- [ ] `--role both` serves all routes
- [ ] `--role API` (uppercase, rejected)
- [ ] `--role transfer,api` (comma separated, rejected or merged)
- [ ] `--role api,transfer` (rejected)
- [ ] Invalid role rejected
- [ ] Empty role flag rejected (or defaults to both)
- [ ] Role split: token issued on api node, used on transfer node (cross node)

### Frontend + role combinations
- [ ] xet + api (tokens + reconstructions)
- [ ] xet + transfer (chunk upload/download)
- [ ] xet + both (everything)
- [ ] lfs + api (batch)
- [ ] lfs + transfer (object upload/download)
- [ ] lfs + both
- [ ] oci + api (manifests, tags)
- [ ] oci + transfer (blobs)
- [ ] oci + both
- [ ] bazel-http + api (nothing, bazel is transfer only)
- [ ] bazel-http + transfer (cache operations)
- [ ] hub + api (webhooks, dataset listing)
- [ ] hub + transfer (file upload/download)
- [ ] metrics + api (metrics endpoint)
- [ ] Every frontend with every role (18 combinations)

### Auth provider flags
- [ ] `SHARDLINE_AUTH_PROVIDER=local` with signing key
- [ ] `SHARDLINE_AUTH_PROVIDER=local` without signing key (rejected)
- [ ] `SHARDLINE_AUTH_PROVIDER=oidc` with issuer URL
- [ ] `SHARDLINE_AUTH_PROVIDER=oidc` without issuer URL (rejected)
- [ ] `SHARDLINE_AUTH_PROVIDER=jwks` with JWKS URL
- [ ] `SHARDLINE_AUTH_PROVIDER=jwks` without JWKS URL (rejected)
- [ ] `SHARDLINE_AUTH_PROVIDER=passthrough` (no key needed)
- [ ] `SHARDLINE_AUTH_PROVIDER=invalid` rejected
- [ ] `SHARDLINE_AUTH_PROVIDER=LOCAL` (uppercase, rejected)
- [ ] `SHARDLINE_AUTH_PROVIDER` empty (defaults to local)

### Body limit flags
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=1048576` (1MB limit, upload 1MB+ file rejected)
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=1` (too small, upload rejected)
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=1073741824` (1GB, large upload succeeds)
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=0` (rejected)
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=-1` (rejected as invalid)
- [ ] `SHARDLINE_MAX_REQUEST_BODY_BYTES=notanumber` (rejected)
- [ ] Default body limit (64MB) in effect when unset
- [ ] Body limit exactly at file size (succeeds)
- [ ] Body limit one byte less than file size (rejected)

### Storage flags
- [ ] `SHARDLINE_STORAGE_ADAPTER=local` (default, no extra config)
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with valid S3 config
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` without bucket (rejected)
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with invalid endpoint (rejected at startup or runtime)
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with allow-http=true (HTTP endpoint works)
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with allow-http=false and HTTP endpoint (rejected)
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with path style access
- [ ] `SHARDLINE_STORAGE_ADAPTER=s3` with virtual hosted style access
- [ ] `SHARDLINE_STORAGE_ADAPTER=invalid` (rejected)

### Secret file flags
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with valid key file
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with missing file (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with symlink outside directory (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with symlink inside directory (accepted)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with empty file (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with zero bytes (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with file that grows after validation (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with file that shrinks after validation (rejected)
- [ ] `SHARDLINE_TOKEN_SIGNING_KEY_FILE` with binary content (accepted if valid bytes)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with valid JSON credentials
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with invalid JSON (rejected)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with missing fields (rejected)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with extra fields (extra ignored or rejected)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with empty file (rejected)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` not found (rejected)
- [ ] `SHARDLINE_S3_CREDENTIAL_FILE` with credentials that fail S3 auth (rejected at runtime)

### OCI specific flags
- [ ] `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS=60`
- [ ] `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS=1` (minimum)
- [ ] `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS=notanumber` (rejected)
- [ ] `SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS=1` (reject second session)
- [ ] `SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS=10`
- [ ] `SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS=0` (rejected, must be >= 1)
- [ ] `SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS=60`
- [ ] `SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS=1`
- [ ] `SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS=0` (rejected)
- [ ] `SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS` + `SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS` combined

### CORS flags
- [ ] `SHARDLINE_CORS_ALLOWED_ORIGINS=https://example.com` (allow specific origin)
- [ ] `SHARDLINE_CORS_ALLOWED_ORIGINS=https://example.com,https://other.com` (allow two)
- [ ] `SHARDLINE_CORS_ALLOWED_ORIGINS=*` (allow all)
- [ ] `SHARDLINE_CORS_ALLOWED_ORIGINS=` (default, none)
- [ ] `SHARDLINE_CORS_ALLOWED_ORIGINS=*` with credentials (cors + credentials interaction)
- [ ] Cross origin request with allowed origin (succeeds)
- [ ] Cross origin request with disallowed origin (rejected)
- [ ] Cross origin preflight OPTIONS with allowed origin
- [ ] Cross origin preflight OPTIONS with disallowed origin
- [ ] Cross origin request without origin header (treated as same origin)

### Configuration precedence
- [ ] CLI flag overrides env var
- [ ] Env var overrides default value
- [ ] Config file (if supported) overrides env var
- [ ] Missing config falls back to documented default
- [ ] Conflicting config sources produce error (not silent override)
- [ ] Unknown env var is silently ignored (no crash)
- [ ] Unknown CLI flag produces error (not silently ignored)

## 9. CLI commands (every subcommand)

### serve
- [ ] `shardline serve` starts, responds to health check, stops on signal
- [ ] `shardline serve` with SIGTERM (graceful shutdown)
- [ ] `shardline serve` with SIGINT (immediate shutdown)
- [ ] `shardline serve` already running on same port (bind error)
- [ ] `shardline serve` with invalid config (rejected at startup)
- [ ] `shardline serve --help` prints help
- [ ] `shardline serve` with all config flag combinations

### admin token
- [ ] `shardline admin token` with valid signing key env var
- [ ] `shardline admin token` with signing key file
- [ ] `shardline admin token` with missing signing key (rejected)
- [ ] `shardline admin token` with both env and file key (rejected, conflict)
- [ ] `shardline admin token` with key file that grows after validation (rejected)
- [ ] `shardline admin token` with symlinked key file outside directory (rejected)
- [ ] `shardline admin token` with key file in same directory as symlink target (accepted)
- [ ] `shardline admin token --help`

### gc
- [ ] `shardline gc --mark` (mark only, no deletion)
- [ ] `shardline gc --sweep` (sweep only, no mark)
- [ ] `shardline gc --mark --sweep` (mark and sweep)
- [ ] `shardline gc --dry-run` (report only, no mutation)
- [ ] `shardline gc --export-report report.json` (write report)
- [ ] `shardline gc --export-orphans orphans.csv` (write orphan list)
- [ ] `shardline gc --export-report` without path (error)
- [ ] `shardline gc --export-orphans` without path (error)
- [ ] `shardline gc` without any flags (help)
- [ ] `shardline gc --retention-seconds 3600`
- [ ] `shardline gc --retention-seconds 0` (immediate sweep)
- [ ] `shardline gc --retention-seconds -1` (rejected)
- [ ] `shardline gc --mark --sweep --retention-seconds 0` (full cycle immediate)
- [ ] `shardline gc --help`

### fsck
- [ ] `shardline fsck` on clean storage (passes)
- [ ] `shardline fsck` with corrupt chunk (detects corruption)
- [ ] `shardline fsck` with missing chunk (reports missing)
- [ ] `shardline fsck` with corrupt webhook metadata
- [ ] `shardline fsck` with corrupt database (detected)
- [ ] `shardline fsck` on empty storage (passes)
- [ ] `shardline fsck --help`

### rebuild
- [ ] `shardline rebuild` runs without error
- [ ] `shardline rebuild` with corrupt records (recovers gracefully)
- [ ] `shardline rebuild` reaches fixed point (second run is no op)
- [ ] `shardline rebuild` with missing database (error)
- [ ] `shardline rebuild` with storage that has extra chunks (indexes them)
- [ ] `shardline rebuild` with storage that has missing chunks (reports them)
- [ ] `shardline rebuild --help`

### repair lifecycle
- [ ] `shardline repair lifecycle` runs without error
- [ ] `shardline repair lifecycle` reaches fixed point
- [ ] `shardline repair lifecycle` with stale metadata (cleans it)
- [ ] `shardline repair lifecycle` on empty storage (passes)
- [ ] `shardline repair lifecycle --help`

### repair orchestrator
- [ ] `shardline repair orchestrator` (rebuild + repair + verify)
- [ ] `shardline repair orchestrator` with corrupt storage (reports errors)
- [ ] `shardline repair orchestrator` on clean storage (passes)
- [ ] `shardline repair orchestrator --help`

### backup manifest
- [ ] `shardline backup manifest --output manifest.json`
- [ ] `shardline backup manifest` with write protected output directory (error)
- [ ] `shardline backup manifest` with symlinked output (rejected)
- [ ] `shardline backup manifest` with symlinked root override (rejected)
- [ ] `shardline backup manifest` without output path (defaults or error)
- [ ] `shardline backup manifest` on empty storage (produces empty manifest)
- [ ] `shardline backup manifest --help`

### hold
- [ ] `shardline hold set --object-hash abc123 --reason "testing"`
- [ ] `shardline hold set --object-hash abc123` without reason (rejected or empty)
- [ ] `shardline hold set` with nonexistent object (succeeds or warns)
- [ ] `shardline hold set` with missing hash (rejected)
- [ ] `shardline hold list` (returns all holds)
- [ ] `shardline hold list` with no holds (empty list)
- [ ] `shardline hold release --object-hash abc123`
- [ ] `shardline hold release` with missing hash (rejected)
- [ ] `shardline hold release` with nonexistent hold (error or no op)
- [ ] `shardline hold set --object-hash abc123 --reason "test"` then release, verify list empty
- [ ] `shardline hold set` with object that is already held (no op or duplicate)

### storage-migrate
- [ ] `shardline storage-migrate` from local to S3
- [ ] `shardline storage-migrate` from S3 to local
- [ ] `shardline storage-migrate` with symlinked local root (rejected)
- [ ] `shardline storage-migrate` with already migrated (no op)
- [ ] `shardline storage-migrate` with missing source (error)
- [ ] `shardline storage-migrate` with invalid destination (error)
- [ ] `shardline storage-migrate` with in progress migration (reject or wait)

### bench
- [ ] `shardline bench e2e` runs benchmark to completion
- [ ] `shardline bench ingest` runs ingest benchmark to completion
- [ ] `shardline bench concurrent` runs concurrent benchmark to completion
- [ ] `shardline bench sparse` runs sparse benchmark
- [ ] `shardline bench` with invalid scenario (rejected)
- [ ] `shardline bench` with zero concurrency (rejected)
- [ ] `shardline bench` with concurrency exceeding reasonable limit (rejected or warn)
- [ ] `shardline bench` with mutation window larger than asset (rejected)
- [ ] `shardline bench e2e` with explicit chunk size
- [ ] `shardline bench e2e` with explicit upload budget
- [ ] `shardline bench e2e` with storage root override
- [ ] `shardline bench --focus sparse` (run only sparse scenario)
- [ ] `shardline bench --help`

### completion
- [ ] `shardline completion bash` generates bash completions
- [ ] `shardline completion zsh` generates zsh completions
- [ ] `shardline completion fish` generates fish completions
- [ ] `shardline completion powershell` generates powershell completions
- [ ] `shardline completion` without shell argument (prints help)
- [ ] `shardline completion unknown` (rejected)
- [ ] Verify generated completion works in actual shell

### manpage
- [ ] `shardline manpage --output shardline.1`
- [ ] `shardline manpage` without output (error or default)
- [ ] `shardline manpage` with write protected output (error)
- [ ] Verify generated manpage renders correctly with `man shardline.1`
- [ ] Manpage mentions all core commands (serve, admin, gc, fsck, rebuild, repair, backup, bench)

### misc CLI
- [ ] `shardline` (no args, prints help)
- [ ] `shardline --help` (same as `shardline`)
- [ ] `shardline --version` (prints version)
- [ ] `shardline help` (prints help)
- [ ] `shardline help serve` (prints serve subcommand help with examples)
- [ ] `shardline help admin token` (deep nested help)
- [ ] `shardline unknown-command` (rejected with suggestion)
- [ ] `shardline --debug serve` (enables debug output)
- [ ] `shardline serve --invalid-flag` (rejected)
- [ ] `shardline serve --config` with missing path (error)
- [ ] `shardline serve --config` with non existent file (error)

### CLI pipeline integration
- [ ] Secret file grows after validation → command fails
- [ ] Secret file is symlinked outside directory → command fails
- [ ] Secret file is symlinked inside directory → command succeeds
- [ ] Multiple signing key sources (env + file + flag) → rejected
- [ ] Debug mode redacts database credentials from output
- [ ] All commands produce exit code 0 on success
- [ ] All commands produce exit code non 0 on failure
- [ ] Commands pipe stdout correctly (no binary output to terminal)
- [ ] Commands write errors to stderr (not stdout)

## 10. Storage backends

### Local filesystem
- [ ] Init storage directory on first run (creates directory)
- [ ] Init storage directory when already exists (reuse)
- [ ] Read/write chunks to storage
- [ ] List chunks in storage (returns all)
- [ ] List chunks in empty storage (returns empty)
- [ ] Delete chunk from storage
- [ ] Delete non existent chunk (error or no op)
- [ ] Reconstruct file from chunks on disk
- [ ] Storage root with existing content (no reset, works)
- [ ] Storage root with symlinked parent (rejected)
- [ ] Storage root with read only permissions (error on write)
- [ ] Storage root with disk full (error on write, clean error)
- [ ] Corrupt chunk on disk (detected during reconstruct via hash mismatch)
- [ ] Missing chunk on disk (reported as unreachable)
- [ ] Chunk file with wrong name (hash does not match content, detected)
- [ ] Chunk file modified externally (hash mismatch detected)
- [ ] Storage with subdirectories (ensure flat or structured layout)

### S3 backend
- [ ] Init S3 bucket (bucket must exist)
- [ ] Read/write chunks to S3
- [ ] List chunks in S3
- [ ] List chunks in empty bucket (empty)
- [ ] Delete chunk from S3
- [ ] Delete non existent chunk (no op)
- [ ] Reconstruct file from S3 chunks
- [ ] S3 with endpoint override (minio, seaweed, ceph)
- [ ] S3 with path style vs virtual hosted
- [ ] S3 with allow http (HTTP endpoint)
- [ ] S3 with credentials from file
- [ ] S3 with credentials from env
- [ ] S3 with credentials from both (conflict, error)
- [ ] S3 credential file with growth after validation (rejected)
- [ ] S3 with invalid bucket (error at startup)
- [ ] S3 with invalid region (error at startup or runtime)
- [ ] S3 with non existent endpoint (connection error, retry)
- [ ] S3 with slow endpoint (timeout)
- [ ] S3 chunk upload retry on network error
- [ ] S3 chunk download retry on network error
- [ ] S3 concurrent read/write to same bucket (no corruption)
- [ ] S3 with virtual hosted style + HTTPS
- [ ] S3 with path style + HTTP

## 11. Index backends

### SQLite
- [ ] Init database file on first run (creates file + runs migrations)
- [ ] Init database file when already exists (reuse, verify schema)
- [ ] Insert reconstruction entry (single)
- [ ] Insert reconstruction entries in batch (1000)
- [ ] Query reconstruction entry by file ID (exists)
- [ ] Query reconstruction entry by file ID (not exists, returns none)
- [ ] Delete reconstruction entry
- [ ] Delete reconstruction entry (not exists, no op)
- [ ] Insert dedupe shard mapping
- [ ] Query dedupe shard mapping
- [ ] Delete dedupe shard mapping
- [ ] Insert stored object record
- [ ] Query stored object record
- [ ] List all stored objects (pagination)
- [ ] List stored objects in range
- [ ] Insert duplicate reconstruction entry (error or replace)
- [ ] Insert duplicate dedupe shard mapping (error or replace)
- [ ] SQLite with WAL mode (verify durability)
- [ ] SQLite with defensive settings (prevent SQL injection, etc.)
- [ ] Corrupt SQLite file (detected, error)
- [ ] SQLite file with incompatible schema version (migrate)
- [ ] SQLite with concurrent readers (thread safe)
- [ ] SQLite with concurrent writer + readers (serialized)
- [ ] SQLite journal mode verification
- [ ] Migrate SQLite from empty (new database)
- [ ] Migrate SQLite from version N to N+1 (up migration)
- [ ] Migrate SQLite from version N+1 to N (down migration)
- [ ] Migrate SQLite with pending migration (run one by one)

### Postgres
- [ ] Connect to PostgreSQL (valid credentials)
- [ ] Connect to PostgreSQL (invalid credentials, error)
- [ ] Connect to PostgreSQL (unreachable host, error)
- [ ] Run migrations on empty database
- [ ] Run migrations on already migrated database (no op)
- [ ] Insert/query/delete records
- [ ] Concurrent transactions (isolation)
- [ ] Connection pool exhaustion (limit reached, new connection queued or error)
- [ ] Postgres connection failure during operation (reconnect)
- [ ] Postgres query timeout (error, not hang)
- [ ] Large result set (pagination via cursor or limit)
- [ ] Network partition during insert (rollback on reconnect)
- [ ] Migrations up from each version
- [ ] Migrations down from each version
- [ ] Migration rollback on failure

## 12. Security and hardening

- [ ] Symlinked storage root on startup (rejected)
- [ ] Symlinked config file on startup (rejected if outside directory)
- [ ] Symlinked secrets file (accepted if inside directory)
- [ ] Symlinked secrets file (rejected if outside directory)
- [ ] Path traversal in object name with `../` (rejected)
- [ ] Path traversal in object name with absolute path (rejected)
- [ ] Path traversal in object name with encoded slashes (rejected)
- [ ] Path traversal in repository name (rejected)
- [ ] Null bytes in object name (rejected)
- [ ] Control characters in object name (rejected or encoded)
- [ ] Oversized request body rejected at limit boundary
- [ ] Oversized request body rejected 1 byte over limit
- [ ] Oversized request body accepted at limit
- [ ] Oversized bearer token (rejected)
- [ ] Bearer token with whitespace (rejected)
- [ ] Bearer token with extra `Bearer ` prefix (stripped or rejected)
- [ ] Missing authorization header on protected route (401)
- [ ] Malformed authorization header (no bearer prefix, 401)
- [ ] Expired token (401)
- [ ] Token for wrong repository (403)
- [ ] Token for wrong scope (403)
- [ ] Token with insufficient permissions (403)
- [ ] CORS disallowed origin (rejected)
- [ ] CORS allowed origin (accepted)
- [ ] CORS preflight OPTIONS with all HTTP methods
- [ ] CORS preflight with custom headers
- [ ] CORS preflight without Access-Control-Request-Method (rejected)
- [ ] Metrics endpoint returns data without auth (public)
- [ ] Health endpoint returns data without auth (public)
- [ ] All other endpoints require auth (default)
- [ ] SSRF: private IP 127.0.0.1 as backend URL (rejected)
- [ ] SSRF: private IP 10.x.x.x as backend URL (rejected)
- [ ] SSRF: private IP 172.16.x.x as backend URL (rejected)
- [ ] SSRF: private IP 192.168.x.x as backend URL (rejected)
- [ ] SSRF: link local 169.254.x.x (rejected)
- [ ] SSRF: IPv6 loopback ::1 (rejected)
- [ ] SSRF: public IP (allowed)
- [ ] Multiple signing key sources (conflict, error)
- [ ] Empty signing key (error)
- [ ] Token signing key too large (error)
- [ ] Algorithm none attack prevention (alg=none rejected)
- [ ] Log injection via header values (prevented)
- [ ] Error messages do not leak internal state
- [ ] Error messages do not include stack traces (production mode)

## 13. Concurrent access and race conditions

- [ ] 10 concurrent uploads of different files, all succeed
- [ ] 10 concurrent uploads of same file, all succeed (idempotent)
- [ ] 100 concurrent uploads, same file (idempotent, no crash)
- [ ] 10 concurrent reconstructs of same file, all return correct bytes
- [ ] 100 concurrent reconstructs of same file, all return correct bytes
- [ ] Upload + GC running at same time (GC does not delete in-flight upload)
- [ ] Reconstruct + GC running at same time (GC does not delete in-flight read)
- [ ] Rebuild + upload running at same time (rebuild sees new data eventually)
- [ ] Rebuild + GC running at same time (no conflict)
- [ ] 100 concurrent requests, mixed read/write (no corruption)
- [ ] 1000 concurrent requests, mixed read/write (no corruption, no connection exhaustion)
- [ ] Token validation + reconstruction concurrent (no auth bottleneck under load)
- [ ] Storage migration + upload concurrent (migration consistent)
- [ ] GC + storage migration concurrent (no conflict)
- [ ] Multiple admins minting tokens concurrently (no race condition on key)
- [ ] Concurrent hold set + GC (hold prevents deletion)
- [ ] Concurrent hold release + GC at same time (GC sees released hold correctly)
- [ ] Webhook delivery + GC concurrent (webhook records survive GC)
- [ ] Two processes accessing same database concurrently (SQLite WAL or Postgres isolation)
- [ ] LIFO: last process to write reconstructs correctly under concurrent load
- [ ] Deadlock scenario: two concurrent transactions each locking a different resource, verify timeout

## 14. Time based and expiration

- [ ] Token minted t=0, used at t=1 (valid)
- [ ] Token minted t=0, used at t=expiration-1s (valid)
- [ ] Token minted t=0, used at t=expiration (valid or invalid at boundary)
- [ ] Token minted t=0, used at t=expiration+1s (invalid)
- [ ] Token with iat (issued at) in the future (rejected)
- [ ] Token with nbf (not before) in the future (rejected)
- [ ] Token with nbf in the past (valid)
- [ ] Token with iat and nbf both present (valid if now >= nbf)
- [ ] OCI upload session TTL expires mid upload (rejected)
- [ ] OCI upload session renewed before expiry (continues)
- [ ] Retention hold TTL expires, GC deletes on next sweep
- [ ] Retention hold TTL not yet expired, GC skips
- [ ] Clock skew between server and client (tolerance window)
- [ ] JWKS keys expire (background refresh picks up new keys)
- [ ] Migrations with timestamps out of order (ordered by version, not time)

## 15. Network and transport errors

- [ ] Connection reset during upload (client retries)
- [ ] Connection reset during download (client retries)
- [ ] Half open connection during long upload
- [ ] Slow client (slow loris) during upload (timeout)
- [ ] Slow client during download (timeout or stream continues)
- [ ] TLS handshake failure (clear error)
- [ ] DNS resolution failure (error)
- [ ] HTTP/2 connection multiplexing
- [ ] Chunked transfer encoding upload
- [ ] Connection reuse (keep-alive)
- [ ] Proxy configuration (HTTP proxy)
- [ ] Load balancer health check passes
- [ ] Backend service returns HTTP 503 (retry or fallback)
- [ ] Backend service returns HTTP 429 (rate limit, backoff)

## 16. Disk and IO errors

- [ ] Disk full during chunk write (error, clean rollback)
- [ ] Disk full during database write (error, database consistency)
- [ ] Permission denied during write (error)
- [ ] Read only filesystem (error on write)
- [ ] Disk IO error during read (retry)
- [ ] Disk IO error during write (retry or error)
- [ ] Directory does not exist (created or error)
- [ ] File already exists (overwrite or error)
- [ ] Max file descriptors reached (error)
- [ ] Storage directory deleted while running (error, graceful degradation)

## 17. Database errors

- [ ] Connection pool exhausted (block or reject)
- [ ] Connection timeout (retry or error)
- [ ] Query timeout (error)
- [ ] Deadlock detected (retry or error)
- [ ] Unique constraint violation (error or no op)
- [ ] Foreign key constraint violation (error)
- [ ] Serialization failure (retry)
- [ ] Database server restart mid transaction (reconnect and retry)
- [ ] Migration fails mid way (rollback)
- [ ] Migration with incompatible SQL syntax (error)
- [ ] Concurrent schema migration (one wins, other errors)
- [ ] Row lock timeout

## 18. Resource exhaustion

- [ ] Memory limit: 1000 concurrent large uploads (server stays up)
- [ ] Memory limit: reconstruct 10GB file (streams, not buffered)
- [ ] File descriptor limit: 1000 concurrent connections (accepted or queued)
- [ ] Thread pool saturation: 1000 concurrent requests (queued or backpressure)
- [ ] Connection pool saturation: 1000 concurrent requests (queued or error)
- [ ] Disk quota exceeded (error, does not corrupt existing data)
- [ ] Inode exhaustion (error)
- [ ] CPU saturation under load (requests still complete, maybe slower)
- [ ] Network bandwidth saturation (backpressure applied)

## 19. Provider event flows

- [ ] Webhook delivery recorded (database insert)
- [ ] Webhook delivery replay (same delivery ID, no op)
- [ ] Webhook delivery with different content for same ID (error)
- [ ] Failed webhook delivery (recorded as failure, can retry)
- [ ] Repository state observed after file access
- [ ] Repository state observed after file push
- [ ] Repository state with revision reference
- [ ] Repository rename migrates state to new scope
- [ ] Repository rename with conflicting target (rejected)
- [ ] Repository rename with missing source (error)
- [ ] Repository delete clears state (or marks as deleted)
- [ ] Repository delete creates retention hold
- [ ] Repository delete with existing retention hold (merged or error)
- [ ] Duplicate webhook delivery across repositories (no collision)
- [ ] Legacy webhook format migration (old -> new)
- [ ] Webhook delivery order preserved
- [ ] Webhook with HMAC signature (valid)
- [ ] Webhook with HMAC signature (invalid, rejected)
- [ ] Webhook with missing signature (rejected if required)
- [ ] Webhook for unknown repository (rejected)
- [ ] Webhook payload exceeds max size (rejected)

## 20. Provider state transitions

- [ ] Initial state (no access, no push)
- [ ] After access (last_access_changed_at set)
- [ ] After push (last_revision_pushed_at set)
- [ ] After access then push (both timestamps set)
- [ ] After push then access (both timestamps set, access may be newer)
- [ ] After rename (scope updated, timestamps preserved)
- [ ] After delete (state cleared or marked)
- [ ] After delete then re create (fresh state)
- [ ] After GC with state (state preserved)
- [ ] After rebuild (state preserved)
- [ ] Multiple pushes (last revision updated)
- [ ] Push with same revision (no change or update timestamp)

## 21. GC edge cases (detailed)

- [ ] GC on empty storage (no op)
- [ ] GC on storage with only unreferenced chunks (all marked for quarantine)
- [ ] GC on storage with only referenced chunks (none marked)
- [ ] GC on storage with some referenced, some not (only unreferenced marked)
- [ ] GC mark creates quarantine file with correct format
- [ ] GC mark does not modify storage chunks (read only)
- [ ] GC sweep deletes quarantine candidates after TTL
- [ ] GC sweep does not delete candidates before TTL
- [ ] GC sweep without prior mark (no op, empty quarantine list)
- [ ] GC mark without sweep (quarantine file remains on disk)
- [ ] GC dry run, no quarantine files created (report only)
- [ ] GC dry run with retention hold (reports held objects)
- [ ] GC with mixed retention holds (some held, some not, only non held marked)
- [ ] GC with 0 second retention (immediate sweep on next run)
- [ ] GC mark and sweep in sequence (full cycle)
- [ ] GC stale dedupe mapping does not keep orphan shard alive
- [ ] GC after rebuild (no false positives, rebuild fixes index)
- [ ] GC after lifecycle repair (no false positives)
- [ ] GC fails closed on corrupt quarantine metadata file
- [ ] GC fails closed on missing quarantine object metadata
- [ ] GC fails closed on active hold quarantine conflict
- [ ] GC with quarantine file in legacy format (migrates or reads both)
- [ ] GC with multiple quarantine files
- [ ] GC concurrent with upload (chunk uploaded after mark, before sweep, not deleted)
- [ ] GC concurrent with reconstruct (chunk read during sweep, not deleted)
- [ ] GC after restore from backup (consistent)

## 22. Rebuild edge cases (detailed)

- [ ] Rebuild from empty storage (no op)
- [ ] Rebuild with all records intact (no changes)
- [ ] Rebuild with missing reconstruction rows (restored from version records)
- [ ] Rebuild with missing version records (prunes stale latest)
- [ ] Rebuild with corrupt version record JSON (reported, skipped)
- [ ] Rebuild with missing latest record (restored from version records)
- [ ] Rebuild with multiple latest records (fixed, keeps one)
- [ ] Rebuild with repository scoped records (preserved)
- [ ] Rebuild with orphan chunks on storage (indexed if referenced, reported if not)
- [ ] Rebuild reaches fixed point (second run is no op, no warnings)
- [ ] Rebuild after partial GC (some records deleted but chunks still exist)
- [ ] Rebuild after full GC (empty storage, empty rebuild)
- [ ] Rebuild with large database (performance, pagination)
- [ ] Rebuild abort mid way (database left in consistent state)
- [ ] Rebuild concurrent with upload (new uploads are indexed)

## 23. FSCK edge cases (detailed)

- [ ] FSCK on clean storage (passes, no errors)
- [ ] FSCK with corrupt chunk (hash mismatch, detected)
- [ ] FSCK with missing chunk (storage record without file, reported)
- [ ] FSCK with extra chunk on storage (not in index, reported or indexed)
- [ ] FSCK with corrupt database (reported)
- [ ] FSCK with corrupt webhook delivery metadata (reported)
- [ ] FSCK with corrupt quarantine metadata (reported)
- [ ] FSCK with storage migration in progress (reported)
- [ ] FSCK with large storage (performance, memory bounded)
- [ ] FSCK on S3 storage (all checks work over S3)
- [ ] FSCK read only (does not mutate state)
- [ ] FSCK with reconstructed file (validates full reconstruction)
- [ ] FSCK with partial reconstruct (chunk by chunk verify)

## 24. Storage migration edge cases (detailed)

- [ ] Migration from local to S3 (all chunks copied)
- [ ] Migration from S3 to local (all chunks copied)
- [ ] Migration with already migrated (idempotent, no op)
- [ ] Migration with some already migrated (skips duplicated)
- [ ] Migration with corrupt source chunk (reported, migration continues or fails)
- [ ] Migration with missing source chunk (reported, migration continues)
- [ ] Migration abort mid way (partial state, restart continues)
- [ ] Migration concurrent with upload (new chunks also migrated)
- [ ] Migration concurrent with GC (no conflict)
- [ ] Migration concurrent with reconstruct (reconstruct uses migration state)
- [ ] Migration with symlinked local root (rejected)
- [ ] Migration with S3 bucket that does not exist (created or error)
- [ ] Migration from local to S3 with allow-http false and HTTP endpoint (rejected)
- [ ] Migration with network failure during transfer (retry)
- [ ] Migration with source being migrated again (already in progress, error)

## 25. Database migration edge cases

- [ ] Migration from empty database (creates schema version 1)
- [ ] Migration from current version (no op)
- [ ] Migration up N versions (applies N migrations)
- [ ] Migration down N versions (reverts N migrations)
- [ ] Migration up with invalid SQL (error, no partial apply)
- [ ] Migration down with invalid SQL (error, no partial apply)
- [ ] Migration with zero steps specified (no op)
- [ ] Migration with more steps than available (applies all available)
- [ ] Migration status (reports current version and pending)
- [ ] Migration rollback on failure (consistent state)
- [ ] Migration lock (prevent concurrent migration)
- [ ] Migration timeout (long running migration cancelled)
- [ ] Migration file not found (error)
- [ ] Migration with duplicate version numbers (error)

## 26. Token lifecycle

- [ ] Create token (signed by local Ed25519)
- [ ] Create token with all claim fields
- [ ] Create token with minimal claims
- [ ] Create token with repository scope
- [ ] Create token with no expiration
- [ ] Validate token immediately (valid)
- [ ] Validate token near expiration (boundary)
- [ ] Validate token after expiration (rejected)
- [ ] Validate token with wrong signing key (rejected)
- [ ] Validate malformed token (no signature, rejected)
- [ ] Validate token with tampered payload (rejected)
- [ ] Validate token with extra fields (ignored or rejected)
- [ ] Validate token with missing required fields (rejected)
- [ ] Rotate signing key, validate old token (works if old key retained)
- [ ] Rotate signing key, validate new token with new key (works)
- [ ] Revoke token (if supported, else token valid until expiry)

## 27. Request validation edge cases

- [ ] Missing Content-Type header (accepted with default)
- [ ] Invalid Content-Type header (accepted or rejected per endpoint)
- [ ] Missing Content-Length header (chunked encoding, accepted)
- [ ] Content-Length mismatch (header vs actual body, rejected)
- [ ] Transfer-Encoding: chunked (accepted)
- [ ] Transfer-Encoding: identity (accepted)
- [ ] Invalid HTTP method (405)
- [ ] HTTP method not allowed for route (405)
- [ ] OPTIONS request to any route (CORS or 200)
- [ ] HEAD request to any route (same headers as GET, no body)
- [ ] Request to non existent route (404)
- [ ] Request to versioned path (v1/ vs v2/ etc.)
- [ ] Request with query parameters (ignored or processed)
- [ ] Request with fragment (#) in URL (ignored per HTTP spec)
- [ ] Duplicate query parameters (last wins or first wins)
- [ ] URL encoded path segments (decoded before processing)

## 28. Chunk and hash validation

- [ ] Upload chunk with correct hash (accepted)
- [ ] Upload chunk with incorrect hash (rejected)
- [ ] Upload chunk with hash from wrong algorithm (rejected)
- [ ] Upload chunk with hash too short (rejected)
- [ ] Upload chunk with hash too long (rejected)
- [ ] Upload chunk with non hex hash (rejected)
- [ ] Upload chunk with uppercase hash (accepted or normalized)
- [ ] Reconstruct file, verify hash matches original
- [ ] Reconstruct file with corrupt chunk (hash mismatch reported)
- [ ] Reconstruct file with missing chunk (error)
- [ ] Reconstruct file with wrong chunk order (hash mismatch)
- [ ] Partial chunk upload (incomplete, detected)
- [ ] Chunk compression round trip (compress, decompress, verify bytes)

## 29. Storage backend abstraction

- [ ] Local backend implements all Store traits (compile check)
- [ ] S3 backend implements all Store traits (compile check)
- [ ] Local and S3 produce identical results for same input
- [ ] Backend switch from local to S3 (migration path)
- [ ] Backend switch from S3 to local (migration path)
- [ ] Custom backend implementer compiles (trait API is stable)

## 30. Observability

- [ ] Server start log line includes version
- [ ] Request log line includes method, path, status, duration
- [ ] Request log line includes trace ID or request ID
- [ ] Error log line includes error details (without stack trace in production)
- [ ] Metrics endpoint returns prometheus text format
- [ ] Metrics registered for each operation (uploads, downloads, GC, auth)
- [ ] Metrics include histograms for latency
- [ ] Metrics include counters for errors
- [ ] Metrics include gauges for active operations
- [ ] Log level respects env var (SHARDLINE_LOG or RUST_LOG)
- [ ] Structured logging (JSON format if configured)
- [ ] Log output goes to stderr by default (not stdout)

## 31. CLI exit codes and signals

- [ ] `shardline serve` starts, receives SIGTERM, exits with code 0
- [ ] `shardline serve` starts, receives SIGINT, exits with code 0
- [ ] `shardline serve` with invalid config, exits with non zero before binding
- [ ] `shardline gc` with invalid argument, exits with non zero
- [ ] `shardline fsck` with corrupt storage, exits with non zero
- [ ] `shardline fsck` with clean storage, exits with 0
- [ ] `shardline admin token` succeeds, exits with 0, prints token to stdout
- [ ] `shardline admin token` fails, exits with non zero, prints error to stderr
- [ ] All CLI commands return appropriate exit code
- [ ] Piped output (stdout to file) succeeds
- [ ] Error output goes to stderr (can be redirected separately)

## 32. API versioning

- [ ] v1 endpoints work
- [ ] v2 endpoints work (if they exist)
- [ ] Unversioned endpoints work
- [ ] Future version returns 404 or 410 (not silent success)
- [ ] Deprecated endpoint returns warning header

## 33. Cross protocol content identity

- [ ] Upload file via Xet, download via LFS (same content hash or bytes)
- [ ] Upload file via LFS, download via OCI blob (same bytes, different format)
- [ ] Upload via Bazel CAS, download via HTTP (same bytes)
- [ ] Upload via Hub, download via Xet (same bytes)
- [ ] Content hash is consistent across all frontends for same bytes
- [ ] Reconstruction is consistent across all frontends

## 34. Performance and stress

- [ ] Upload 1GB file (sequential, measure throughput)
- [ ] Download 1GB file (sequential, measure throughput)
- [ ] Upload 1000 files of 1MB each (batch throughput)
- [ ] 100 concurrent clients uploading 10MB each (concurrent throughput)
- [ ] Reconstruct 1000 files in parallel (reconstruction throughput)
- [ ] GC with 10000 chunks (mark and sweep time)
- [ ] Rebuild with 10000 records (rebuild time)
- [ ] FSCK with 10000 chunks (fsck time)
- [ ] Token validation throughput (tokens/second)
- [ ] Memory usage under load (no leak, bounded)
- [ ] File descriptor usage under load (no leak, bounded)
- [ ] CPU usage under load (reasonable)
- [ ] Startup time (cold start, warm start)
- [ ] Long running test (24h, no memory leak, no crash)

## 35. Upgrade and downgrade

- [ ] Upgrade from current version to next (database migration)
- [ ] Downgrade from next to current (database migration down)
- [ ] Upgrade binary without database change (no op)
- [ ] Upgrade with new config option (default value used)
- [ ] Upgrade with removed config option (ignored or error)
- [ ] Rolling upgrade with mixed versions (compatibility)
- [ ] Snapshot before upgrade, restore after failure (data preserved)

## 36. OCI route dispatch and internal functions

### OCI dispatch
- [ ] `GET /v2/` returns Docker-Distribution-API-Version header
- [ ] `GET /v2/` returns 200
- [ ] OCI dispatch with `ServerRole::All` serves both API and transfer routes
- [ ] OCI dispatch with `ServerRole::Api` serves API routes only (token, tags, manifests)
- [ ] OCI dispatch with `ServerRole::Api` rejects blob upload/download (404)
- [ ] OCI dispatch with `ServerRole::Transfer` serves transfer routes only (blobs)
- [ ] OCI dispatch with `ServerRole::Transfer` rejects manifest operations (404)
- [ ] OCI dispatch with arbitrary method+path combo (returns 404)

### OCI path parsing
- [ ] `parse_oci_path` with valid blob path
- [ ] `parse_oci_path` with valid manifest path
- [ ] `parse_oci_path` with valid tags list path
- [ ] `parse_oci_path` with valid token path
- [ ] `parse_oci_path` with unknown path (returns 404)
- [ ] `parse_oci_path` with path traversal attempt (rejected)
- [ ] `parse_oci_path` with missing repository name

### OCI registry token
- [ ] `GET /v2/token` with Basic auth (valid credentials, returns token)
- [ ] `GET /v2/token` with Bearer auth (valid token, returns new token)
- [ ] `GET /v2/token` without auth (returns challenge)
- [ ] `GET /v2/token` with invalid Basic auth (401)
- [ ] `GET /v2/token` with invalid scope format (400)
- [ ] `GET /v2/token` with unknown service (400)
- [ ] `GET /v2/token` with duplicate service param (400)
- [ ] `GET /v2/token` with too many scope params (400)
- [ ] `GET /v2/token` with scope param too long (400)
- [ ] `GET /v2/token` returns `Bearer realm` challenge header format
- [ ] Token TTL clamping (min/max enforcement)
- [ ] Rate limit: `--oci-registry-token-max-in-flight-requests=5` blocks 6th request
- [ ] Bootstrap credentials: Basic auth decode, UTF-8 validation, empty password check

### OCI blob upload (detailed)
- [ ] `POST /v2/{name}/blobs/uploads/` with digest (monolithic upload)
- [ ] `POST /v2/{name}/blobs/uploads/` with mount (cross-repo mount)
- [ ] `POST /v2/{name}/blobs/uploads/` with mount and source not found (falls back to new session)
- [ ] `PATCH /v2/{name}/blobs/uploads/{session}` with Content-Range header (valid)
- [ ] `PATCH /v2/{name}/blobs/uploads/{session}` with Content-Range start mismatch (400)
- [ ] `PATCH /v2/{name}/blobs/uploads/{session}` with Content-Range integrity failure
- [ ] `PATCH /v2/{name}/blobs/uploads/{session}` without Content-Range (chunked)
- [ ] `PUT /v2/{name}/blobs/uploads/{session}?digest=sha256:...` (finalize)
- [ ] `PUT /v2/{name}/blobs/uploads/{session}` without digest query param (400)
- [ ] `PUT /v2/{name}/blobs/uploads/{session}` with digest mismatch (400)
- [ ] `GET /v2/{name}/blobs/uploads/{session}` (session status)
- [ ] `GET /v2/{name}/blobs/uploads/{session}` with session repo/scope mismatch (404)
- [ ] `DELETE /v2/{name}/blobs/uploads/{session}` (cancel upload)
- [ ] Upload session TTL expiry mid-upload (session invalidated)
- [ ] Upload session max active limit reached (429)

### OCI manifest (detailed)
- [ ] `GET /v2/{name}/manifests/{reference}` with Accept header negotiation
- [ ] `GET /v2/{name}/manifests/{reference}` with Accept: */* (returns manifest)
- [ ] `GET /v2/{name}/manifests/{reference}` with Accept: application/vnd.docker.distribution.manifest.v2+json
- [ ] `GET /v2/{name}/manifests/{reference}` with unsupported Accept type (406 or fallback)
- [ ] `PUT /v2/{name}/manifests/{reference}` with valid manifest
- [ ] `PUT /v2/{name}/manifests/{reference}` with digest=reference mismatch (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with missing Content-Type (defaults to OCI)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-existent config blob (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-existent layer blob (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-existent subject manifest (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-existent nested manifest (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with schemaVersion != 2 (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with unsupported media type (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with tag too long (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` as image index (multi-arch)
- [ ] `PUT /v2/{name}/manifests/{reference}` with missing config field (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with missing layers field (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-array layers (400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with missing manifests field (index, 400)
- [ ] `PUT /v2/{name}/manifests/{reference}` with non-array manifests (400)
- [ ] `DELETE /v2/{name}/manifests/{reference}` by digest
- [ ] `DELETE /v2/{name}/manifests/{reference}` by tag (rejected, must use digest)
- [ ] `DELETE /v2/{name}/manifests/{reference}` non-existent (404)
- [ ] Tag resolution: tag -> digest -> manifest (valid)
- [ ] Tag resolution: tag points to non-existent digest (404)
- [ ] Referrers API: (NOTE: not implemented in code, do not test)

### OCI tags (detailed)
- [ ] `GET /v2/{name}/tags/list` with no pagination (returns all)
- [ ] `GET /v2/{name}/tags/list` with n=1 (pagination, returns one)
- [ ] `GET /v2/{name}/tags/list` with n=0 (rejected)
- [ ] `GET /v2/{name}/tags/list` with n exceeding max (clamped)
- [ ] `GET /v2/{name}/tags/list` with last=tag (returns tags after)
- [ ] `GET /v2/{name}/tags/list` with invalid last tag (400)
- [ ] `GET /v2/{name}/tags/list` with non-numeric n (400)
- [ ] Tags list returns Link header on pagination
- [ ] Tag update: push manifest with new tag, verify tag points to new digest
- [ ] Tag update: push manifest with same tag, old digest cleaned up
- [ ] Tag delete: manifest deleted, tag removed from listing
- [ ] Tag delete: only tag for digest deleted (other tags preserved)
- [ ] Empty repository tag listing (empty)

## 37. OCI manifest validation (internal)

- [ ] `schemaVersion` must be 2 (rejects 1 or 3)
- [ ] `mediaType` must match Content-Type header
- [ ] `mediaType` mismatch returns 400
- [ ] Unsupported `mediaType` returns 400
- [ ] `config` must exist for image manifest
- [ ] `config.digest` must point to existing blob
- [ ] `layers` must be non-empty array
- [ ] Each layer `digest` must point to existing blob
- [ ] `manifests` must be non-empty array for image index
- [ ] Each manifest entry `digest` must point to existing manifest
- [ ] Each descriptor must have `digest` (string), `size` (integer), `mediaType` (string)
- [ ] Manifest media type normalization: `application/json; charset=utf-8` -> `application/json`
- [ ] Subject manifest validation: referenced manifest must exist
- [ ] OCI descriptor validation: missing fields rejected
- [ ] Blob existence check: existing blob passes
- [ ] Blob existence check: missing blob rejected
- [ ] Manifest existence check: existing manifest passes
- [ ] Manifest existence check: missing manifest rejected

## 38. Provider routes (app/provider_routes.rs)

- [ ] `POST /v1/providers/{provider}/tokens` with valid provider token request
- [ ] `POST /v1/providers/{provider}/tokens` with missing provider API key (401)
- [ ] `POST /v1/providers/{provider}/tokens` with invalid provider API key (403)
- [ ] `POST /v1/providers/{provider}/tokens` with missing provider subject (401)
- [ ] `POST /v1/providers/{provider}/tokens` with invalid provider token request (400)
- [ ] `POST /v1/providers/{provider}/tokens` with disabled provider tokens (404)
- [ ] `POST /v1/providers/{provider}/tokens` with unknown provider (404)
- [ ] `POST /v1/providers/{provider}/tokens` with oversized request body (413)
- [ ] `POST /v1/providers/{provider}/git-lfs-authenticate` with valid request
- [ ] `POST /v1/providers/{provider}/git-lfs-authenticate` with invalid provider
- [ ] `POST /v1/providers/{provider}/webhooks` with valid HMAC signature
- [ ] `POST /v1/providers/{provider}/webhooks` with invalid HMAC signature (403)
- [ ] `POST /v1/providers/{provider}/webhooks` with missing webhook auth (401)
- [ ] `POST /v1/providers/{provider}/webhooks` with invalid payload (400)
- [ ] `POST /v1/providers/{provider}/webhooks` with oversized payload (413)
- [ ] `GET /v1/stats` returns valid stats
- [ ] `GET /v1/stats` without auth (if public)

## 39. Git Smart HTTP routes (hub_api)

- [ ] `GET /{type}/{ns}/{repo}/info/refs` with valid repository (returns refs)
- [ ] `GET /{type}/{ns}/{repo}/info/refs` with non existent repository
- [ ] `GET /{type}/{ns}/{repo}/info/refs` with unknown service name (404)
- [ ] `HEAD /{type}/{ns}/{repo}/HEAD` with valid repository
- [ ] `HEAD /{type}/{ns}/{repo}/HEAD` with non existent repository
- [ ] `POST /{type}/{ns}/{repo}/git-upload-pack` (clone/fetch)
- [ ] `POST /{type}/{ns}/{repo}/git-upload-pack` with empty refs path (empty pack)
- [ ] `POST /{type}/{ns}/{repo}/git-upload-pack` with invalid pack data
- [ ] `POST /{type}/{ns}/{repo}/git-receive-pack` (push)
- [ ] `POST /{type}/{ns}/{repo}/git-receive-pack` with empty updates (no-op report)
- [ ] `POST /{type}/{ns}/{repo}/git-receive-pack` with invalid reference
- [ ] Git push creates entries in storage and index
- [ ] Git clone fetches entries from storage and index
- [ ] Git pull (fetch + merge) reconstructs correctly
- [ ] Git smart HTTP with authentication (valid token)
- [ ] Git smart HTTP without authentication (401)

### Git ref validation
- [ ] `is_valid_refname` rejects empty string
- [ ] `is_valid_refname` rejects control characters (< 0x20)
- [ ] `is_valid_refname` rejects DEL (0x7f)
- [ ] `is_valid_refname` rejects non-refs/ prefix
- [ ] `is_valid_refname` accepts `refs/heads/main`
- [ ] `is_valid_refname` accepts `refs/tags/v1.0.0`

### Git pack parsing
- [ ] `parse_pack_data` with valid pack header ("PACK" magic + version)
- [ ] `parse_pack_data` with invalid magic bytes (rejected)
- [ ] `parse_pack_data` with wrong version (rejected)
- [ ] `parse_pack_data` with truncated data (error)
- [ ] `parse_pack_data` with shift overflow (rejected, PackError::ShiftOverflow)
- [ ] `parse_pack_data` with OFS_DELTA object type (skipped or resolved)
- [ ] `parse_pack_data` with REF_DELTA object type (skipped or resolved)
- [ ] `decompress_zlib` with valid compressed data
- [ ] `decompress_zlib` with decompression bomb >512 MB (rejected)
- [ ] `decompress_zlib` with corrupt compressed data (error)
- [ ] `pack_error` variants map to correct HTTP status codes

### Git pktline
- [ ] `encode_line` with valid payload
- [ ] `encode_line` with payload exceeding 65516 bytes (PayloadTooLarge)
- [ ] `encode_line_bytes` with same limit
- [ ] `decode_lines` with valid hex length prefix
- [ ] `decode_lines` with non-hex prefix (error)
- [ ] `decode_lines` with length < 4 (error)
- [ ] `decode_lines` with position overflow (error)
- [ ] `sideband_data` chunks at 65516 boundary
- [ ] `decode_sideband` with known channel byte (1=stdout, 2=stderr, 3=progress)
- [ ] `decode_sideband` with unknown channel byte (error)
- [ ] `decode_sideband` with non-UTF-8 on channel 2/3 (error)
- [ ] Flush packet (0000) terminates sequence
- [ ] Delimiting packet decodes correctly

### Git object model
- [ ] `GitObject` round-trip (serialize, deserialize)
- [ ] `ObjectType` variants (commit, tree, blob, tag)
- [ ] `generate_pack` with valid objects
- [ ] `generate_pack` with zero objects (empty pack)
- [ ] `generate_pack` with objects exceeding u32::MAX (TooManyObjects)
- [ ] `write_object` with zlib compression
- [ ] `write_object` with zlib compression failure (error)
- [ ] `create_commit_object` produces valid commit
- [ ] `empty_pack` produces valid empty pack data
- [ ] Git tree building: files at root
- [ ] Git tree building: files in subdirectories
- [ ] Git tree building: deeply nested directories
- [ ] Git tree building: path traversal in filenames
- [ ] Git tree entries: mixed LFS and non-LFS files
- [ ] `build_lfs_pointer_blob` produces correct format
- [ ] `build_gitattributes_blob` with LFS files (present)
- [ ] `build_gitattributes_blob` without LFS files (none, returns None)
- [ ] `build_inline_blob` content hash is deterministic

## 40. Hub API routes (detailed)

- [ ] `GET /api/whoami-v2` returns current user
- [ ] `POST /api/repos/create` creates repository
- [ ] `GET /api/repos` lists repositories
- [ ] `GET /api/repos` with pagination
- [ ] `GET /api/{type}/search?search=query` returns results
- [ ] `GET /api/{type}/{ns}/{repo}` returns repository info
- [ ] `GET /api/{type}/{ns}/{repo}/modelcard` returns model card
- [ ] `GET /api/{type}/{ns}/{repo}/revisions` lists revisions
- [ ] `POST /api/{type}/{ns}/{repo}/preupload/{rev}` returns upload URLs
- [ ] `POST /api/{type}/{ns}/{repo}/commit/{rev}` commits files
- [ ] `GET /api/{type}/{ns}/{repo}/tree/{rev}/{*path}` returns file tree
- [ ] `GET /api/datasets/{ns}/{repo}/parquet` returns parquet data
- [ ] `GET /api/datasets/{ns}/{repo}/parquet` with CSV file (falls through)
- [ ] `GET /api/datasets/{ns}/{repo}/parquet` with JSONL file (falls through)
- [ ] `GET /api/datasets/{ns}/{repo}/parquet` with unsupported format (error)
- [ ] Dataset viewer with pagination (offset + rows)
- [ ] Dataset viewer with data capped at 10000 rows
- [ ] `POST /api/collections/{collection}` creates or updates collection
- [ ] `GET /api/collections` lists collections
- [ ] `GET /api/user/profile` returns user profile
- [ ] Hub API with auth token (valid)
- [ ] Hub API without auth token (401)
- [ ] Hub API with expired token (401)
- [ ] Hub API with token for wrong scope (403)
- [ ] All Hub API routes return correct HTTP status codes

### Hub API LFS
- [ ] `POST /objects/batch` with `upload` operation
- [ ] `POST /objects/batch` with `download` operation
- [ ] `POST /objects/batch` with `verify` operation
- [ ] `POST /objects/batch` with invalid OID (rejected)
- [ ] `PUT /lfs/objects/{oid}` (upload)
- [ ] `GET /lfs/objects/{oid}` (download, found)
- [ ] `GET /lfs/objects/{oid}` (not found, 404)

### Hub API webhooks
- [ ] Webhook create with valid URL
- [ ] Webhook create with invalid URL (SSRF validation)
- [ ] Webhook create with too many events (rejected)
- [ ] Webhook create for non-existent repo (404)
- [ ] Webhook list for repository
- [ ] Webhook delete
- [ ] Webhook validation: private IP URL rejected (127.0.0.1, 10.x.x.x, 172.16.x.x, 192.168.x.x, 169.254.x.x, ::1, 100.64.x.x)
- [ ] Webhook validation: public IP URL accepted
- [ ] Webhook URL sanitization (control characters replaced)

### Hub API Xet tokens
- [ ] `GET /api/{type}/{ns}/{repo}/xet/read` with auth (returns token)
- [ ] `GET /api/{type}/{ns}/{repo}/xet/read` without auth (401)
- [ ] `GET /api/{type}/{ns}/{repo}/xet/write` with auth (returns token)
- [ ] `GET /api/{type}/{ns}/{repo}/xet/write` without auth (401)

### Hub API dataset parsing internals
- [ ] `find_dataset_file` with .parquet file
- [ ] `find_dataset_file` with .csv file
- [ ] `find_dataset_file` with .jsonl file
- [ ] `find_dataset_file` with no matching files (error)
- [ ] `parse_rows_from_content` with valid UTF-8
- [ ] `parse_rows_from_content` with invalid UTF-8 (error)
- [ ] `parse_rows_from_content` with unsupported format (error)
- [ ] `parse_jsonl_rows` with valid JSONL
- [ ] `parse_jsonl_rows` with empty lines (skipped)
- [ ] `parse_jsonl_rows` with non-object JSON values (skipped or error)
- [ ] `parse_jsonl_rows` with offset and limit enforcement
- [ ] `parse_csv_rows` with valid CSV
- [ ] `parse_csv_rows` with header line only (no data rows)
- [ ] `parse_csv_rows` with empty lines after header (skipped)
- [ ] `parse_csv_rows` with quoted fields
- [ ] `parse_csv_rows` with escaped quotes
- [ ] `parse_csv_line` with unterminated quotes (error)
- [ ] `parse_csv_line` with trailing comma (empty field)
- [ ] `parse_csv_line` with leading comma (empty field)
- [ ] `parse_csv_line` with consecutive commas (empty fields)

## 41. Webhook URL validation (SSRF)

- [ ] `validate_webhook_url` with valid HTTPS URL (passes)
- [ ] `validate_webhook_url` with non-HTTPS scheme (rejected)
- [ ] `validate_webhook_url` with URL exceeding max length (rejected)
- [ ] `validate_webhook_url` with no host (rejected)
- [ ] `validate_webhook_url` with IPv6 address (accepted or rejected based on config)
- [ ] `validate_webhook_url` with localhost (rejected)
- [ ] `validate_webhook_url` with 127.0.0.1 (rejected)
- [ ] `validate_webhook_url` with 10.x.x.x (rejected)
- [ ] `validate_webhook_url` with 172.16-31.x.x (rejected)
- [ ] `validate_webhook_url` with 192.168.x.x (rejected)
- [ ] `validate_webhook_url` with 169.254.x.x (rejected)
- [ ] `validate_webhook_url` with ::1 (rejected)
- [ ] `validate_webhook_url` with 100.64-127.x.x (CGNAT, rejected)
- [ ] `validate_webhook_url` with legitimate external URL (accepted)
- [ ] `sanitize_log_url` replaces control characters
- [ ] `sanitize_log_url` truncates long URLs
- [ ] `is_private_ip` with CGNAT range (100.64.0.0/10)
- [ ] `is_private_ip` with IPv6 link-local (fe80::)
- [ ] `is_private_ip` with documentation addresses (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24)

## 42. Missing config env vars

- [ ] `SHARDLINE_BIND_ADDR=0.0.0.0:8080` (default)
- [ ] `SHARDLINE_BIND_ADDR=127.0.0.1:9090` (custom)
- [ ] `SHARDLINE_BIND_ADDR=invalid` (rejected)
- [ ] `SHARDLINE_PUBLIC_BASE_URL=https://assets.example.com` (custom base URL)
- [ ] `SHARDLINE_PUBLIC_BASE_URL=not-a-url` (rejected, InvalidPublicBaseUrl)
- [ ] `SHARDLINE_SERVER_ROLE=all` (default)
- [ ] `SHARDLINE_SERVER_ROLE=api` (API only)
- [ ] `SHARDLINE_SERVER_ROLE=transfer` (transfer only)
- [ ] `SHARDLINE_SERVER_ROLE=invalid` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_FILES=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_FILES=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_XORBS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_XORBS=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_XORB_CHUNKS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_XORB_CHUNKS=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory` (default)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis` with Redis URL
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis` without Redis URL (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=invalid` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=60`
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=100`
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://localhost:6379` (valid)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=` (empty, rejected if redis adapter)
- [ ] `SHARDLINE_INDEX_POSTGRES_URL=postgres://user:pass@localhost/db` (valid)
- [ ] `SHARDLINE_INDEX_POSTGRES_URL=` (empty, falls back to local SQLite)
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE=/path/to/providers.json`
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE` with invalid JSON (rejected)
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE` with missing file (rejected)
- [ ] `SHARDLINE_PROVIDER_API_KEY_FILE=/path/to/key`
- [ ] `SHARDLINE_PROVIDER_API_KEY_FILE` with invalid key (rejected)
- [ ] `SHARDLINE_PROVIDER_TOKEN_ISSUER=example-issuer`
- [ ] `SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS=3600`
- [ ] `SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_METRICS_TOKEN_FILE=/path/to/metrics-token`
- [ ] `SHARDLINE_S3_REGION=us-west-2` (custom region)
- [ ] `SHARDLINE_S3_KEY_PREFIX=shardline/` (prefix all keys)
- [ ] `SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=true`
- [ ] `SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=false`
- [ ] `SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=invalid` (rejected, InvalidS3VirtualHostedStyleRequest)
- [ ] `SHARDLINE_S3_ALLOW_HTTP=invalid` (rejected, InvalidS3AllowHttp)
- [ ] `SHARDLINE_S3_ACCESS_KEY_ID_FILE=/path/to/access-key`
- [ ] `SHARDLINE_S3_SECRET_ACCESS_KEY_FILE=/path/to/secret-key`
- [ ] `SHARDLINE_S3_SESSION_TOKEN_FILE=/path/to/session-token`
- [ ] `SHARDLINE_S3_ACCESS_KEY_ID` + `SHARDLINE_S3_ACCESS_KEY_ID_FILE` both set (conflict, rejected)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=65536` (64KB chunks)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=1` (minimum, rejected or allowed)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=1073741824` (1GB, rejected if too large)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=0` (rejected)
- [ ] `SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=1` (serial processing)
- [ ] `SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=0` (rejected)
- [ ] `SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=1`
- [ ] `SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=0` (rejected)
- [ ] `SHARDLINE_SERVER_FRONTENDS=` (empty, MissingServerFrontends)
- [ ] `SHARDLINE_SERVER_FRONTENDS=metrics` (BUG: CLI cannot pass --frontend metrics, see section N)
- [ ] `SHARDLINE_AUTH_PROVIDER=invalid` (rejected, InvalidAuthProvider)

## 43. Config builder methods (every `with_` method)

- [ ] `with_root_dir` with writable directory
- [ ] `with_root_dir` with read only directory (error at runtime)
- [ ] `with_root_dir` with symlinked parent (rejected)
- [ ] `with_reconstruction_cache_disabled()` (cache disabled, no impact on correctness)
- [ ] `with_reconstruction_cache_memory(60, 1000)` (memory cache with TTL and capacity)
- [ ] `with_reconstruction_cache_redis(60, &redis_url)` with valid URL
- [ ] `with_reconstruction_cache_redis(60, "")` with empty URL (rejected, EmptyReconstructionCacheRedisUrl)
- [ ] `with_index_postgres_url(url)` (Postgres backend)
- [ ] `with_index_postgres_url("")` (rejected, EmptyIndexPostgresUrl)
- [ ] `with_index_postgres_url` (not called, local SQLite used)
- [ ] `with_auth_oidc_issuer(issuer_url)` (OIDC auth)
- [ ] `with_auth_jwks_url(url)` and `with_auth_jwks_issuer(issuer)` (JWKS auth)
- [ ] `with_metrics_token(token)` (metrics endpoint requires token)
- [ ] `with_metrics_token(empty)` (rejected, EmptyMetricsToken)
- [ ] `with_metrics_token` (not called, metrics public)
- [ ] `with_provider_runtime(config, key, issuer, ttl, signing_key)` (provider integration)
- [ ] `with_provider_runtime` with incomplete config (rejected, IncompleteProviderTokenConfig)
- [ ] Multiple `with_` calls chain correctly (all options set)
- [ ] `with_token_signing_key` with empty key (rejected, EmptyTokenSigningKey)
- [ ] `with_token_signing_key` with key too large (rejected, TokenSigningKeyTooLarge)
- [ ] `with_server_frontends` with empty list (rejected, MissingServerFrontends)

## 44. Config validation runtime checks

- [ ] `validate_runtime_requirements` passes with signing key set
- [ ] `validate_runtime_requirements` fails when serving routes without signing key (MissingTokenSigningKeyForServedRoutes)
- [ ] `resolve_secret_file_path` with regular file (passes)
- [ ] `resolve_secret_file_path` with symlink inside parent directory (passes)
- [ ] `resolve_secret_file_path` with symlink outside parent directory (rejected)
- [ ] `resolve_secret_file_path` with non-regular file (rejected)
- [ ] `resolve_secret_file_path` with file in directory with no parent (rejected)
- [ ] `configure_provider_runtime_from_paths` with both config and key path (passes)
- [ ] `configure_provider_runtime_from_paths` with config path but no key path (rejected)
- [ ] `configure_provider_runtime_from_paths` with key path but no config path (rejected)
- [ ] `configure_provider_runtime_from_paths` with neither path (passes, disabled)
- [ ] `optional_s3_secret_from_sources` with direct env var only (passes)
- [ ] `optional_s3_secret_from_sources` with file env var only (passes)
- [ ] `optional_s3_secret_from_sources` with both direct and file (conflict, rejected)
- [ ] `optional_s3_secret_from_sources` with file too large (rejected)
- [ ] `optional_s3_secret_from_sources` with file length mismatch (rejected)
- [ ] `optional_s3_secret_from_sources` with non-UTF-8 file (rejected)
- [ ] `parse_env_bool` with "true" (passes)
- [ ] `parse_env_bool` with "false" (passes)
- [ ] `parse_env_bool` with "1" (passes or rejected)
- [ ] `parse_env_bool` with "yes" (passes or rejected)
- [ ] `parse_env_bool` with invalid string (rejected)

## 45. HTTP error code coverage (every ServerError variant)

- [ ] 400 Bad Request: `RequestBodyRead`, `RequestBodyFrameOutOfBounds`, `HashParse`, `InvalidFileId`, `InvalidContentHash`, `InvalidXorbPrefix`, `XorbHashMismatch`, `InvalidSerializedXorb`, `InvalidSerializedShard`, `MissingReferencedXorb`, `InvalidRangeHeader`, `InvalidProviderTokenRequest`, `InvalidProviderWebhookPayload`, `ExpectedBodyHashMismatch`, `InvalidDigest`, `InvalidRepositoryName`, `InvalidManifestReference`, `InvalidUploadSession`, `PathValidation`
- [ ] 401 Unauthorized: `MissingAuthorization`, `InvalidAuthorizationHeader`, `InvalidToken` (with sub variants), `MissingProviderApiKey`, `MissingProviderSubject`, `MissingProviderWebhookAuthentication`, `UnauthorizedChallenge`
- [ ] 403 Forbidden: `InsufficientScope`, `InvalidProviderApiKey`, `InvalidProviderWebhookAuthentication`, `ProviderDenied`
- [ ] 404 Not Found: `NotFound`, `ProviderTokensDisabled`, `UnknownProvider`
- [ ] 406 Not Acceptable: `NotAcceptable`
- [ ] 413 Payload Too Large: `RequestBodyTooLarge`, `TooManyShardTerms`, `TooManyBatchReconstructionFileIds`, `StoredFileMetadataTooLarge`
- [ ] 414 URI Too Long: `RequestQueryTooLarge`
- [ ] 416 Range Not Satisfiable: `RangeNotSatisfiable`
- [ ] 429 Too Many Requests: `TooManyUploadSessions`, `TooManyRegistryTokenRequests`
- [ ] 500 Internal Server Error: `Io`, `Json`, `NumericConversion`, `ObjectStore`, `Index`, `StoredFileMetadataLengthMismatch`, `Overflow`, `Config`, `BlockingTask`, `ReconstructionCache`, `Provider`
- [ ] 503 Service Unavailable: `TransferLimiterClosed`
- [ ] Error response body is valid JSON
- [ ] Error response body includes `error` field
- [ ] Error response body does not include stack trace (production mode)
- [ ] Error response body does not include internal paths

## 46. Token error variants (every TokenCodecError)

- [ ] `EmptySigningKey` (rejected at config validation)
- [ ] `SigningKeyTooShort` (rejected at config validation)
- [ ] `Json` error (malformed claim payload, rejected)
- [ ] `InvalidFormat` (token does not match expected format, rejected)
- [ ] `InvalidHex` (signature hex decoding failure, rejected)
- [ ] `InvalidSignature` (signature verification failure, rejected)
- [ ] `Expired` (token past expiration, rejected)
- [ ] `Claims` violation (invalid claims content, rejected)

## 47. S3 runtime errors (every S3ObjectStoreError variant)

- [ ] `IncompleteCredentials` (one of access_key/secret_key set but not both)
- [ ] `EmptyBucket` (bucket name missing)
- [ ] `EmptyRegion` (region missing)
- [ ] `InvalidKeyPrefix` (key prefix invalid format)
- [ ] `IntegrityLengthMismatch` (downloaded chunk length differs from expected)
- [ ] `IntegrityHashMismatch` (downloaded chunk hash differs from expected)
- [ ] `ExistingObjectConflict` (TOCTOU conflict during put-if-absent)
- [ ] `RangeOutOfBounds` (requested range exceeds object size)
- [ ] `InvalidListedKey` (S3 returned an unparseable key)
- [ ] `Io` (underlying IO error)
- [ ] `Path` (path validation error)
- [ ] `Runtime` (S3 SDK runtime error, can recover)
- [ ] `RuntimeUnavailable` (S3 SDK unavailable, cannot recover)
- [ ] S3 client construction with incomplete credentials (fails at startup or runtime)
- [ ] S3 client construction with invalid endpoint (connection error)
- [ ] S3 `put_if_absent` with `AlreadyExists` conflict (uses existing_object_outcome)
- [ ] S3 `put_if_absent` with `Precondition` conflict (uses existing_object_outcome)
- [ ] S3 `put_if_absent` byte-level comparison when conflicts detected
- [ ] S3 `read_range` with `None` range length (reads to end)
- [ ] S3 `read_range` with returned bytes mismatch (error)
- [ ] S3 `metadata` for existing object (returns metadata)
- [ ] S3 `metadata` for missing object (returns Ok(None))
- [ ] S3 `list_prefix` with sort-by-key behavior
- [ ] S3 `delete_if_present` with existing object (deletes, returns Deleted)
- [ ] S3 `delete_if_present` with missing object (returns NotFound)
- [ ] S3 `put_file_if_absent` TOCTOU behavior (temporary upload + copy + conflict)
- [ ] S3 `put_file_if_absent` temporary upload failure (recoverable)
- [ ] S3 `put_file_if_absent` copy failure (recoverable)
- [ ] S3 `put_content_addressed_file` with existing object (idempotent)
- [ ] S3 `copy_object_if_absent` with source=destination (AlreadyExists or NotFound)
- [ ] S3 `copy_object_if_absent` with missing source metadata (error)
- [ ] S3 `copy_object_if_absent` with AlreadyExists conflict (fallback)
- [ ] S3 `stream_range` streaming reads with range validation
- [ ] S3 `list_flat_namespace_page` with start_after offset
- [ ] S3 `list_flat_namespace_page` excludes children with `/`
- [ ] S3 `list_flat_namespace_page` limit enforcement
- [ ] S3 `begin_content_addressed_upload` TOCTOU race (metadata probe + put)
- [ ] S3 `create_resumable_upload` S3 API failure
- [ ] S3 `upload_resumable_part` part index and content-id handling
- [ ] S3 `complete_resumable_upload` part-id collection
- [ ] S3 `abort_resumable_upload` abort failure
- [ ] S3 `put_overwrite` with verify_integrity failure (length/hash mismatch)
- [ ] S3 `validated_external_range` with None length
- [ ] S3 `stream_payload_for_range` range mismatch
- [ ] S3 `verify_integrity` with length mismatch
- [ ] S3 `verify_integrity` with hash mismatch
- [ ] S3 `existing_object_outcome` with empty object
- [ ] S3 `existing_object_outcome_from_file` chunked compare with STREAM_COMPARE_CHUNK_BYTES
- [ ] S3 `existing_copy_outcome` source/dest byte comparison
- [ ] S3 `verify_file_length` failure
- [ ] S3 `temporary_upload_location` atomic counter + pid + timestamp uniqueness
- [ ] S3 multipart upload writer: write, wait_for_capacity, finish, abort

## 48. CLI frontend flag BUG

- [ ] VERIFY: CLI cannot pass `--frontend metrics` (CliServerFrontend enum missing Metrics variant)
- [ ] VERIFY: `--frontend metrics` is unreachable through CLI (only available via env var SHARDLINE_SERVER_FRONTENDS)
- [ ] If intentional: document that metrics is env-var only
- [ ] If a bug: add Metrics variant to CliServerFrontend

## 49. Platform-specific `#[cfg]` gate testing

### Unix path handling
- [ ] Secret file with symlink: `O_NOFOLLOW` prevents following symlink (Unix)
- [ ] Non-Unix fallback: secret file opens without O_NOFOLLOW
- [ ] macOS: `/tmp` is symlink to `/private/tmp`, storage root validation handles this
- [ ] Linux: `/tmp` is a real directory, validation passes
- [ ] macOS: anchored_fs path resolution handles /tmp symlink
- [ ] Linux: anchored_fs path resolution (no /tmp symlink)
- [ ] Other OS: path resolution for non-Unix platforms

### Linux-only commands
- [ ] `shardline gc schedule install` works on Linux (generates systemd units)
- [ ] `shardline gc schedule install` on macOS (returns error, Linux only)
- [ ] `shardline gc schedule uninstall` works on Linux
- [ ] `shardline gc schedule uninstall` on macOS (returns error, Linux only)
- [ ] Systemd unit files contain correct ExecStart, Calendar, User, Group
- [ ] Systemd unit files reject symlinked output directory
- [ ] Systemd unit files reject symlinked working directory
- [ ] Systemd unit files use correct binary path, env file, retention

### Production-only code paths
- [ ] `#[cfg(not(test))]` paths compile and do not panic (compile check)
- [ ] `provider.rs` production-only path works
- [ ] `config/mod.rs` production-only const stubs work
- [ ] `admin.rs` production-only key reading path works
- [ ] `storage_migration.rs` production-only path works

### Module imports
- [ ] `local_fs.rs` #[cfg(not(unix))] import resolves correctly on non-Unix
- [ ] `provider/config_io.rs` #[cfg(not(unix))] path compiles
- [ ] `local.rs` #[cfg(not(unix))] create_dir_all with permissions compiles

## 50. Config validation error paths (from env.rs)

- [ ] `InvalidPublicBaseUrl` (URL parse failure on SHARDLINE_PUBLIC_BASE_URL)
- [ ] `ChunkSize` parse error (invalid chunk size string)
- [ ] `ChunkSizeTooLarge` (chunk size > 1GB)
- [ ] `MissingOidcIssuer` (OIDC auth selected without issuer)
- [ ] `MissingJwksUrl` (JWKS auth selected without URL)
- [ ] `HubRequiresAuth` (hub frontend requires auth provider)
- [ ] `ProviderTokenTtl` parse error (invalid TTL string)
- [ ] `ZeroProviderTokenTtl` (TTL is zero)
- [ ] `MissingServerFrontends` (all frontend tokens empty after trim)
- [ ] Unknown env var is silently ignored (not rejected)

## 51. Reconstruction cache

- [ ] Memory cache: store reconstruction response, retrieve same
- [ ] Memory cache: miss returns load error
- [ ] Memory cache: TTL expiration removes entry
- [ ] Memory cache: max entries cap evicts oldest
- [ ] Memory cache: disabled cache always loads
- [ ] Redis cache: store response, retrieve same
- [ ] Redis cache: connection failure falls back to load
- [ ] Reconstruction cache benchmark produces correct metrics
- [ ] Cache keyed correctly by file ID and scope

## 52. Retention holds lifecycle

- [ ] Create hold with object hash and reason
- [ ] Create hold without reason (rejected or empty)
- [ ] Create hold for non existent object (succeeds or warns)
- [ ] List holds when none exist (empty)
- [ ] List holds when some exist (returns all)
- [ ] List holds with active only filter
- [ ] Release hold that exists
- [ ] Release hold that does not exist (error or no op)
- [ ] Release hold then verify GC deletes object
- [ ] Hold prevents GC from deleting object
- [ ] Hold TTL expires, GC deletes on next sweep
- [ ] Multiple holds on same object (all must be released)
- [ ] Hold on object that is already held (no op or update reason)
- [ ] Hold with empty object hash (rejected)
- [ ] Hold with whitespace only reason (rejected or trimmed)

## 53. Webhook delivery lifecycle

- [ ] Record webhook delivery (first time)
- [ ] Record webhook delivery with same ID (no op, replay protected)
- [ ] Record webhook delivery with different content for same ID (error)
- [ ] Record webhook delivery failure (marked as failed)
- [ ] Retry failed webhook delivery (succeeds if state allows)
- [ ] Webhook delivery for different repositories with same ID (no collision)
- [ ] Purge webhook delivery after retention period
- [ ] Legacy webhook delivery format migration (old to new)
- [ ] Webhook delivery order is preserved
- [ ] Webhook delivery with HMAC signature validation
- [ ] Webhook with invalid HMAC signature (rejected)
- [ ] Webhook with missing HMAC signature (rejected if required)

## 54. Provider repository state lifecycle

- [ ] Initial state: no access, no push (no records)
- [ ] After access: last_access_changed_at is set
- [ ] After push: last_revision_pushed_at is set
- [ ] After access then push: both timestamps set
- [ ] After push then access: both timestamps set
- [ ] After rename: scope updated, timestamps preserved
- [ ] After rename to conflicting name: rejected
- [ ] After delete: state cleared or marked as deleted
- [ ] After delete then re create: fresh state
- [ ] State survives GC (not affected)
- [ ] State survives rebuild (not affected)
- [ ] Multiple pushes: last revision is the latest
- [ ] Push with same revision: timestamp updated or no op
- [ ] Provider config file with multiple providers
- [ ] Provider config file with each provider kind (github, gitlab, gitea, generic, codeberg)
- [ ] Provider config file with invalid provider kind (rejected)

## 55. Upload ingest pipeline

- [ ] Upload body reader: reads full body
- [ ] Upload body reader: reads partial body (connection drop, error)
- [ ] Upload body reader: reads oversized body (413)
- [ ] Upload chunk store: processes chunks in parallel within window
- [ ] Upload chunk store: dedup within same upload
- [ ] Upload chunk store: dedup across different uploads
- [ ] Upload chunk store: hash validation (correct hash accepted)
- [ ] Upload chunk store: hash validation (wrong hash rejected)
- [ ] Upload ingest with compression (round trip, verify bytes)
- [ ] Upload ingest without compression (round trip, verify bytes)
- [ ] Upload ingest with empty file (zero chunks)
- [ ] Upload ingest progress tracking (reports correctly)

## 56. Server role split (multi node)

- [ ] API node issues token, transfer node validates token on read
- [ ] API node receives webhook, transfer node handles chunk
- [ ] API node and transfer node share storage backend
- [ ] API node starts without transfer routes (reconstruct only)
- [ ] Transfer node starts without API routes (chunks only)
- [ ] API node behind load balancer, multiple transfer nodes

## 57. GC entry points (library API)

- [ ] `run_gc` with Postgres index (connects to Postgres, runs GC)
- [ ] `run_gc` with Postgres connection failure (error)
- [ ] `run_gc` with local index (runs GC)
- [ ] `run_gc` with local store open failure (error)
- [ ] `run_gc_diagnostics` with local index
- [ ] `run_gc_diagnostics` without retention report
- [ ] `run_gc_diagnostics` with both retention report and orphan inventory exports

## 58. Resource cleanup and leak prevention

- [ ] Temp directories cleaned up after test
- [ ] Temporary databases cleaned up after test
- [ ] Open file descriptors closed after operation
- [ ] Network connections closed after operation
- [ ] Memory released after large operation (no leak)
- [ ] Temporary files cleaned up after crash/error
- [ ] No orphan processes left after CLI command exit
- [ ] Database connections returned to pool after operation

## 59. Benchmark and performance commands

- [ ] `shardline bench e2e` completes with valid report
- [ ] `shardline bench e2e` with custom chunk size
- [ ] `shardline bench e2e` with custom concurrency
- [ ] `shardline bench e2e` with custom iterations
- [ ] `shardline bench ingest` completes with valid report
- [ ] `shardline bench concurrent` completes with valid report
- [ ] `shardline bench sparse` completes with valid report
- [ ] `shardline bench` with --json flag outputs valid JSON
- [ ] `shardline bench` with --focus runs only requested scenario
- [ ] `shardline bench` with --deployment-target uses correct endpoint
- [ ] `shardline bench` with --storage-dir override
- [ ] All bench commands handle interrupt gracefully (Ctrl+C)

## 60. Database migration CLI commands

- [ ] `shardline db migrate up` with Postgres URL
- [ ] `shardline db migrate up` with SQLite (if supported)
- [ ] `shardline db migrate up` with --steps 1 (single migration)
- [ ] `shardline db migrate up` with --steps 0 (rejected)
- [ ] `shardline db migrate down` with valid Postgres URL
- [ ] `shardline db migrate down` with --steps 1
- [ ] `shardline db migrate down` with --steps exceeds available (reverts all)
- [ ] `shardline db migrate down` with --steps 0 (rejected)
- [ ] `shardline db migrate status` shows applied and pending
- [ ] `shardline db migrate` without subcommand (help)
- [ ] Migration commands with invalid Postgres URL (error)
- [ ] Migration commands with unreachable Postgres (error)

## 61. GC schedule CLI commands

- [ ] `shardline gc schedule install` generates systemd service file (Linux only)
- [ ] `shardline gc schedule install` on macOS (error, Linux only)
- [ ] `shardline gc schedule install` with custom output directory
- [ ] `shardline gc schedule install` with custom calendar expression
- [ ] `shardline gc schedule install` with custom retention seconds
- [ ] `shardline gc schedule install` with custom binary path
- [ ] `shardline gc schedule install` with custom env file
- [ ] `shardline gc schedule install` with symlinked output (rejected)
- [ ] `shardline gc schedule install` with symlinked working directory (rejected)
- [ ] `shardline gc schedule uninstall` removes generated files
- [ ] `shardline gc schedule uninstall` with non existent files (no op)

## 62. Config check CLI command

- [ ] `shardline config check` with valid environment (passes)
- [ ] `shardline config check` with missing signing key (reported)
- [ ] `shardline config check` with invalid config (reported)
- [ ] `shardline config check` without server running (validates local config)
- [ ] Config check reports correct backend type (local, S3, Postgres)
- [ ] Config check reports reconstruction cache status

## 63. Providerless setup CLI command

- [ ] `shardline providerless setup` creates `.shardline/` directory
- [ ] `shardline providerless setup` creates signing key file
- [ ] `shardline providerless setup` when already initialized (no op or recreate)

## 64. Health probe CLI command

- [ ] `shardline health --server http://localhost:8080` returns healthy
- [ ] `shardline health --server http://localhost:9999` returns unhealthy
- [ ] `shardline health` without server URL (default or error)

## 65. Client operations and model structs

- [ ] `GitLfsAuthenticateResponse` serializes and deserializes correctly
- [ ] `HealthResponse` contains expected fields
- [ ] `ReadyResponse` contains ready status
- [ ] `ProviderTokenIssueRequest` validation
- [ ] `ProviderTokenIssueResponse` contains token
- [ ] `ProviderWebhookResponse` validation
- [ ] `ServerStatsResponse` contains meaningful stats
- [ ] `XetCasTokenResponse` contains token
- [ ] All model structs derive expected traits (Debug, Clone, Serialize, Deserialize)

## 66. Fuzz target coverage

- [ ] Each fuzz target initializes without crash
- [ ] Each fuzz target runs for minimum iterations without crash
- [ ] Fuzz targets cover: protocol frontends, token parsing, shard parsing, reconstruction, lifecycle repair, retained shards, GC reachability, index operations (record, hub, sqlite), hub API routes, rebuild candidates, FSCK, server core auth, LFS/Bazel/OCI frontend summaries

## 67. Cross crate integration (all crates compile and link)

- [ ] `shardline-protocol` compiles standalone
- [ ] `shardline-storage` compiles standalone
- [ ] `shardline-cache` compiles standalone
- [ ] `shardline-vcs` compiles standalone
- [ ] `shardline-index` compiles standalone
- [ ] `shardline-cas` compiles standalone
- [ ] `shardline-server-core` compiles standalone
- [ ] `shardline-metrics` compiles standalone
- [ ] `shardline-oci-adapter` compiles standalone
- [ ] `shardline-protocol-adapters` compiles standalone
- [ ] `shardline-hub-api` compiles standalone
- [ ] `shardline-xet-core` compiles standalone (vendored shim)
- [ ] `shardline-xet-adapter` compiles standalone
- [ ] `shardline-provider-events` compiles standalone
- [ ] `shardline-fsck` compiles standalone
- [ ] `shardline-gc` compiles standalone
- [ ] `shardline-rebuild` compiles standalone
- [ ] `shardline-bench` compiles standalone
- [ ] `shardline-server` compiles standalone
- [ ] `shardline` (CLI) compiles standalone
- [ ] Each crate's public API is consistent with its documentation


## 36. Provider routes (app/provider_routes.rs)

- [ ] `POST /v1/providers/{provider}/tokens` with valid provider token request
- [ ] `POST /v1/providers/{provider}/tokens` with missing provider API key (401)
- [ ] `POST /v1/providers/{provider}/tokens` with invalid provider API key (403)
- [ ] `POST /v1/providers/{provider}/tokens` with missing provider subject (401)
- [ ] `POST /v1/providers/{provider}/tokens` with invalid provider token request (400)
- [ ] `POST /v1/providers/{provider}/tokens` with disabled provider tokens (404)
- [ ] `POST /v1/providers/{provider}/tokens` with unknown provider (404)
- [ ] `POST /v1/providers/{provider}/tokens` with oversized request body (413)
- [ ] `POST /v1/providers/{provider}/git-lfs-authenticate` with valid request
- [ ] `POST /v1/providers/{provider}/git-lfs-authenticate` with invalid provider
- [ ] `POST /v1/providers/{provider}/webhooks` with valid HMAC signature
- [ ] `POST /v1/providers/{provider}/webhooks` with invalid HMAC signature (403)
- [ ] `POST /v1/providers/{provider}/webhooks` with missing webhook auth (401)
- [ ] `POST /v1/providers/{provider}/webhooks` with invalid payload (400)
- [ ] `POST /v1/providers/{provider}/webhooks` with oversized payload (413)
- [ ] `GET /v1/stats` returns valid stats
- [ ] `GET /v1/stats` without auth (if public)

## 37. Git Smart HTTP routes (hub_api)

- [ ] `GET /{type}/{ns}/{repo}/info/refs` with valid repository (returns refs)
- [ ] `GET /{type}/{ns}/{repo}/info/refs` with non existent repository
- [ ] `GET /{type}/{ns}/{repo}/info/refs` with invalid service parameter
- [ ] `HEAD /{type}/{ns}/{repo}/HEAD` with valid repository
- [ ] `HEAD /{type}/{ns}/{repo}/HEAD` with non existent repository
- [ ] `POST /{type}/{ns}/{repo}/git-upload-pack` (clone/fetch)
- [ ] `POST /{type}/{ns}/{repo}/git-upload-pack` with invalid pack data
- [ ] `POST /{type}/{ns}/{repo}/git-receive-pack` (push)
- [ ] `POST /{type}/{ns}/{repo}/git-receive-pack` with invalid reference
- [ ] Git push creates entries in storage and index
- [ ] Git clone fetches entries from storage and index
- [ ] Git pull (fetch + merge) reconstructs correctly
- [ ] Git smart HTTP with authentication (valid token)
- [ ] Git smart HTTP without authentication (401)
- [ ] Pack file parsing: valid pack file accepted
- [ ] Pack file parsing: corrupt pack file rejected
- [ ] Pack file parsing: shift overflow prevented
- [ ] Pktline encoding and decoding round trip
- [ ] Pktline flush packet (0000)
- [ ] Pktline delimiting packet

## 38. Hub API routes (detailed)

- [ ] `GET /api/whoami-v2` returns current user
- [ ] `POST /api/repos/create` creates repository
- [ ] `GET /api/repos` lists repositories
- [ ] `GET /api/repos` with pagination
- [ ] `GET /api/{type}/search?search=query` returns results
- [ ] `GET /api/{type}/{ns}/{repo}` returns repository info
- [ ] `GET /api/{type}/{ns}/{repo}/modelcard` returns model card
- [ ] `GET /api/{type}/{ns}/{repo}/revisions` lists revisions
- [ ] `POST /api/{type}/{ns}/{repo}/preupload/{rev}` returns upload URLs
- [ ] `POST /api/{type}/{ns}/{repo}/commit/{rev}` commits files
- [ ] `GET /api/{type}/{ns}/{repo}/tree/{rev}/{*path}` returns file tree
- [ ] `GET /api/datasets/{ns}/{repo}/parquet` returns parquet data
- [ ] `POST /api/collections/{collection}` creates or updates collection
- [ ] `GET /api/collections` lists collections
- [ ] `GET /api/user/profile` returns user profile
- [ ] Hub API with auth token (valid)
- [ ] Hub API without auth token (401)
- [ ] Hub API with expired token (401)
- [ ] Hub API with token for wrong scope (403)
- [ ] All Hub API routes return correct HTTP status codes

## 39. Missing config env vars

- [ ] `SHARDLINE_BIND_ADDR=0.0.0.0:8080` (default)
- [ ] `SHARDLINE_BIND_ADDR=127.0.0.1:9090` (custom)
- [ ] `SHARDLINE_BIND_ADDR=invalid` (rejected)
- [ ] `SHARDLINE_PUBLIC_BASE_URL=https://assets.example.com` (custom base URL)
- [ ] `SHARDLINE_SERVER_ROLE=all` (default)
- [ ] `SHARDLINE_SERVER_ROLE=api` (API only)
- [ ] `SHARDLINE_SERVER_ROLE=transfer` (transfer only)
- [ ] `SHARDLINE_SERVER_ROLE=invalid` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_FILES=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_FILES=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_XORBS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_XORBS=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS=0` (rejected)
- [ ] `SHARDLINE_MAX_SHARD_XORB_CHUNKS=1` (minimum)
- [ ] `SHARDLINE_MAX_SHARD_XORB_CHUNKS=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory` (default)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis` with Redis URL
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis` without Redis URL (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=invalid` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=60`
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=100`
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES=0` (rejected)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://localhost:6379` (valid)
- [ ] `SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=` (empty, rejected if redis adapter)
- [ ] `SHARDLINE_INDEX_POSTGRES_URL=postgres://user:pass@localhost/db` (valid)
- [ ] `SHARDLINE_INDEX_POSTGRES_URL=` (empty, falls back to local SQLite)
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE=/path/to/providers.json`
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE` with invalid JSON (rejected)
- [ ] `SHARDLINE_PROVIDER_CONFIG_FILE` with missing file (rejected)
- [ ] `SHARDLINE_PROVIDER_API_KEY_FILE=/path/to/key`
- [ ] `SHARDLINE_PROVIDER_API_KEY_FILE` with invalid key (rejected)
- [ ] `SHARDLINE_PROVIDER_TOKEN_ISSUER=example-issuer`
- [ ] `SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS=3600`
- [ ] `SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS=0` (rejected)
- [ ] `SHARDLINE_METRICS_TOKEN_FILE=/path/to/metrics-token`
- [ ] `SHARDLINE_S3_REGION=us-west-2` (custom region)
- [ ] `SHARDLINE_S3_KEY_PREFIX=shardline/` (prefix all keys)
- [ ] `SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=true`
- [ ] `SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST=false`
- [ ] `SHARDLINE_S3_ACCESS_KEY_ID_FILE=/path/to/access-key`
- [ ] `SHARDLINE_S3_SECRET_ACCESS_KEY_FILE=/path/to/secret-key`
- [ ] `SHARDLINE_S3_SESSION_TOKEN_FILE=/path/to/session-token`
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=65536` (64KB chunks)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=1` (minimum, rejected or allowed)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=1073741824` (1GB, rejected if too large)
- [ ] `SHARDLINE_CHUNK_SIZE_BYTES=0` (rejected)
- [ ] `SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=1` (serial processing)
- [ ] `SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS=0` (rejected)
- [ ] `SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=1`
- [ ] `SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS=0` (rejected)

## 40. Config builder methods (every `with_` method)

- [ ] `with_root_dir` with writable directory
- [ ] `with_root_dir` with read only directory (error at runtime)
- [ ] `with_root_dir` with symlinked parent (rejected)
- [ ] `with_reconstruction_cache_disabled()` (cache disabled, no impact on correctness)
- [ ] `with_reconstruction_cache_memory(60, 1000)` (memory cache with TTL and capacity)
- [ ] `with_reconstruction_cache_redis(60, &redis_url)` (Redis cache)
- [ ] `with_index_postgres_url(url)` (Postgres backend)
- [ ] `with_index_postgres_url` (not called, local SQLite used)
- [ ] `with_auth_oidc_issuer(issuer_url)` (OIDC auth)
- [ ] `with_auth_jwks_url(url)` and `with_auth_jwks_issuer(issuer)` (JWKS auth)
- [ ] `with_metrics_token(token)` (metrics endpoint requires token)
- [ ] `with_provider_runtime(...)` (provider integration enabled)
- [ ] Multiple `with_` calls chain correctly (all options set)

## 41. HTTP error code coverage (every ServerError variant)

- [ ] 400 Bad Request: `RequestBodyRead`, `RequestBodyFrameOutOfBounds`, `HashParse`, `InvalidFileId`, `InvalidContentHash`, `InvalidXorbPrefix`, `XorbHashMismatch`, `InvalidSerializedXorb`, `InvalidSerializedShard`, `MissingReferencedXorb`, `InvalidRangeHeader`, `InvalidProviderTokenRequest`, `InvalidProviderWebhookPayload`, `ExpectedBodyHashMismatch`, `InvalidDigest`, `InvalidRepositoryName`, `InvalidManifestReference`, `InvalidUploadSession`, `PathValidation`
- [ ] 401 Unauthorized: `MissingAuthorization`, `InvalidAuthorizationHeader`, `InvalidToken` (with sub variants), `MissingProviderApiKey`, `MissingProviderSubject`, `MissingProviderWebhookAuthentication`, `UnauthorizedChallenge`
- [ ] 403 Forbidden: `InsufficientScope`, `InvalidProviderApiKey`, `InvalidProviderWebhookAuthentication`, `ProviderDenied`
- [ ] 404 Not Found: `NotFound`, `ProviderTokensDisabled`, `UnknownProvider`
- [ ] 406 Not Acceptable: `NotAcceptable`
- [ ] 413 Payload Too Large: `RequestBodyTooLarge`, `TooManyShardTerms`, `TooManyBatchReconstructionFileIds`, `StoredFileMetadataTooLarge`
- [ ] 414 URI Too Long: `RequestQueryTooLarge`
- [ ] 416 Range Not Satisfiable: `RangeNotSatisfiable`
- [ ] 429 Too Many Requests: `TooManyUploadSessions`, `TooManyRegistryTokenRequests`
- [ ] 500 Internal Server Error: `Io`, `Json`, `NumericConversion`, `ObjectStore`, `Index`, `StoredFileMetadataLengthMismatch`, `Overflow`, `Config`, `BlockingTask`, `ReconstructionCache`
- [ ] 503 Service Unavailable: `TransferLimiterClosed`
- [ ] 500 + 5xx: `Provider`, `BlockingTask`
- [ ] Error response body is valid JSON
- [ ] Error response body includes `error` field
- [ ] Error response body does not include stack trace (production mode)
- [ ] Error response body does not include internal paths
- [ ] Error response body may include `error_code` or structured error

## 42. Token error variants (every TokenCodecError)

- [ ] `EmptySigningKey` (rejected at config validation)
- [ ] `SigningKeyTooShort` (rejected at config validation)
- [ ] `Json` error (malformed claim payload, rejected)
- [ ] `InvalidFormat` (token does not match expected format, rejected)
- [ ] `InvalidHex` (signature hex decoding failure, rejected)
- [ ] `InvalidSignature` (signature verification failure, rejected)
- [ ] `Expired` (token past expiration, rejected)
- [ ] `Claims` violation (invalid claims content, rejected)

## 43. Config error variants (every ServerConfigError)

- [ ] `BindAddress` (invalid bind address format)
- [ ] `RootDir` (root directory does not exist or not writable)
- [ ] `InvalidServerRole` (invalid role string)
- [ ] `InvalidServerFrontend` (invalid frontend string)
- [ ] `MissingServerFrontends` (no frontends specified)
- [ ] `InvalidObjectStorageAdapter` (invalid storage adapter string)
- [ ] `InvalidAuthProvider` (invalid auth provider string)
- [ ] `MissingS3Bucket` (S3 storage selected without bucket)
- [ ] `InvalidS3AllowHttp` (invalid allow http boolean)
- [ ] `InvalidS3VirtualHostedStyleRequest` (invalid virtual hosted boolean)
- [ ] `S3CredentialSourceConflict` (both direct and file credential sources)
- [ ] `S3CredentialFile` (credential file error)
- [ ] `S3CredentialTooLarge` (credential exceeds size limit)
- [ ] `S3CredentialLengthMismatch` (credential file length mismatch)
- [ ] `S3CredentialUtf8` (credential file not valid UTF 8)
- [ ] `ChunkSize` (invalid chunk size)
- [ ] `ZeroChunkSize` (chunk size is zero)
- [ ] `ChunkSizeTooLarge` (chunk size exceeds maximum)
- [ ] `MaxRequestBodyBytes` (invalid max request body)
- [ ] `ZeroMaxRequestBodyBytes` (max request body is zero)
- [ ] `MaxShardFiles`, `ZeroMaxShardFiles`
- [ ] `MaxShardXorbs`, `ZeroMaxShardXorbs`
- [ ] `MaxShardReconstructionTerms`, `ZeroMaxShardReconstructionTerms`
- [ ] `MaxShardXorbChunks`, `ZeroMaxShardXorbChunks`
- [ ] `ZeroUploadMaxInFlightChunks`
- [ ] `ZeroTransferMaxInFlightChunks`
- [ ] `InvalidReconstructionCacheAdapter`
- [ ] `ZeroReconstructionCacheTtlSeconds`
- [ ] `ZeroReconstructionCacheMemoryMaxEntries`
- [ ] `OciUploadSessionTtl`, `ZeroOciUploadSessionTtlSeconds`
- [ ] `OciUploadMaxActiveSessions`, `ZeroOciUploadMaxActiveSessions`
- [ ] `OciRegistryTokenTtl`, `ZeroOciRegistryTokenTtlSeconds`
- [ ] `OciRegistryTokenMaxInFlightRequests`, `ZeroOciRegistryTokenMaxInFlightRequests`
- [ ] `EmptyReconstructionCacheRedisUrl`
- [ ] `MissingReconstructionCacheRedisUrl`
- [ ] `EmptyIndexPostgresUrl`
- [ ] `TokenSigningKey` (invalid signing key)
- [ ] `EmptyTokenSigningKey`
- [ ] `TokenSigningKeySourceConflict` (multiple key sources)
- [ ] `TokenSigningKeyTooLarge`
- [ ] `TokenSigningKeyLengthMismatch`
- [ ] `MissingTokenSigningKeyForServedRoutes` (server started without signing key)
- [ ] `ProviderTokenTtl`
- [ ] `ZeroProviderTokenTtl`
- [ ] `EmptyProviderApiKey`
- [ ] `ProviderApiKey`
- [ ] `ProviderApiKeyTooLarge`
- [ ] `ProviderApiKeyLengthMismatch`
- [ ] `EmptyProviderTokenIssuer`
- [ ] `IncompleteProviderTokenConfig`
- [ ] `ProviderTokensRequireSigningKey`
- [ ] `InvalidPublicBaseUrl`
- [ ] `MissingOidcIssuer`
- [ ] `MissingJwksUrl`
- [ ] `HubRequiresAuth`
- [ ] `MetricsToken`, `EmptyMetricsToken`, `MetricsTokenTooLarge`, `MetricsTokenLengthMismatch`
- [ ] Unknown env var is silently ignored (not rejected)
- [ ] Every config error produces a clear, actionable error message

## 44. Module specific tests (new modules created during refactor)

### clock
- [ ] Clock returns current time
- [ ] Clock mocking for deterministic time based tests

### overflow
- [ ] Overflow detection on arithmetic operations
- [ ] Overflow returns clean error

### local_path
- [ ] Local path validation (absolute, relative, symlinks)
- [ ] Symlink detection
- [ ] Path traversal prevention

### app/reconstruction_helpers
- [ ] Reconstruction helper functions produce correct responses
- [ ] Error handling in reconstruction helpers

### upload_ingest/chunk_store
- [ ] Chunk store upload flows (parallel, sequential)
- [ ] Chunk store overflow handling
- [ ] Concurrent chunk store operations

### upload_ingest/body_reader
- [ ] Body reader streaming with backpressure
- [ ] Body reader error handling (connection drop, timeout)
- [ ] Body reader with chunked transfer encoding

### chunk_store
- [ ] Chunk storage round trip
- [ ] Chunk storage concurrent access
- [ ] Chunk storage error handling

## 45. Server backend abstraction

- [ ] `ServerBackend::from_config` with local backend
- [ ] `ServerBackend::from_config` with S3 backend
- [ ] `ServerBackend::from_config` with Postgres backend
- [ ] `LocalBackend` round trip: upload xorb, reconstruct, verify
- [ ] `LocalBackend` round trip: upload shard, reconstruct, verify
- [ ] `PostgresBackend` round trip: upload xorb, reconstruct, verify
- [ ] All `ServerBackend` methods return correct error types
- [ ] Backend stats endpoint returns meaningful data
- [ ] Backend health check passes for initialized storage
- [ ] Backend health check fails for uninitialized storage
- [ ] Backend reuses unchanged chunks across uploads
- [ ] Backend rejects oversized metadata before reading

## 46. Reconstruction cache

- [ ] Memory cache: store reconstruction response, retrieve same
- [ ] Memory cache: miss returns load error
- [ ] Memory cache: TTL expiration removes entry
- [ ] Memory cache: max entries cap evicts oldest
- [ ] Memory cache: disabled cache always loads
- [ ] Redis cache: store response, retrieve same
- [ ] Redis cache: connection failure falls back to load
- [ ] Reconstruction cache benchmark produces correct metrics
- [ ] Cache keyed correctly by file ID and scope

## 47. Retention holds lifecycle

- [ ] Create hold with object hash and reason
- [ ] Create hold without reason (rejected or empty)
- [ ] Create hold for non existent object (succeeds or warns)
- [ ] List holds when none exist (empty)
- [ ] List holds when some exist (returns all)
- [ ] List holds with active only filter
- [ ] Release hold that exists
- [ ] Release hold that does not exist (error or no op)
- [ ] Release hold then verify GC deletes object
- [ ] Hold prevents GC from deleting object
- [ ] Hold TTL expires, GC deletes on next sweep
- [ ] Multiple holds on same object (all must be released)
- [ ] Hold on object that is already held (no op or update reason)
- [ ] Hold with empty object hash (rejected)
- [ ] Hold with whitespace only reason (rejected or trimmed)

## 48. Webhook delivery lifecycle

- [ ] Record webhook delivery (first time)
- [ ] Record webhook delivery with same ID (no op, replay protected)
- [ ] Record webhook delivery with different content for same ID (error)
- [ ] Record webhook delivery failure (marked as failed)
- [ ] Retry failed webhook delivery (succeeds if state allows)
- [ ] Webhook delivery for different repositories with same ID (no collision)
- [ ] Purge webhook delivery after retention period
- [ ] Legacy webhook delivery format migration (old to new)
- [ ] Webhook delivery order is preserved
- [ ] Webhook delivery with HMAC signature validation
- [ ] Webhook with invalid HMAC signature (rejected)
- [ ] Webhook with missing HMAC signature (rejected if required)

## 49. Provider repository state lifecycle

- [ ] Initial state: no access, no push (no records)
- [ ] After access: last_access_changed_at is set
- [ ] After push: last_revision_pushed_at is set
- [ ] After access then push: both timestamps set
- [ ] After push then access: both timestamps set
- [ ] After rename: scope updated, timestamps preserved
- [ ] After rename to conflicting name: rejected
- [ ] After delete: state cleared or marked as deleted
- [ ] After delete then re create: fresh state
- [ ] State survives GC (not affected)
- [ ] State survives rebuild (not affected)
- [ ] Multiple pushes: last revision is the latest
- [ ] Push with same revision: timestamp updated or no op
- [ ] Provider config file with multiple providers
- [ ] Provider config file with each provider kind (github, gitlab, gitea, generic, codeberg)
- [ ] Provider config file with invalid provider kind (rejected)

## 50. Upload ingest pipeline

- [ ] Upload body reader: reads full body
- [ ] Upload body reader: reads partial body (connection drop, error)
- [ ] Upload body reader: reads oversized body (413)
- [ ] Upload chunk store: processes chunks in parallel within window
- [ ] Upload chunk store: dedup within same upload
- [ ] Upload chunk store: dedup across different uploads
- [ ] Upload chunk store: hash validation (correct hash accepted)
- [ ] Upload chunk store: hash validation (wrong hash rejected)
- [ ] Upload ingest with compression (round trip, verify bytes)
- [ ] Upload ingest without compression (round trip, verify bytes)
- [ ] Upload ingest with empty file (zero chunks)
- [ ] Upload ingest progress tracking (reports correctly)

## 51. Server role split (multi node)

- [ ] API node issues token, transfer node validates token on read
- [ ] API node receives webhook, transfer node handles chunk
- [ ] API node and transfer node share storage backend
- [ ] API node starts without transfer routes (reconstruct only)
- [ ] Transfer node starts without API routes (chunks only)
- [ ] API node behind load balancer, multiple transfer nodes

## 52. Apple/Unix platform specific

- [ ] macOS: `/tmp` symlink is not rejected (or handled correctly)
- [ ] Linux: all operations pass
- [ ] Symlink protection works on both platforms
- [ ] Filesystem path handling (case sensitivity, separators)
- [ ] Large file support (>4GB on 64 bit systems)

## 53. Resource cleanup and leak prevention

- [ ] Temp directories cleaned up after test
- [ ] Temporary databases cleaned up after test
- [ ] Open file descriptors closed after operation
- [ ] Network connections closed after operation
- [ ] Memory released after large operation (no leak)
- [ ] Temporary files cleaned up after crash/error
- [ ] No orphan processes left after CLI command exit
- [ ] Database connections returned to pool after operation

## 54. Benchmark and performance commands

- [ ] `shardline bench e2e` completes with valid report
- [ ] `shardline bench e2e` with custom chunk size
- [ ] `shardline bench e2e` with custom concurrency
- [ ] `shardline bench e2e` with custom iterations
- [ ] `shardline bench ingest` completes with valid report
- [ ] `shardline bench concurrent` completes with valid report
- [ ] `shardline bench sparse` completes with valid report
- [ ] `shardline bench` with --json flag outputs valid JSON
- [ ] `shardline bench` with --focus runs only requested scenario
- [ ] `shardline bench` with --deployment-target uses correct endpoint
- [ ] `shardline bench` with --storage-dir override
- [ ] All bench commands handle interrupt gracefully (Ctrl+C)

## 55. Database migration CLI commands

- [ ] `shardline db migrate up` with Postgres URL
- [ ] `shardline db migrate up` with SQLite (if supported)
- [ ] `shardline db migrate up` with --steps 1 (single migration)
- [ ] `shardline db migrate up` with --steps 0 (rejected)
- [ ] `shardline db migrate down` with valid Postgres URL
- [ ] `shardline db migrate down` with --steps 1
- [ ] `shardline db migrate down` with --steps exceeds available (reverts all)
- [ ] `shardline db migrate down` with --steps 0 (rejected)
- [ ] `shardline db migrate status` shows applied and pending
- [ ] `shardline db migrate` without subcommand (help)
- [ ] Migration commands with invalid Postgres URL (error)
- [ ] Migration commands with unreachable Postgres (error)

## 56. GC schedule CLI commands

- [ ] `shardline gc schedule install` generates systemd service file
- [ ] `shardline gc schedule install` with custom output directory
- [ ] `shardline gc schedule install` with custom calendar expression
- [ ] `shardline gc schedule install` with custom retention seconds
- [ ] `shardline gc schedule install` with custom binary path
- [ ] `shardline gc schedule install` with custom env file
- [ ] `shardline gc schedule install` with symlinked output (rejected)
- [ ] `shardline gc schedule install` with symlinked working directory (rejected)
- [ ] `shardline gc schedule uninstall` removes generated files
- [ ] `shardline gc schedule uninstall` with non existent files (no op)

## 57. Config check CLI command

- [ ] `shardline config check` with valid environment (passes)
- [ ] `shardline config check` with missing signing key (reported)
- [ ] `shardline config check` with invalid config (reported)
- [ ] `shardline config check` without server running (validates local config)
- [ ] Config check reports correct backend type (local, S3, Postgres)
- [ ] Config check reports reconstruction cache status

## 58. Providerless setup CLI command

- [ ] `shardline providerless setup` creates `.shardline/` directory
- [ ] `shardline providerless setup` creates signing key file
- [ ] `shardline providerless setup` when already initialized (no op or recreate)

## 59. Health probe CLI command

- [ ] `shardline health --server http://localhost:8080` returns healthy
- [ ] `shardline health --server http://localhost:9999` returns unhealthy
- [ ] `shardline health` without server URL (default or error)

## 60. Client operations and model structs

- [ ] `GitLfsAuthenticateResponse` serializes and deserializes correctly
- [ ] `HealthResponse` contains expected fields
- [ ] `ReadyResponse` contains ready status
- [ ] `ProviderTokenIssueRequest` validation
- [ ] `ProviderTokenIssueResponse` contains token
- [ ] `ProviderWebhookResponse` validation
- [ ] `ServerStatsResponse` contains meaningful stats
- [ ] `XetCasTokenResponse` contains token
- [ ] All model structs derive expected traits (Debug, Clone, Serialize, Deserialize)

## 61. Fuzz target coverage

- [ ] Each fuzz target initializes without crash
- [ ] Each fuzz target runs for minimum iterations without crash
- [ ] Fuzz targets cover: protocol frontends, token parsing, shard parsing, reconstruction, lifecycle repair, retained shards, GC reachability, index operations (record, hub, sqlite), hub API routes, rebuild candidates, FSCK, server core auth, LFS/Bazel/OCI frontend summaries

## 62. Cross crate integration (all crates compile and link)

- [ ] `shardline-protocol` compiles standalone
- [ ] `shardline-storage` compiles standalone
- [ ] `shardline-cache` compiles standalone
- [ ] `shardline-vcs` compiles standalone
- [ ] `shardline-index` compiles standalone
- [ ] `shardline-cas` compiles standalone
- [ ] `shardline-server-core` compiles standalone
- [ ] `shardline-metrics` compiles standalone
- [ ] `shardline-oci-adapter` compiles standalone
- [ ] `shardline-protocol-adapters` compiles standalone
- [ ] `shardline-hub-api` compiles standalone
- [ ] `shardline-xet-core` compiles standalone (vendored shim)
- [ ] `shardline-xet-adapter` compiles standalone
- [ ] `shardline-provider-events` compiles standalone
- [ ] `shardline-fsck` compiles standalone
- [ ] `shardline-gc` compiles standalone
- [ ] `shardline-rebuild` compiles standalone
- [ ] `shardline-bench` compiles standalone
- [ ] `shardline-server` compiles standalone
- [ ] `shardline` (CLI) compiles standalone
- [ ] Each crate's public API is consistent with its documentation
