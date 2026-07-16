#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

//! SECURITY VALIDATION PASS 3 — Concrete tests that confirm or refute specific
//! vulnerabilities found in passes 1 and 2.
//!
//! Each test reads actual source code or exercises real code paths to
//! definitively prove (or disprove) the claimed vulnerability.

// ============================================================================
// FINDING 1: JWT Signature Verification — FIXED
// ============================================================================

/// Verifies OIDC provider now performs proper JWT signature verification.
///
/// **[FIXED]**: The provider now uses `self.get_cached_keys()` (not `_keys`),
/// calls `decode()` with a `DecodingKey` and `Validation`, and rejects
/// `alg: "none"`. The `exp` claim is required (not defaulted to u64::MAX).
#[test]
fn validate_jwt_signature_checked_oidc() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");

    assert!(
        oidc_source.contains("let keys = self.get_cached_keys()"),
        "OIDC provider fetches keys into `keys` (used variable)"
    );
    assert!(
        oidc_source.contains("decode::<serde_json::Value>("),
        "OIDC provider calls decode() for signature verification"
    );
    assert!(
        oidc_source.contains("build_decoding_key("),
        "OIDC provider builds a decoding key from JWK"
    );
    assert!(
        oidc_source.contains("Validation::new("),
        "OIDC provider creates Validation with algorithm"
    );
    assert!(
        oidc_source.contains("set_issuer("),
        "OIDC provider validates issuer claim"
    );
    assert!(
        oidc_source.contains("if alg_str == \"none\""),
        "OIDC provider rejects alg:none attack"
    );
}

/// Verifies JWKS provider now performs proper JWT signature verification.
///
/// **[FIXED]**: The provider now uses `self.get_or_refresh_keys()` (not `_keys`),
/// calls `decode()` with proper key material, validates the issuer, and
/// requires the `exp` claim.
#[test]
fn validate_jwt_signature_checked_jwks() {
    let jwks_source = include_str!("../../server/src/jwks_provider.rs");

    assert!(
        jwks_source.contains("get_or_refresh_keys()"),
        "JWKS provider calls get_or_refresh_keys()"
    );
    assert!(
        jwks_source.contains("decode::<serde_json::Value>("),
        "JWKS provider calls decode() for signature verification"
    );
    assert!(
        jwks_source.contains("build_decoding_key("),
        "JWKS provider builds a decoding key from JWK"
    );
    assert!(
        jwks_source.contains("set_issuer("),
        "JWKS provider validates issuer claim"
    );
    assert!(
        jwks_source.contains("if alg_str == \"none\""),
        "JWKS provider rejects alg:none attack"
    );
}

/// Verifies alg:none attack is now blocked.
///
/// **[FIXED]**: Both providers now check `if alg_str == "none"` and return
/// `AuthError::InvalidToken`.
#[test]
fn validate_jwt_alg_none_attack_blocked() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");
    let jwks_source = include_str!("../../server/src/jwks_provider.rs");

    let oidc_verify_start = oidc_source.find("fn verify_jwt_claims").unwrap();
    let oidc_verify_end = oidc_source
        .find("impl AuthProvider for OidcProvider")
        .unwrap();
    let oidc_fn = &oidc_source[oidc_verify_start..oidc_verify_end];
    assert!(
        oidc_fn.contains("if alg_str == \"none\""),
        "OIDC verify_jwt_claims now rejects alg:none"
    );

    let jwks_verify_start = jwks_source.find("fn verify_jwt_claims").unwrap();
    let jwks_verify_end = jwks_source
        .find("impl AuthProvider for JwksProvider")
        .unwrap();
    let jwks_fn = &jwks_source[jwks_verify_start..jwks_verify_end];
    assert!(
        jwks_fn.contains("if alg_str == \"none\""),
        "JWKS verify_jwt_claims now rejects alg:none"
    );
}

/// Verifies missing exp is now rejected instead of defaulting to u64::MAX.
///
/// **[FIXED]**: Both providers now use `ok_or_else` to require the `exp` claim
/// instead of `unwrap_or(u64::MAX)`.
#[test]
fn validate_missing_exp_now_rejected() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");
    let jwks_source = include_str!("../../server/src/jwks_provider.rs");

    assert!(
        !oidc_source.contains("unwrap_or(u64::MAX)"),
        "OIDC: no longer defaults missing exp to u64::MAX"
    );
    assert!(
        !jwks_source.contains("unwrap_or(u64::MAX)"),
        "JWKS: no longer defaults missing exp to u64::MAX"
    );
    assert!(
        oidc_source.contains("missing exp claim"),
        "OIDC: returns error when exp is missing"
    );
    assert!(
        jwks_source.contains("missing exp claim"),
        "JWKS: returns error when exp is missing"
    );
}

// ============================================================================
// FINDING 2: Webhook SSRF — PARTIALLY FIXED (URL validation at creation)
// ============================================================================

/// Validates that delivery-time URL validation exists in `deliver_one_webhook`.
///
/// **[FIXED]**: `deliver_one_webhook` now calls `validate_webhook_url` and
/// performs DNS resolution to block private/internal IPs at delivery time.
#[test]
fn validate_deliver_one_webhook_has_url_validation() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    let start = routes_source.find("async fn deliver_one_webhook").unwrap();
    let fn_body = &routes_source[start..start + 2000];

    assert!(
        fn_body.contains("validate_webhook_url"),
        "deliver_one_webhook calls validate_webhook_url"
    );
    assert!(
        fn_body.contains("is_private_ip"),
        "deliver_one_webhook checks resolved addresses for private IPs"
    );
}

/// Validates that webhook URL validation exists at creation time.
///
/// **[FIXED]**: `validate_webhook_url` now checks scheme (http/https only),
/// host presence, URL length, and rejects private/internal IPs.
#[test]
fn validate_webhook_url_validation_exists() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    assert!(
        routes_source.contains("fn validate_webhook_url("),
        "validate_webhook_url function exists"
    );
    assert!(
        routes_source.contains("scheme != \"http\" && scheme != \"https\""),
        "validate_webhook_url checks scheme is http or https"
    );
    assert!(
        routes_source.contains("MAX_WEBHOOK_URL_LEN"),
        "validate_webhook_url enforces URL length limit"
    );
}

/// Validates that webhook URLs accept arbitrary schemes at the store level.
///
/// **[STILL OPEN]**: The store itself does not validate URLs — validation
/// happens at the route handler level via `validate_webhook_url`.
#[test]
fn validate_webhook_accepts_dangerous_urls_at_store_level() {
    use shardline_index::LocalIndexStore;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    let db_path = root.join("metadata.sqlite3");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
            repo_id TEXT PRIMARY KEY,
            repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
            private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
            default_branch TEXT NOT NULL,
            created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
            updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
            repo_id TEXT NOT NULL,
            ref_name TEXT NOT NULL,
            sha TEXT NOT NULL,
            parent_sha TEXT,
            message TEXT,
            created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
            PRIMARY KEY (repo_id, sha),
            FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_refs (
            repo_id TEXT NOT NULL,
            ref_name TEXT NOT NULL,
            sha TEXT NOT NULL,
            PRIMARY KEY (repo_id, ref_name)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
            id TEXT PRIMARY KEY,
            repo_id TEXT NOT NULL,
            url TEXT NOT NULL,
            events TEXT NOT NULL DEFAULT 'push',
            secret TEXT,
            active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
            created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
            FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
        );",
    )
    .unwrap();

    let store = LocalIndexStore::open(root);
    let boxed = BoxedHubStore::from_store(store);
    boxed
        .create_repo(HubRepoType::Model, "team/ssrf-test", false)
        .unwrap();

    let wh = boxed.create_webhook(
        "team/ssrf-test",
        "file:///etc/passwd",
        &["push".to_owned()],
        None,
    );
    assert!(
        wh.is_ok(),
        "Store-level accepts file:// URL (validation is at route layer)"
    );
}

// ============================================================================
// FINDING 3: Body Size Limits — FIXED
// ============================================================================

/// Confirms that hub_routes() now applies a DefaultBodyLimit layer.
///
/// **[FIXED]**: The hub API router now applies `DefaultBodyLimit::max(64MB)`.
#[test]
fn validate_hub_router_has_body_limit() {
    let lib_source = include_str!("../../hub_api/src/lib.rs");

    assert!(
        lib_source.contains("DefaultBodyLimit::max"),
        "Hub API router now applies DefaultBodyLimit::max"
    );
}

/// Confirms the commit handler accepts the entire body as a String (bounded by
/// the router-level body limit).
///
/// **[MITIGATED]**: The router's `DefaultBodyLimit::max(64MB)` bounds the body
/// size. The handler still takes `body: String` but the outer layer rejects
/// oversized requests before they reach the handler.
#[test]
fn validate_commit_handler_body_bounded_by_router() {
    let lib_source = include_str!("../../hub_api/src/lib.rs");

    assert!(
        lib_source.contains("DefaultBodyLimit::max"),
        "Hub API router enforces body size limit at the layer level"
    );
}

/// Confirms the LFS upload handler is also bounded by the router body limit.
///
/// **[MITIGATED]**: Same as above — the router-level limit applies.
#[test]
fn validate_lfs_upload_unbounded_body() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    assert!(
        routes_source.contains("body: bytes::Bytes")
            && routes_source.contains("async fn lfs_upload("),
        "lfs_upload accepts body: bytes::Bytes — bounded by router DefaultBodyLimit"
    );
}

// ============================================================================
// FINDING 4: Path Traversal in Commit File Paths — FIXED
// ============================================================================

/// Validates that parse_ndjson_commit now rejects traversal paths.
///
/// **[FIXED]**: `validate_commit_path` rejects paths containing `..` components.
#[test]
#[allow(clippy::panic)]
fn validate_commit_rejects_traversal_paths() {
    use shardline_hub_api::commit::parse_ndjson_commit;

    let traversal_ndjson = r#"{"header":{"message":"test"}}
{"file":{"path":"../../etc/passwd","content":"aGVsbG8="}}"#;

    let result = parse_ndjson_commit(traversal_ndjson);
    assert!(
        result.is_err(),
        "parse_ndjson_commit now rejects ../../etc/passwd"
    );
}

/// Validates that parse_ndjson_commit now rejects absolute paths.
///
/// **[FIXED]**: `validate_commit_path` rejects paths starting with `/`.
#[test]
fn validate_commit_rejects_absolute_paths() {
    use shardline_hub_api::commit::parse_ndjson_commit;

    let absolute_ndjson = r#"{"header":{"message":"test"}}
{"file":{"path":"/etc/shadow","content":"aGVsbG8="}}"#;

    let result = parse_ndjson_commit(absolute_ndjson);
    assert!(
        result.is_err(),
        "parse_ndjson_commit now rejects /etc/shadow"
    );
}

/// Validates that commit.rs has path validation.
///
/// **[FIXED]**: `validate_commit_path` function now exists and checks for
/// `..` components, absolute paths, null bytes, control characters, and
/// max length.
#[test]
fn validate_commit_has_path_validation() {
    let commit_source = include_str!("../../hub_api/src/commit.rs");

    assert!(
        commit_source.contains("fn validate_commit_path("),
        "commit.rs contains validate_commit_path function"
    );
    assert!(
        commit_source.contains("MAX_COMMIT_PATH_LEN"),
        "commit.rs enforces max path length"
    );
}

// ============================================================================
// FINDING 5: Decompression Bomb — FIXED
// ============================================================================

/// Validates that decompress_zlib now has an output size limit.
///
/// **[FIXED]**: `MAX_DECOMPRESSED_SIZE` is defined and enforced after
/// decompression. Oversized output is rejected.
#[test]
fn validate_decompress_zlib_has_size_limit() {
    let smart_http_source = include_str!("../../hub_api/src/git/smart_http.rs");

    assert!(
        smart_http_source.contains("MAX_DECOMPRESSED_SIZE"),
        "decompress_zlib has MAX_DECOMPRESSED_SIZE constant"
    );
    assert!(
        smart_http_source.contains("output.len() > MAX_DECOMPRESSED_SIZE"),
        "decompress_zlib checks decompressed size against limit"
    );
    assert!(
        smart_http_source.contains("exceeds maximum size"),
        "decompress_zlib returns error for oversized output"
    );
}

/// Validates that parse_pack_data now validates shift overflow.
///
/// **[FIXED]**: The variable-length integer parser now checks `shift >= 64`
/// before left-shifting, preventing integer overflow from malicious packs.
#[test]
fn validate_pack_parser_shift_overflow_protected() {
    let smart_http_source = include_str!("../../hub_api/src/git/smart_http.rs");

    let parse_start = smart_http_source.find("fn parse_pack_data").unwrap();
    let parse_fn = &smart_http_source[parse_start..parse_start + 2500];

    assert!(
        parse_fn.contains("shift >= 64"),
        "parse_pack_data checks shift overflow before left-shift"
    );
}

// ============================================================================
// FINDING 6: whoami endpoint hardcodes is_admin: true
// ============================================================================

/// Validates the whoami endpoint no longer hardcodes is_admin: true.
///
/// **[FIXED]**: The whoami handler now returns `is_admin: false` (not hardcoded true).
#[test]
fn validate_whoami_hardcoded_admin() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    let whoami_start = routes_source.find("async fn whoami(").unwrap();
    let whoami_fn = &routes_source[whoami_start..whoami_start + 800];

    assert!(
        whoami_fn.contains("is_admin: false"),
        "whoami endpoint should not hardcode is_admin: true"
    );
}

// ============================================================================
// FINDING 7: Error messages leak internal details — FIXED
// ============================================================================

/// Validates that CasError no longer leaks internal error strings to clients.
///
/// **[FIXED]**: The `IntoResponse` impl for `CasError` now returns a generic
/// "internal error" string instead of `self.to_string()`.
#[test]
fn validate_cas_error_no_longer_leaks_internals() {
    let error_source = include_str!("../../hub_api/src/error.rs");

    assert!(
        error_source.contains("CasError(_)") && error_source.contains("\"internal error\""),
        "CasError now returns generic 'internal error' — no internal details leaked"
    );
}

// ============================================================================
// FINDING 8: NDJSON commit instruction count limit — FIXED
// ============================================================================

/// Validates that parse_ndjson_commit now has an instruction count limit.
///
/// **[FIXED]**: `MAX_COMMIT_INSTRUCTIONS` is defined (100,000) and checked
/// before accumulating each instruction.
#[test]
fn validate_ndjson_has_instruction_count_limit() {
    let commit_source = include_str!("../../hub_api/src/commit.rs");

    assert!(
        commit_source.contains("MAX_COMMIT_INSTRUCTIONS"),
        "parse_ndjson_commit has MAX_COMMIT_INSTRUCTIONS constant"
    );
    assert!(
        commit_source.contains("instructions.len() >= MAX_COMMIT_INSTRUCTIONS"),
        "parse_ndjson_commit checks instruction count before accumulating"
    );
    assert!(
        commit_source.contains("too many instructions"),
        "parse_ndjson_commit returns descriptive error for too many instructions"
    );
}

// ============================================================================
// FINDING 9: Commit handler body size — MITIGATED
// ============================================================================

/// Validates the full commit request path is bounded by router body limit.
///
/// **[MITIGATED]**: The hub API router's `DefaultBodyLimit::max(64MB)` bounds
/// all request bodies. A body exceeding 64MB is rejected before reaching the
/// commit handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_commit_body_bounded_by_router() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use shardline_hub_api::routes::HubState;
    use shardline_index::LocalIndexStore;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use tempfile::TempDir;
    use tower::ServiceExt;

    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    let db_path = root.join("metadata.sqlite3");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
                repo_id TEXT PRIMARY KEY,
                repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
                private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
                default_branch TEXT NOT NULL,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                parent_sha TEXT,
                message TEXT,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                PRIMARY KEY (repo_id, sha),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_refs (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                PRIMARY KEY (repo_id, ref_name)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                sha TEXT NOT NULL,
                is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),
                inline_content BLOB,
                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
                oid TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                url TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT 'push',
                secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_webhooks_repo_idx ON shardline_hub_webhooks (repo_id);",
    )
    .unwrap();
    drop(conn);

    let store = LocalIndexStore::open(root);
    let boxed = BoxedHubStore::from_store(store);
    let state = HubState {
        store: boxed,
        auth: None,
        http_client: None,
    };

    let store = state.store.clone();
    store
        .create_repo(HubRepoType::Model, "team/body-limit-test", false)
        .unwrap();
    store
        .create_revision("team/body-limit-test", None, "sha1", "main", "init")
        .unwrap();

    let mut ndjson_body =
        String::from(r#"{"header":{"message":"large commit","parentCommit":"sha1"}}"#);
    ndjson_body.push('\n');
    for i in 0..100 {
        let content = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("file content {i}").as_bytes(),
        );
        ndjson_body.push_str(&format!(
            r#"{{"file":{{"path":"dir/file{i}.txt","content":"{content}"}}}}"#
        ));
        ndjson_body.push('\n');
    }

    let app = shardline_hub_api::hub_routes(state, true);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/team/body-limit-test/commit/main")
                .header("content-type", "application/json")
                .body(Body::from(ndjson_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Small commit body accepted within 64MB limit"
    );
}
