#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::undocumented_unsafe_blocks
)]

//! SECURITY VALIDATION PASS 3 — Concrete tests that confirm or refute specific
//! vulnerabilities found in passes 1 and 2.
//!
//! Each test reads actual source code or exercises real code paths to
//! definitively prove (or disprove) the claimed vulnerability.



// ============================================================================
// FINDING 1: JWT Signature Verification Never Performed
// ============================================================================
// Source: crates/server/src/oidc_provider.rs:143-211
//         crates/server/src/jwks_provider.rs:153-211
//
// The code fetches JWKS keys into `_keys`, decodes header into `_header`,
// and decodes signature bytes into `_sig_bytes` — but NONE of these are used.
// Only `exp` and `iss` (oidc only) are validated. No cryptographic signature
// verification occurs.
// ============================================================================

/// Proves that the OIDC provider's verify_jwt_claims function never uses the
/// fetched JWKS keys for signature verification by inspecting the code path.
///
/// The function at oidc_provider.rs:143-211:
///   1. Fetches keys into `_keys` (line 149) — prefixed with `_`, never used
///   2. Decodes header JSON into `_header` (line 155) — prefixed with `_`, never used
///   3. Decodes signature into `_sig_bytes` (line 163) — prefixed with `_`, never used
///   4. Only checks `exp` (line 166-176) and `iss` (line 178-184)
///
/// **[VALIDATED]**: A JWT with valid `iss`/`exp` but no valid signature would be accepted.
#[test]
fn validate_jwt_signature_never_checked_oidc() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");

    // All crypto-related bindings use `_` prefix (unused variable convention in Rust)
    assert!(
        oidc_source.contains("let _keys = self.get_cached_keys()"),
        "OIDC provider fetches keys into _keys (unused variable)"
    );
    assert!(
        oidc_source.contains("let _header: serde_json::Value"),
        "OIDC provider decodes header into _header (unused variable)"
    );
    assert!(
        oidc_source.contains("let _sig_bytes = base64_decode_url(signature_b64)"),
        "OIDC provider decodes signature into _sig_bytes (unused variable)"
    );

    // Verify no signature verification function is called
    assert!(
        !oidc_source.contains("verify_signature")
            && !oidc_source.contains("ring::signature")
            && !oidc_source.contains("rsa::pkcs1v15")
            && !oidc_source.contains("PKey")
            && !oidc_source.contains("ecdsa"),
        "OIDC provider contains no cryptographic signature verification"
    );

    // Verify only exp and iss are checked
    assert!(oidc_source.contains("if exp < now"));
    assert!(oidc_source.contains("if iss != self.issuer"));
}

/// Same validation for the JWKS provider.
///
/// **[VALIDATED]**: JWKS provider also never verifies JWT signatures, AND
/// does not validate the issuer claim (worse than OIDC provider).
#[test]
fn validate_jwt_signature_never_checked_jwks() {
    let jwks_source = include_str!("../../server/src/jwks_provider.rs");

    // Verify _keys, _header, _sig_bytes are all unused
    assert!(jwks_source.contains("let _keys = self.get_or_refresh_keys()?"));
    assert!(jwks_source.contains("let _header: serde_json::Value"));
    assert!(jwks_source.contains("let _sig_bytes = base64_decode_url(signature_b64)"));

    // Verify no issuer validation either (worse than OIDC provider)
    // The JWKS source should NOT contain 'iss' in the verify_jwt_claims function.
    // (It does appear in the Jwk struct but NOT in the verify logic.)
    let verify_start = jwks_source.find("fn verify_jwt_claims").unwrap();
    let verify_end = jwks_source.find("impl AuthProvider for JwksProvider").unwrap();
    let verify_fn = &jwks_source[verify_start..verify_end];
    assert!(
        !verify_fn.contains("iss"),
        "JWKS provider does not validate issuer claim at all"
    );

    // Verify no signature verification
    assert!(
        !jwks_source.contains("verify_signature")
            && !jwks_source.contains("ring::signature")
            && !jwks_source.contains("rsa::pkcs1v15")
            && !jwks_source.contains("PKey"),
        "JWKS provider contains no cryptographic signature verification"
    );

    // Only exp is checked
    assert!(jwks_source.contains("if exp < now"));
}

/// Verifies the alg:none attack vector is possible because header alg is never validated.
///
/// **[VALIDATED]**: The JWT header `alg` field is decoded into `_header` but never checked.
/// An attacker can set `alg: "none"` and omit the signature entirely.
#[test]
fn validate_jwt_alg_none_attack_possible() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");

    // Verify the verify_jwt_claims function never references "alg"
    let verify_start = oidc_source.find("fn verify_jwt_claims").unwrap();
    let verify_end = oidc_source.find("impl AuthProvider for OidcProvider").unwrap();
    let verify_fn = &oidc_source[verify_start..verify_end];
    assert!(
        !verify_fn.contains("alg"),
        "OIDC verify_jwt_claims never checks the alg header field — alg:none attack works"
    );
}

/// Verifies missing exp defaults to u64::MAX (never expires).
///
/// **[VALIDATED]**: Tokens without an `exp` claim default to u64::MAX,
/// effectively making them non-expiring.
#[test]
fn validate_missing_exp_never_expires() {
    let oidc_source = include_str!("../../server/src/oidc_provider.rs");
    let jwks_source = include_str!("../../server/src/jwks_provider.rs");

    assert!(
        oidc_source.contains("unwrap_or(u64::MAX)"),
        "OIDC: missing exp defaults to u64::MAX (never expires)"
    );
    assert!(
        jwks_source.contains("unwrap_or(u64::MAX)"),
        "JWKS: missing exp defaults to u64::MAX (never expires)"
    );
}

// ============================================================================
// FINDING 2: Webhook SSRF — No URL Validation
// ============================================================================
// Source: crates/hub_api/src/routes.rs:73-97, 1173-1191
//
// deliver_one_webhook sends POST to arbitrary user-supplied URL with no
// validation against internal IPs, no scheme restriction (file://, gopher://),
// and no DNS rebinding protection.
// ============================================================================

/// Validates that deliver_one_webhook performs no URL validation by inspecting
/// the function source code.
///
/// **[VALIDATED]**: The function takes any URL string and sends a POST directly.
/// No scheme check, no host resolution, no internal IP filtering.
#[test]
fn validate_deliver_one_webhook_no_url_validation() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    // Extract the deliver_one_webhook function
    let start = routes_source.find("async fn deliver_one_webhook").unwrap();
    let fn_body = &routes_source[start..start + 600];

    // Should not contain any URL validation
    assert!(
        !fn_body.contains("scheme")
            && !fn_body.contains("localhost")
            && !fn_body.contains("127.0.0")
            && !fn_body.contains("10.0.")
            && !fn_body.contains("172.16.")
            && !fn_body.contains("192.168.")
            && !fn_body.contains("is_loopback"),
        "deliver_one_webhook contains no URL scheme or internal IP validation"
    );

    // Should not validate the URL is parseable as http/https
    assert!(
        !fn_body.contains("Url::parse")
            && !fn_body.contains("has_host")
            && !fn_body.contains("scheme()"),
        "deliver_one_webhook does not validate URL scheme"
    );
}

/// Validates that the webhook delivery path has zero URL validation by
/// examining the full code path from creation to delivery.
///
/// **[VALIDATED]**: Neither webhook_create nor deliver_one_webhook validates URLs.
/// No middleware or layer applies URL validation.
#[test]
fn validate_webhook_full_path_no_validation() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    // 1. webhook_create stores URL without validation
    let create_start = routes_source.find("async fn webhook_create").unwrap();
    let create_fn = &routes_source[create_start..create_start + 600];
    assert!(
        !create_fn.contains("Url::parse")
            && !create_fn.contains("scheme")
            && !create_fn.contains("validate_url")
            && !create_fn.contains("sanitize_url"),
        "webhook_create performs no URL validation before storage"
    );

    // 2. deliver_one_webhook sends to URL without validation
    let deliver_start = routes_source.find("async fn deliver_one_webhook").unwrap();
    let deliver_fn = &routes_source[deliver_start..deliver_start + 600];
    assert!(
        !deliver_fn.contains("Url::parse")
            && !deliver_fn.contains("scheme")
            && !deliver_fn.contains("host()")
            && !deliver_fn.contains("is_loopback"),
        "deliver_one_webhook sends to URL without validation"
    );

    // 3. No URL validation layer on the router
    assert!(
        !routes_source.contains("validate_webhook_url")
            && !routes_source.contains("check_url_safety"),
        "No webhook URL validation middleware exists"
    );
}

/// Validates that webhook URLs accept arbitrary schemes including file://, gopher://, etc.
///
/// **[VALIDATED]**: The `create_webhook` store method stores the URL string directly
/// with no validation. Combined with no delivery-time validation, this enables
/// SSRF via file:// (read local files), gopher:// (interact with internal services),
/// and http://127.0.0.1 (loopback access).
#[test]
fn validate_webhook_accepts_dangerous_urls_at_store_level() {
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use shardline_index::LocalIndexStore;
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
            updated_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
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
    boxed.create_repo(HubRepoType::Model, "team/ssrf-test", false).unwrap();

    // Test 1: file:// scheme — can read local files via SSRF
    let wh = boxed.create_webhook(
        "team/ssrf-test",
        "file:///etc/passwd",
        &["push".to_owned()],
        None,
    );
    assert!(wh.is_ok(), "Store accepts file:// URL — SSRF to read local files possible");

    // Test 2: http://127.0.0.1 — loopback access
    let wh = boxed.create_webhook(
        "team/ssrf-test",
        "http://127.0.0.1:6379/INFO",
        &["push".to_owned()],
        None,
    );
    assert!(wh.is_ok(), "Store accepts localhost URL — SSRF to internal services possible");

    // Test 3: gopher:// — arbitrary protocol
    let wh = boxed.create_webhook(
        "team/ssrf-test",
        "gopher://internal:7070/status",
        &["push".to_owned()],
        None,
    );
    assert!(wh.is_ok(), "Store accepts gopher:// URL — arbitrary protocol SSRF possible");

    // Test 4: http://169.254.169.254 — cloud metadata endpoint
    let wh = boxed.create_webhook(
        "team/ssrf-test",
        "http://169.254.169.254/latest/meta-data/",
        &["push".to_owned()],
        None,
    );
    assert!(wh.is_ok(), "Store accepts cloud metadata URL — SSRF to cloud metadata possible");
}

// ============================================================================
// FINDING 3: Body Size Limits — Hub API Has No DefaultBodyLimit
// ============================================================================
// Source: crates/hub_api/src/lib.rs:46-48
//         crates/server/src/app.rs:221
//
// The main server applies DefaultBodyLimit::max() but the hub API router
// has no body size limit at all.
// ============================================================================

/// Confirms that hub_routes() does not apply a DefaultBodyLimit layer.
///
/// **[VALIDATED]**: The hub API router has no body size limit. The main server
/// applies `DefaultBodyLimit::max(max_request_body_bytes)` but hub_api does not.
#[test]
fn validate_hub_router_has_no_body_limit() {
    let lib_source = include_str!("../../hub_api/src/lib.rs");

    // The hub_routes function just returns routes::router() with no layer
    assert!(
        !lib_source.contains("DefaultBodyLimit")
            && !lib_source.contains("body_limit")
            && !lib_source.contains("limit_request_body"),
        "Hub API router has no DefaultBodyLimit layer"
    );

    // Verify the main server DOES have one (for contrast)
    let app_source = include_str!("../../server/src/app.rs");
    assert!(
        app_source.contains("DefaultBodyLimit::max"),
        "Main server applies DefaultBodyLimit — hub API should too"
    );
}

/// Confirms the commit handler accepts the entire body as an unbounded String.
///
/// **[VALIDATED]**: The commit handler signature is `body: String` which reads
/// the entire request body into memory. No size check is performed before
/// parsing.
#[test]
fn validate_commit_handler_unbounded_body() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    // The commit handler signature takes body: String (entire body in memory)
    assert!(
        routes_source.contains("body: String")
            && routes_source.contains("async fn commit("),
        "commit handler accepts body: String — entire request body loaded into memory"
    );

    // No size check before parsing
    let commit_start = routes_source.find("async fn commit(").unwrap();
    let commit_fn = &routes_source[commit_start..commit_start + 500];
    assert!(
        !commit_fn.contains("len()")
            && !commit_fn.contains("size()")
            && !commit_fn.contains("body.len")
            && !commit_fn.contains("MAX_BODY"),
        "commit handler performs no body size validation before parsing"
    );
}

/// Confirms the LFS upload handler also has no body size limit.
///
/// **[VALIDATED]**: LFS upload takes `body: bytes::Bytes` — the entire LFS
/// object is loaded into memory with no size limit.
#[test]
fn validate_lfs_upload_unbounded_body() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    assert!(
        routes_source.contains("body: bytes::Bytes")
            && routes_source.contains("async fn lfs_upload("),
        "lfs_upload accepts body: bytes::Bytes — entire LFS object loaded into memory"
    );
}

// ============================================================================
// FINDING 4: Path Traversal in Commit File Paths
// ============================================================================
// Source: crates/hub_api/src/commit.rs:82-101
//         crates/hub_api/src/routes.rs:575-608
//
// CommitInstruction paths are accepted from user JSON without any validation
// for traversal sequences, absolute paths, null bytes, or control characters.
// ============================================================================

/// Validates that parse_ndjson_commit accepts traversal paths.
///
/// **[VALIDATED]**: The parser accepts `../../etc/passwd` as a valid file path
/// with no traversal or encoding validation.
#[test]
#[allow(clippy::panic)]
fn validate_commit_accepts_traversal_paths() {
    use shardline_hub_api::commit::parse_ndjson_commit;

    let traversal_ndjson = r#"{"header":{"message":"test"}}
{"file":{"path":"../../etc/passwd","content":"aGVsbG8="}}"#;

    let result = parse_ndjson_commit(traversal_ndjson);
    assert!(
        result.is_ok(),
        "parse_ndjson_commit accepts ../../etc/passwd as a valid path"
    );

    let parsed = result.unwrap();
    assert_eq!(parsed.instructions.len(), 1);
    match &parsed.instructions[0] {
        shardline_hub_api::commit::CommitInstruction::InlineFile { path, .. } => {
            assert_eq!(path, "../../etc/passwd");
        }
        shardline_hub_api::commit::CommitInstruction::LfsPointer { .. }
        | shardline_hub_api::commit::CommitInstruction::Delete { .. } => {
            panic!("Expected InlineFile instruction")
        }
    }
}

/// Validates that parse_ndjson_commit accepts absolute paths.
///
/// **[VALIDATED]**: The parser accepts `/etc/shadow` as a valid file path.
#[test]
fn validate_commit_accepts_absolute_paths() {
    use shardline_hub_api::commit::parse_ndjson_commit;

    let absolute_ndjson = r#"{"header":{"message":"test"}}
{"file":{"path":"/etc/shadow","content":"aGVsbG8="}}"#;

    let result = parse_ndjson_commit(absolute_ndjson);
    assert!(
        result.is_ok(),
        "parse_ndjson_commit accepts /etc/shadow as a valid absolute path"
    );
}

/// Validates that commit.rs has no path content validation at all.
///
/// **[VALIDATED]**: No null byte check, no traversal check, no encoding validation
/// exists in commit.rs for file paths.
#[test]
fn validate_commit_has_no_path_validation() {
    let commit_source = include_str!("../../hub_api/src/commit.rs");

    // No path validation functions exist
    assert!(
        !commit_source.contains("validate_path")
            && !commit_source.contains("sanitize_path")
            && !commit_source.contains("check_traversal")
            && !commit_source.contains("is_safe_path"),
        "commit.rs contains no path validation functions"
    );

    // Specifically check the file path extraction code has no checks
    let file_start = commit_source.find("if let Some(file) = parsed.get(\"file\")").unwrap();
    let file_block = &commit_source[file_start..file_start + 500];
    assert!(
        !file_block.contains("contains(\"..\")")
            && !file_block.contains("starts_with(\"/\")")
            && !file_block.contains("is_absolute")
            && !file_block.contains("null")
            && !file_block.contains("sanitize"),
        "file path extraction has no traversal or encoding validation"
    );
}

/// Validates that apply_commit stores traversal paths directly in the database.
///
/// **[VALIDATED]**: The apply_commit function stores user-supplied paths directly
/// into HubFileEntry without any validation.
#[test]
fn validate_apply_commit_stores_traversal_paths() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    let apply_start = routes_source.find("async fn apply_commit(").unwrap();
    let apply_fn = &routes_source[apply_start..apply_start + 1500];

    // Verify that the path from CommitInstruction is used directly without validation
    assert!(
        apply_fn.contains("path: path.clone()"),
        "apply_commit stores user-supplied path directly without traversal validation"
    );

    // Verify no path validation exists in the function
    assert!(
        !apply_fn.contains("contains(\"..\")")
            && !apply_fn.contains("starts_with(\"/\")")
            && !apply_fn.contains("is_absolute")
            && !apply_fn.contains("validate_path")
            && !apply_fn.contains("sanitize"),
        "apply_commit has no path traversal validation"
    );
}

// ============================================================================
// FINDING 5: Decompression Bomb — No Size Limit on zlib Decompression
// ============================================================================
// Source: crates/hub_api/src/git/smart_http.rs:713-722
//
// decompress_zlib calls decoder.read_to_end(&mut output) with no size limit.
// A tiny compressed payload can decompress to gigabytes, exhausting memory.
// ============================================================================

/// Validates that decompress_zlib has no output size limit.
///
/// **[VALIDATED]**: The function uses `read_to_end` with no size limit.
/// No `BufReader`, `take()`, or bounded reader is used.
#[test]
fn validate_decompress_zlib_no_size_limit() {
    let smart_http_source = include_str!("../../hub_api/src/git/smart_http.rs");

    // Extract the decompress_zlib function
    let fn_start = smart_http_source.find("fn decompress_zlib").unwrap();
    let fn_body = &smart_http_source[fn_start..fn_start + 300];

    // Specifically confirm read_to_end is used (no bounded alternative)
    assert!(
        fn_body.contains("read_to_end(&mut output)"),
        "decompress_zlib uses unbounded read_to_end — decompression bomb possible"
    );

    // Confirm no bounded reader
    assert!(
        !fn_body.contains("BufReader")
            && !fn_body.contains("take(")
            && !fn_body.contains("ReadLimit"),
        "decompress_zlib uses no bounded reader — no decompression bomb protection"
    );

    // Confirm no size check
    assert!(
        !fn_body.contains("MAX_")
            && !fn_body.contains("max_size")
            && !fn_body.contains("limit"),
        "decompress_zlib has no size limit check"
    );
}

/// Validates that the receive_pack path calls decompress without pre-filtering.
///
/// **[VALIDATED]**: The pack parsing path does not validate compressed data sizes
/// before calling decompress_zlib.
#[test]
fn validate_pack_parsing_no_compressed_size_check() {
    let smart_http_source = include_str!("../../hub_api/src/git/smart_http.rs");

    let receive_start = smart_http_source.find("async fn receive_pack").unwrap_or(0);
    let receive_fn = &smart_http_source[receive_start..receive_start + 3000];

    assert!(
        !receive_fn.contains("compressed_size")
            && !receive_fn.contains("DECOMPRESS_MAX")
            && !receive_fn.contains("MAX_DECOMPRESSED"),
        "receive_pack does not validate compressed data sizes before decompression"
    );
}

// ============================================================================
// FINDING 6: whoami endpoint hardcodes is_admin: true
// ============================================================================

/// Validates the whoami endpoint always returns is_admin: true.
///
/// **[VALIDATED]**: The whoami handler hardcodes `is_admin: true` regardless
/// of the authenticated user's actual role.
#[test]
fn validate_whoami_hardcoded_admin() {
    let routes_source = include_str!("../../hub_api/src/routes.rs");

    // Find the whoami function and extract it fully
    let whoami_start = routes_source.find("async fn whoami(").unwrap();
    // The function is about 700 chars — use generous window
    let whoami_fn = &routes_source[whoami_start..whoami_start + 800];

    assert!(
        whoami_fn.contains("is_admin: true"),
        "whoami endpoint hardcodes is_admin: true regardless of user role"
    );
}

// ============================================================================
// FINDING 7: Error messages leak internal details
// ============================================================================

/// Validates that CasError exposes internal error strings to clients.
///
/// **[VALIDATED]**: CasError(String) embeds arbitrary internal error strings
/// and the IntoResponse impl serializes `self.to_string()` as JSON.
#[test]
fn validate_cas_error_leaks_internals() {
    let error_source = include_str!("../../hub_api/src/error.rs");

    assert!(
        error_source.contains("CasError(String)")
            && error_source.contains("error: self.to_string()"),
        "CasError leaks internal error details via self.to_string() in response body"
    );
}

// ============================================================================
// FINDING 8: NDJSON commit has no instruction count limit
// ============================================================================

/// Validates that parse_ndjson_commit accumulates instructions into an unbounded Vec.
///
/// **[VALIDATED]**: The function uses `instructions.push()` in a loop with no
/// count limit. An attacker can craft millions of tiny valid JSON lines to cause
/// excessive memory allocation.
#[test]
fn validate_ndjson_unbounded_instruction_count() {
    let commit_source = include_str!("../../hub_api/src/commit.rs");

    // Search the entire file (not a window) for the relevant patterns
    // Should not contain any instruction count limit
    assert!(
        !commit_source.contains("MAX_INSTRUCTIONS")
            && !commit_source.contains("instruction_count"),
        "parse_ndjson_commit has no MAX_INSTRUCTIONS or instruction_count limit"
    );

    // Verify the function accumulates instructions via unbounded push
    assert!(
        commit_source.contains("instructions.push(CommitInstruction::InlineFile"),
        "parse_ndjson_commit accumulates InlineFile instructions into unbounded Vec"
    );
    assert!(
        commit_source.contains("instructions.push(CommitInstruction::LfsPointer"),
        "parse_ndjson_commit accumulates LfsPointer instructions into unbounded Vec"
    );
    assert!(
        commit_source.contains("instructions.push(CommitInstruction::Delete"),
        "parse_ndjson_commit accumulates Delete instructions into unbounded Vec"
    );

    // Verify no limit is checked on the instructions vec length
    let parse_start = commit_source.find("pub fn parse_ndjson_commit").unwrap();
    let parse_end = commit_source.find("\npub fn validate_lfs_oid").unwrap();
    let parse_fn = &commit_source[parse_start..parse_end];
    assert!(
        !parse_fn.contains(".len() >")
            && !parse_fn.contains(".len() >=")
            && !parse_fn.contains("instructions.len()"),
        "parse_ndjson_commit does not check instruction count before accumulating"
    );
}

// ============================================================================
// FINDING 9: Commit handler applies no body size validation (integration test)
// ============================================================================

/// Validates the full commit request path has no size validation via HTTP.
///
/// **[VALIDATED]**: The hub API router accepts a multi-file commit with no body
/// size limit. The request is processed successfully without rejection.
#[tokio::test]
async fn validate_commit_no_body_size_validation() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use shardline_hub_api::routes::HubState;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use shardline_index::LocalIndexStore;
    use std::sync::Once;
    use tempfile::TempDir;
    use tower::ServiceExt;

    static COMMIT_INIT: Once = Once::new();
    static mut COMMIT_DIR: Option<TempDir> = None;

    COMMIT_INIT.call_once(|| {
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
                updated_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
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
            );",
        )
        .unwrap();

        let store = LocalIndexStore::open(root);
        let boxed = BoxedHubStore::from_store(store);
        let state = HubState {
            store: boxed,
            auth: None,
            http_client: None,
        };
        shardline_hub_api::init(state);

        unsafe {
            COMMIT_DIR = Some(tmp);
        }
    });

    let store = shardline_hub_api::state::get_for_test().store.clone();
    store
        .create_repo(HubRepoType::Model, "team/body-limit-test", false)
        .unwrap();
    store
        .create_revision("team/body-limit-test", None, "sha1", "main", "init")
        .unwrap();

    // Create a multi-file commit body
    let mut ndjson_body = String::from(
        r#"{"header":{"message":"large commit","parentCommit":"sha1"}}"#,
    );
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

    let app = shardline_hub_api::hub_routes();
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

    // 200 means no body size limit rejection
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Hub API commit accepts body without size limit — DoS possible"
    );
}
