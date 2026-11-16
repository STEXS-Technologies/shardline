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

// JWT signature, issuer and algorithm rejection are exercised through both
// real providers in shardline-server's oidc_provider/jwks_provider tests,
// including signed tokens with a missing expiry claim.
// Shared algorithm guards live in jwt_keys::tests; source layout is not a contract.

// ============================================================================
// FINDING 2: Webhook SSRF — PARTIALLY FIXED (URL validation at creation)
// ============================================================================

/// Validates that delivery-time URL validation exists in `deliver_one_webhook`.
///
/// **[FIXED]**: `deliver_one_webhook` now calls `validate_webhook_url` and
/// performs DNS resolution to block private/internal IPs at delivery time.
#[test]
fn validate_deliver_one_webhook_has_url_validation() {
    let routes_source = include_str!("../src/routes/webhooks.rs");

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
    let store = LocalIndexStore::new(root).unwrap();
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

// ============================================================================
// FINDING 5: Decompression Bomb — FIXED
// ============================================================================

/// Oversized declared inflation is rejected before invoking the decoder.
#[test]
fn validate_decompress_zlib_has_size_limit() {
    use sha1::{Digest, Sha1};
    let mut pack = b"PACK".to_vec();
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&1u32.to_be_bytes());
    let size = 512usize * 1024 * 1024 + 1;
    pack.push(0xb0 | (size & 15) as u8);
    let mut size = size >> 4;
    while size != 0 {
        let mut byte = (size & 127) as u8;
        size >>= 7;
        if size != 0 {
            byte |= 128;
        }
        pack.push(byte);
    }
    pack.extend_from_slice(&Sha1::digest(&pack));
    assert!(matches!(
        shardline_hub_api::git::smart_http::parse_pack_data(&pack),
        Err(shardline_hub_api::git::pack::PackError::ExcessiveDecompressedSize)
    ));
}

/// Validates that parse_pack_data now validates shift overflow.
///
/// **[FIXED]**: The variable-length integer parser now checks `shift >= 64`
/// before left-shifting, preventing integer overflow from malicious packs.
#[test]
fn validate_pack_parser_shift_overflow_protected() {
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&1u32.to_be_bytes());
    pack.push(0xb0); // blob header with an unterminated size varint
    pack.extend_from_slice(&[0xff; 10]);
    use sha1::{Digest, Sha1};
    pack.extend_from_slice(&Sha1::digest(&pack));
    assert!(
        matches!(
            shardline_hub_api::git::smart_http::parse_pack_data(&pack),
            Err(shardline_hub_api::git::pack::PackError::ShiftOverflow)
        ),
        "malicious pack size must be rejected before shifting past integer width"
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
    let mut body = String::from("{\"header\":{\"message\":\"instruction bound\"}}\n");
    let instruction = "{\"deletedEntry\":{\"path\":\"absent\"}}\n";
    for _ in 0..100_000 {
        body.push_str(instruction);
    }
    let parsed = shardline_hub_api::commit::parse_ndjson_commit(&body)
        .expect("documented instruction ceiling remains accepted");
    assert_eq!(parsed.instructions.len(), 100_000);
    body.push_str(instruction);
    let error = shardline_hub_api::commit::parse_ndjson_commit(&body)
        .expect_err("one instruction beyond the ceiling must be rejected");
    assert!(error.to_string().contains("too many instructions"));
}

// ============================================================================
// FINDING 9: Commit handler body size — MITIGATED
// ============================================================================

/// JSON extractors enforce the router cap; streamed commits enforce a separate
/// per-line cap without advancing the branch when parsing fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_request_limits_reject_oversized_json_and_commit_lines() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use shardline_hub_api::routes::HubState;
    use shardline_index::LocalIndexStore;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use tempfile::TempDir;
    use tower::ServiceExt;

    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    let store = LocalIndexStore::new(root.clone()).unwrap();
    let boxed = BoxedHubStore::from_store(store);
    let object_store = shardline_server_core::ServerObjectStore::local(root.join("lfs"))
        .expect("local object store");
    let state = HubState {
        store: boxed,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };

    let store = state.store.clone();
    store
        .create_repo(HubRepoType::Model, "team/body-limit-test", false)
        .unwrap();
    store
        .create_revision("team/body-limit-test", None, "sha1", "main", "init")
        .unwrap();

    let ndjson_body = concat!(
        "{\"header\":{\"message\":\"small commit\",\"parentCommit\":\"sha1\"}}\n",
        "{\"file\":{\"path\":\"file.txt\",\"content\":\"aGVsbG8=\"}}\n",
    );

    let app = shardline_hub_api::hub_routes(state, true);
    let response = app
        .clone()
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

    let status = response.status();
    let response_body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "small streamed commit should succeed: {}",
        String::from_utf8_lossy(&response_body)
    );
    let head = store
        .resolve_revision("team/body-limit-test", "main")
        .unwrap();
    for (endpoint, size, expected_status, expected_message) in [
        (
            "commit/main",
            16 * 1024 * 1024 + 1,
            StatusCode::BAD_REQUEST,
            "commit line exceeds 16 MiB limit; use LFS for large files",
        ),
        (
            "preupload/main",
            64 * 1024 * 1024 + 1,
            StatusCode::PAYLOAD_TOO_LARGE,
            "length limit exceeded",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/models/team/body-limit-test/{endpoint}"))
                    .header("content-type", "application/json")
                    .body(Body::from(vec![b' '; size]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status, "{endpoint}");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains(expected_message),
            "{endpoint}: {body:?}"
        );
        assert_eq!(
            store
                .resolve_revision("team/body-limit-test", "main")
                .unwrap(),
            head
        );
    }
}
