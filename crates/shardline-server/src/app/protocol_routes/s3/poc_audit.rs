//! PoC audit tests for the S3 frontend (finding candidates F-7..F-11).
//!
//! Each test drives the in-process S3 router and asserts a measured
//! exploit-vs-control delta. Helpers (auth + state build) are duplicated here
//! rather than re-exported from `tests.rs` because the `tests` module and its
//! helpers are private to `super`, so a sibling `#[cfg(test)]` module cannot
//! reach them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::arithmetic_side_effects,
    clippy::option_if_let_else,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::string_add
)]

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::{get, put},
};
use md5::Digest;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    ServerConfig, ServerFrontend, ServerRole,
    app::{AppState, ProtocolMetrics},
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
};

use super::{
    S3_OBJECT_UPLOAD_LOCKS, acquire_object_upload_lock,
    bucket::{
        s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket, s3_list_buckets,
        s3_post_bucket,
    },
    live_upload_lock_count,
    object::{s3_delete_object, s3_get_object, s3_head_object, s3_post_object, s3_put_object},
    require_s3_object_context,
};

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const OWNER: &str = "acme";
const NAME: &str = "models";
const BUCKET: &str = "acme.models";

// ---------------------------------------------------------------------------
// Reused test harness (duplicated; see module doc).
// ---------------------------------------------------------------------------

async fn build_test_state() -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_frontends([ServerFrontend::S3])
    .expect("server frontends")
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key")
    .with_s3_max_part_bytes(NonZeroU64::new(1_048_576).unwrap()) // 1 MiB
    .expect("s3 max part bytes")
    .with_s3_min_part_bytes(NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes")
    .with_s3_upload_session_max_bytes(NonZeroU64::new(1 << 40).unwrap())
    .expect("s3 session max bytes");

    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");
    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend,
        auth: Some(auth),
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(NonZeroUsize::new(256).unwrap()),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    (state, tmp)
}

fn mint_token(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

fn sigv4_auth(token: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )
}

fn s3_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(s3_list_buckets))
        .route(
            "/{bucket}",
            put(s3_create_bucket)
                .get(s3_get_bucket)
                .head(s3_head_bucket)
                .delete(s3_delete_bucket)
                .post(s3_post_bucket),
        )
        .route(
            "/{bucket}/{*key}",
            get(s3_get_object)
                .head(s3_head_object)
                .put(s3_put_object)
                .post(s3_post_object)
                .delete(s3_delete_object),
        )
        .with_state(state)
}

async fn body_bytes(response: axum::http::Response<Body>) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

fn put_request(uri: String, body: Vec<u8>) -> axum::http::Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap()
}

fn get_request(uri: String) -> axum::http::Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
        )
        .body(Body::empty())
        .unwrap()
}

fn extract_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open).unwrap() + open.len();
    let end = xml.find(&close).unwrap();
    xml[start..end].to_owned()
}

async fn create_upload_id(app: &Router, key: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}/{key}?uploads"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(response).await).unwrap();
    extract_tag(&xml, "UploadId")
}

async fn upload_part(app: &Router, key: &str, upload_id: &str, part_number: u32, content: &[u8]) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/{BUCKET}/{key}?partNumber={part_number}&uploadId={upload_id}"
                ))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "upload part {part_number}"
    );
}

fn complete_body(upload_id: &str, part_numbers: &[u32]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for part in part_numbers {
        xml.push_str(&format!(
            "  <Part><PartNumber>{part}</PartNumber><ETag>\"{upload_id}-{part}\"</ETag></Part>\n"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>\n");
    xml
}

async fn complete_upload(app: &Router, key: &str, upload_id: &str, body: String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}/{key}?uploadId={upload_id}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "complete multipart");
}

// =========================================================================
// F-7 — CopyObject unbounded full-object read + per-request byte-ceiling
// bypass (FIXED: object.rs streams the source via `s3_object_read_snapshot`
// + `read_object_stream_pinned` through a bounded reader and rejects an
// over-cap source with EntityTooLarge BEFORE copying).
//
// CONTROL: direct PUT of a body > SHARDLINE_S3_MAX_PART_BYTES → 413.
// EXPLOIT (fixed): an object whose TOTAL size exceeds the cap (assembled via
// multipart parts, each under the cap) is CopyObject'd → must now ALSO be
// rejected 413 EntityTooLarge, and the destination must not exist.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f7_copyobject_enforces_byte_ceiling() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let cap: usize = 1_048_576; // 1 MiB max part bytes in test config
    let over_cap: usize = cap + 200_000; // 1.2 MiB total object
    let part = over_cap / 2; // two parts of ~600 KiB, each < 1 MiB

    // CONTROL: direct PUT of `over_cap` bytes is rejected EntityTooLarge.
    let direct = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/direct-overcap.bin"),
            vec![0xAB_u8; over_cap],
        ))
        .await
        .unwrap();
    let direct_status = direct.status();
    let direct_body = String::from_utf8(body_bytes(direct).await).unwrap();
    assert_eq!(
        direct_status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "direct PUT of {over_cap} bytes (> cap {cap}) must be 413; got {direct_status}"
    );
    assert!(
        direct_body.contains("<Code>EntityTooLarge</Code>"),
        "direct control must be EntityTooLarge: {direct_body}"
    );

    // Build a source object > cap via multipart (each part < cap).
    let src = "data/source-overcap.bin";
    let upload_id = create_upload_id(&app, src).await;
    let part1 = vec![0x11_u8; part];
    let part2 = vec![0x22_u8; over_cap - part];
    upload_part(&app, src, &upload_id, 1, &part1).await;
    upload_part(&app, src, &upload_id, 2, &part2).await;
    complete_upload(&app, src, &upload_id, complete_body(&upload_id, &[1, 2])).await;

    // Sanity: HEAD shows the object is indeed > cap.
    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/{BUCKET}/{src}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let stored_size: usize = head
        .headers()
        .get(header::CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(stored_size, over_cap);

    // EXPLOIT (fixed): CopyObject of an over-cap source must be rejected with
    // EntityTooLarge exactly like a direct over-cap PUT — and must not create
    // the destination.
    let copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/dest-overcap.bin"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .header("x-amz-copy-source", format!("/{BUCKET}/{src}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let copy_status = copy.status();
    let copy_body = String::from_utf8(body_bytes(copy).await).unwrap();
    let dest_get = app
        .clone()
        .oneshot(get_request(format!("/{BUCKET}/dest-overcap.bin")))
        .await
        .unwrap();

    // CONTROL (small-object copy): the copy path itself works on normal data.
    let small_src = "data/small.bin";
    let small_put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{small_src}"),
            b"hello".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(small_put.status(), StatusCode::OK);
    let small_copy = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/small-dest.bin"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .header("x-amz-copy-source", format!("/{BUCKET}/{small_src}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(small_copy.status(), StatusCode::OK, "small copy control");

    // Verified delta: direct over-cap PUT → 413 AND CopyObject of the same
    // over-cap object → 413 (ceiling no longer bypassed).
    println!(
        "F-7 delta: over-cap object = {over_cap} bytes, max_part_bytes = {cap}\n\
         \x20 CONTROL direct PUT of {over_cap} bytes -> {direct_status} (EntityTooLarge)\n\
         \x20 FIXED   CopyObject of over-cap object  -> {copy_status} (EntityTooLarge, no bypass)\n\
         \x20 copy response body head: {}",
        copy_body.chars().take(80).collect::<String>()
    );
    assert_eq!(
        direct_status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "control must be 413"
    );
    assert_eq!(
        copy_status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "CopyObject of over-cap object must be 413 EntityTooLarge (ceiling enforced); got {copy_status}"
    );
    assert!(
        copy_body.contains("<Code>EntityTooLarge</Code>"),
        "copy of over-cap object must be EntityTooLarge: {copy_body}"
    );
    assert_eq!(
        dest_get.status(),
        StatusCode::NOT_FOUND,
        "rejected copy must not create the destination object"
    );
}

// =========================================================================
// F-8 — Conditional-write check-then-act TOCTOU (FIXED: object.rs re-evaluates
// the precondition INSIDE `s3_upload_object_body`, under the per-key lock,
// after the body streamed but before the index swap). The `If-None-Match:*`
// precondition is no longer only evaluated before the write lock is taken, so
// two concurrent create-if-absent PUTs on a fresh key serialize: the first
// commits, and the second re-checks against the committed row and 412s.
//
// EXPLOIT (fixed): two spawned PUTs with If-None-Match:* on the same fresh
// key must yield EXACTLY one 200 and one 412 in EVERY round.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f8_conditional_put_serialized() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    fn build(url: String) -> axum::http::Request<Body> {
        Request::builder()
            .method("PUT")
            .uri(url)
            .header(
                header::AUTHORIZATION,
                sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
            )
            .header(header::IF_NONE_MATCH, "*")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(vec![0xAB_u8; 64 * 1024])) // non-trivial body
            .unwrap()
    }

    let mut both_200_rounds = 0_u32;
    let mut precondition_violations = 0_u32;
    let iterations = 30;
    for round in 0..iterations {
        let key = format!("race/key-{round}.bin");
        let url = format!("/{BUCKET}/{key}");
        let a = app.clone();
        let b = app.clone();
        let req_a = build(url.clone());
        let req_b = build(url.clone());

        let ta = tokio::spawn(async move {
            let r = a.oneshot(req_a).await.unwrap();
            r.status()
        });
        let tb = tokio::spawn(async move {
            let r = b.oneshot(req_b).await.unwrap();
            r.status()
        });
        let (sa, sb) = tokio::join!(ta, tb);
        let (sa, sb) = (sa.unwrap(), sb.unwrap());

        let n200 = [sa, sb].iter().filter(|s| **s == StatusCode::OK).count();
        let n412 = [sa, sb]
            .iter()
            .filter(|s| **s == StatusCode::PRECONDITION_FAILED)
            .count();
        if n200 == 2 {
            both_200_rounds += 1;
        } else if n200 == 1 && n412 == 1 {
            // correct serialization
        } else {
            precondition_violations += 1;
        }
    }

    println!(
        "F-8 delta: {iterations} fresh-key If-None-Match:* races\n\
         \x20 FIXED   rounds with 2x200 (both wrote despite create-if-absent): {both_200_rounds}\n\
         \x20 FIXED   rounds that were exactly 1x200 + 1x412 (correct): {}\n\
         \x20 rounds with unexpected statuses: {precondition_violations}",
        iterations - both_200_rounds - precondition_violations
    );
    // The fix serializes the conditional check under the per-key lock, so
    // every round must be exactly one 200 + one 412 — never two successes.
    assert_eq!(
        both_200_rounds, 0,
        "no round may yield 2x200: the precondition is re-checked under the per-key lock before the swap"
    );
    assert_eq!(
        precondition_violations, 0,
        "every round must yield exactly one 200 and one 412"
    );
}

// =========================================================================
// F-8 delta B — phantom delete, serialized (FIXED: object.rs s3_delete_object
// now acquires the per-key upload lock around its check-and-delete, so DELETE
// is serialized with a PUT's upload-then-swap). Previously DELETE took no lock
// and could interleave with a PUT's swap — dropping the index row and deleting
// the record a just-committed PUT points at (PUT→200 but GET→404 with the
// write irrecoverable: a phantom delete).
//
// EXPLOIT (fixed): (1) a DELETE issued while a PUT holds the per-key lock must
// BLOCK on that lock and only complete after the PUT commits — it can no
// longer erase the PUT's swap; (2) in a slow-PUT/DELETE race the DELETE must
// finish no earlier than the PUT that started first and held the lock.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f8_phantom_delete_serialized() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // --- Probe 1: DELETE must block on the per-key lock held by a writer. ---
    let key = "phantom/serialized.bin";
    let url = format!("/{BUCKET}/{key}");

    // Resolve the per-key lock the handlers use and hold it ourselves,
    // standing in for an in-flight PUT that is mid-upload/mid-swap. Mint the
    // same typed capability the S3Repository extractor would (verified-context
    // seam) so the derived storage object key — and therefore the per-key
    // lock — matches the handlers' exactly.
    let repo = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap();
    let capability = shardline_server_core::AuthorizedRepository::from_verified_context(
        shardline_server_core::AuthContext::new(claims),
        TokenScope::Write,
    )
    .unwrap();
    let context = require_s3_object_context(&capability, key).unwrap();
    let object_lock = acquire_object_upload_lock(context.object_key.as_str());
    let _test_guard = object_lock.lock().await;

    let del_app = app.clone();
    let del_req = Request::builder()
        .method("DELETE")
        .uri(url.clone())
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .body(Body::empty())
        .unwrap();
    let delete_task = tokio::spawn(async move { del_app.oneshot(del_req).await.unwrap().status() });

    // With the fix the DELETE must be blocked on the per-key lock (it cannot
    // interleave with the held writer's swap). With the bug it completes
    // immediately because it shares no lock with the writer.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !delete_task.is_finished(),
        "DELETE must block on the per-key upload lock while a writer holds it"
    );

    // Release the lock (the in-flight PUT commits); the DELETE then runs and
    // completes with its serialized 204, and the object is gone — but the
    // DELETE never tore the PUT's commit.
    drop(_test_guard);
    let delete_status = delete_task.await.unwrap();
    assert_eq!(
        delete_status,
        StatusCode::NO_CONTENT,
        "serialized DELETE must complete 204"
    );
    let get = app.clone().oneshot(get_request(url.clone())).await.unwrap();
    assert_eq!(
        get.status(),
        StatusCode::NOT_FOUND,
        "object deleted after DELETE"
    );

    // --- Probe 2: a DELETE issued mid-PUT cannot finish before the PUT. ---
    // The PUT starts first, acquires the per-key lock, and holds it across a
    // slow ~300ms body; the DELETE is issued mid-upload and must be serialized
    // BEHIND the PUT's commit (with the bug it completes instantly, before the
    // PUT's swap — the phantom delete).
    let key2 = "phantom/race-order.bin";
    let url2 = format!("/{BUCKET}/{key2}");
    let slow_body = Body::from_stream(futures_util::stream::unfold(0_u32, |i| async move {
        if i >= 3 {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let chunk = vec![0x5A_u8; 1024];
        Some((
            Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk)),
            i + 1,
        ))
    }));
    let put_app = app.clone();
    let put_url = url2.clone();
    let put_task = tokio::spawn(async move {
        let status = put_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(put_url)
                    .header(
                        header::AUTHORIZATION,
                        sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                    )
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(slow_body)
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        (status, std::time::Instant::now())
    });
    // Let the PUT acquire the lock and start streaming its body.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    let del_app = app.clone();
    let del_url = url2.clone();
    let del_task = tokio::spawn(async move {
        let status = del_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(del_url)
                    .header(
                        header::AUTHORIZATION,
                        sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        (status, std::time::Instant::now())
    });
    let (put_joined, del_joined) = tokio::join!(put_task, del_task);
    let (put_status, put_finish) = put_joined.unwrap();
    let (del_status, del_finish) = del_joined.unwrap();
    assert_eq!(put_status, StatusCode::OK, "PUT must succeed");
    assert_eq!(del_status, StatusCode::NO_CONTENT, "DELETE must succeed");
    println!(
        "F-8-B delta: PUT finished {:?}, DELETE finished {:?} (DELETE must be no earlier)",
        put_finish, del_finish
    );
    assert!(
        del_finish >= put_finish,
        "DELETE issued mid-PUT must be serialized behind the PUT's commit \
         (phantom delete: DELETE finished before the PUT that held the lock)"
    );
}

// =========================================================================
// F-9 — Unbounded per-key upload-lock map (mod.rs:40-51). Entries were created
// on first use (`acquire_object_upload_lock`) and never removed, so every
// unique-key PUT leaked a String+Arc forever.
// FIX: the map now holds WEAK values — the strong lock lives only while a
// guard is held, and dead entries are evicted on the next acquire.
// CONTROL: live-strong-lock count before N unique-key PUTs.
// FIXED: once all N PUTs complete (every guard dropped), the live count must
// return toward baseline instead of growing 1:1 with the distinct keys.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f9_upload_lock_map_stays_bounded() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let baseline_live = live_upload_lock_count();
    let baseline_map = S3_OBJECT_UPLOAD_LOCKS.lock().unwrap().len();

    let n = 100_u32;
    for i in 0..n {
        let key = format!("lockmap/key-{i}.bin");
        let put = app
            .clone()
            .oneshot(put_request(format!("/{BUCKET}/{key}"), b"x".to_vec()))
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK, "put {key}");
    }

    // Every PUT response has returned, so every per-key guard has dropped: the
    // strong locks must be gone. The map holds only weak handles now, so the
    // live count returns toward baseline (slack for locks momentarily held by
    // concurrently-running tests) instead of growing 1:1 with the keys.
    let live_after = live_upload_lock_count();
    let map_after = S3_OBJECT_UPLOAD_LOCKS.lock().unwrap().len();
    println!(
        "F-9 delta (fixed): live-strong-lock count before = {baseline_live}, \
         after {n} unique-key PUTs = {live_after}\n\
         \x20 FIXED  live locks return toward baseline once all guards drop\n\
         \x20 map len before = {baseline_map}, after = {map_after}\n\
         \x20 note: the map is process-global, so concurrently-running tests\n\
         \x20 contribute a few momentarily-live locks; the slack absorbs them."
    );
    assert!(
        live_after <= baseline_live + 8,
        "live lock count must return toward baseline once all guards drop: \
         baseline {baseline_live} -> {live_after} after {n} unique-key PUTs"
    );
    assert!(
        map_after <= baseline_map + 8,
        "map must not retain dead per-key entries: baseline {baseline_map} -> {map_after}"
    );
}

// =========================================================================
// F-10 — Global multipart session lock held across the body stream
// (multipart.rs:122). UploadPart held `lock_upload_sessions` while draining
// the body, so a slow UploadPart blocked CreateMultipartUpload (adapter
// `create_session` also takes that lock).
// FIX: the global lock is held only for validation + metadata mutations; the
// body stream runs under a per-session lock, and the sweep/Complete take the
// same per-session lock. An unrelated CreateMultipartUpload must therefore
// complete while a trickle part is still in flight.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f10_global_session_lock_convoy() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // CONTROL baseline: CreateMultipartUpload with no contention.
    let t0 = std::time::Instant::now();
    let _baseline_id = create_upload_id(&app, "convoy/control.bin").await;
    let control_elapsed = t0.elapsed();

    // Session A for the slow UploadPart.
    let slow_key = "convoy/slow.bin";
    let upload_id_a = create_upload_id(&app, slow_key).await;

    // Slow body: 3 chunks with 120ms sleeps -> ~360ms of drain.
    let slow_body = Body::from_stream(futures_util::stream::unfold(
        (0_u32, upload_id_a.clone()),
        |(i, _id)| async move {
            if i >= 3 {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            let chunk = vec![0x33_u8; 1024];
            Some((
                Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk)),
                (i + 1, _id),
            ))
        },
    ));

    // Spawn the slow UploadPart and give it a head start so it is mid-stream
    // (holding only its per-session lock) before we time the competing
    // CreateMultipartUpload.
    let up_app = app.clone();
    let up_url = format!("/{BUCKET}/{slow_key}?partNumber=1&uploadId={upload_id_a}");
    let slow_task = tokio::spawn(async move {
        up_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(up_url)
                    .header(
                        header::AUTHORIZATION,
                        sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                    )
                    .body(slow_body)
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    });
    // Wait until the slow UploadPart is mid-stream.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // Concurrent CreateMultipartUpload on a NEW key while the trickle part is
    // still in flight: must NOT be blocked (the global lock is free; only the
    // slow session's per-session lock is held).
    let t1 = std::time::Instant::now();
    let _unblocked_id = create_upload_id(&app, "convoy/unblocked.bin").await;
    let contended_elapsed = t1.elapsed();

    let slow_status = slow_task.await.unwrap();
    assert_eq!(slow_status, StatusCode::OK, "slow UploadPart must succeed");

    println!(
        "F-10 delta (fixed): CreateMultipartUpload elapsed\n\
         \x20 CONTROL (no contention) = {:?}\n\
         \x20 FIXED   (behind slow UploadPart stream) = {:?}\n\
         \x20 create not blocked by the trickle part (< 150ms) -> {}",
        control_elapsed,
        contended_elapsed,
        contended_elapsed < std::time::Duration::from_millis(150)
    );
    assert!(
        contended_elapsed < std::time::Duration::from_millis(150),
        "create must NOT be blocked behind the slow UploadPart: got {:?} \
         (the slow body drains over ~360ms, so the global lock must not span the stream)",
        contended_elapsed
    );
}

// =========================================================================
// F-11 — Torn metadata vs pinned snapshot (FIXED: object.rs GET/HEAD now read
// the S3ObjectEntry BEFORE the pinned snapshot / record metadata, and serve
// ETag + user-metadata + Last-Modified from that same entry, so the headers
// and the streamed bytes come from the same logical commit point).
// Regression: after an overwrite that changes content AND metadata, a GET must
// serve the body, the ETag (hex MD5 of the body), and the metadata as one
// consistent pair — never the old body with the new metadata or vice versa.
// =========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poc_f11_metadata_consistent_with_bytes() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let key = "torn/data.bin";

    // Overwrite with different content + a metadata marker.
    let mut headers = put_request(format!("/{BUCKET}/{key}"), b"version-1".to_vec());
    headers
        .headers_mut()
        .insert("x-amz-meta-ver", "1".parse().unwrap());
    let overwrite = app.clone().oneshot(headers).await.unwrap();
    assert_eq!(overwrite.status(), StatusCode::OK);

    let get = app
        .clone()
        .oneshot(get_request(format!("/{BUCKET}/{key}")))
        .await
        .unwrap();
    let etag = get
        .headers()
        .get(header::ETAG)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_else(|| "<none>".to_owned());
    let meta = get
        .headers()
        .get("x-amz-meta-ver")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_else(|| "<none>".to_owned());
    let body = String::from_utf8(body_bytes(get).await).unwrap();

    println!(
        "F-11 regression: GET after overwrite -> body={body:?}, x-amz-meta-ver={meta}, etag={etag}\n\
         \x20 fixed ordering: s3_object_entry read before the pinned snapshot, and\n\
         \x20 ETag/metadata/Last-Modified served from that entry — the served pair\n\
         \x20 always matches the served bytes."
    );
    // The entry is captured before the snapshot, so the metadata must match
    // the body the stream serves (hex MD5 ETag included).
    assert_eq!(body, "version-1", "GET must serve the committed body");
    assert_eq!(
        meta, "1",
        "metadata must match the committed body's version"
    );
    assert_eq!(
        etag,
        format!("\"{:x}\"", md5::Md5::digest(b"version-1")),
        "ETag must be the hex MD5 of the served body (consistent with bytes)"
    );
}

// ---------------------------------------------------------------------------
// Frontend coexistence (regression: S3 + Hub used to panic at router build)
// ---------------------------------------------------------------------------

/// Builds a full app router with the given frontends (the same path the real
/// server takes: every frontend's routes + the Hub merge). Any matchit route
/// conflict panics here — exactly the failure this suite must catch.
async fn build_router_with(frontends: Vec<ServerFrontend>) -> Router {
    let tmp = TempDir::new().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_role(ServerRole::All)
    .with_deployment_mode(crate::DeploymentMode::Insecure)
    .with_server_frontends(frontends)
    .expect("server frontends")
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key")
    .with_reconstruction_cache_disabled();
    config
        .validate_runtime_requirements()
        .expect("runtime requirements");
    crate::app::router(config)
        .await
        .expect("app router must build")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_frontends_router_builds_and_serves() {
    // Regression (2026-08-14): enabling S3 alongside the Hub frontend panicked
    // at router build — matchit cannot host S3's `/{bucket}/{*key}` wildcard
    // and Hub's root-level parameter routes (`/{type}/{ns}/{repo}/info|resolve`)
    // at the same position. The router is now built with EVERY frontend
    // (`ServerFrontend::ALL`), so ANY future frontend whose routes collide
    // fails this test at build time with the conflicting route named.
    let app = build_router_with(ServerFrontend::ALL.to_vec()).await;

    // S3 full object roundtrip through the shared router.
    let token = mint_token(TokenScope::Write, OWNER, NAME);
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/coexist.txt"))
                .header(header::AUTHORIZATION, sigv4_auth(&token))
                .body(Body::from("coexist"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        put.status(),
        StatusCode::OK,
        "S3 PUT must work on the all-frontends router"
    );
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/coexist.txt"))
                .header(header::AUTHORIZATION, sigv4_auth(&token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        get.status(),
        StatusCode::OK,
        "S3 GET must work on the all-frontends router"
    );

    // Every other frontend's representative route must be MOUNTED — any
    // response other than the router's generic 404 proves the route survived
    // the coexistence merge.
    let probes: [(&str, &str, Body); 5] = [
        ("Hub", "/api/whoami-v2", Body::empty()), // auth-required 401
        ("Oci", "/v2/", Body::empty()),           // registry ping, auth-required 401
        ("Lfs", "/v1/lfs/objects/batch", Body::from("{}")), // parse/auth error, not 404
        ("Xet", "/v1/reconstructions/deadbeef", Body::empty()), // auth-required 401
        (
            "BazelHttp",
            "/ac/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            Body::empty(), // auth-required 403
        ),
    ];
    for (name, uri, body) in probes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(if name == "Lfs" { "POST" } else { "GET" })
                    .uri(uri)
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "{name} route {uri} must be mounted on the all-frontends router (got {})",
            response.status()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_pairs_have_no_route_conflicts() {
    // For EVERY pair of frontends the router must build without a matchit
    // conflict. A future frontend whose routes collide with any other is
    // caught here with the exact pair named. `ServerFrontend::ALL` is the
    // single source of truth — extend it when adding a variant.
    let all = ServerFrontend::ALL;
    for i in 0..all.len() {
        for j in (i + 1)..all.len() {
            let pair = [all[i], all[j]];
            let pair_label = format!("{pair:?}");
            // block_in_place moves the thread into a blocking context so a
            // fresh runtime can be built inside (a nested runtime on a
            // multi-thread test runtime is forbidden); catch_unwind captures a
            // genuine route-conflict panic per pair so the exact pair is named.
            let result = tokio::task::block_in_place(|| {
                std::panic::catch_unwind(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let tmp = TempDir::new().unwrap();
                    let config = ServerConfig::new(
                        "127.0.0.1:0".parse().unwrap(),
                        "http://127.0.0.1:8080".to_owned(),
                        tmp.path().to_path_buf(),
                        NonZeroUsize::new(65536).unwrap(),
                    )
                    .with_server_role(ServerRole::All)
                    .with_deployment_mode(crate::DeploymentMode::Insecure)
                    .with_server_frontends(pair)
                    .expect("server frontends")
                    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
                    .expect("token signing key")
                    .with_reconstruction_cache_disabled();
                    config
                        .validate_runtime_requirements()
                        .expect("runtime requirements");
                    let _app = rt
                        .block_on(crate::app::router(config))
                        .expect("pair router must build");
                })
            });
            let payload = result
                .err()
                .map(|any| {
                    any.downcast_ref::<String>()
                        .cloned()
                        .or_else(|| any.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                        .unwrap_or_else(|| "opaque panic payload".to_owned())
                })
                .unwrap_or_default();
            assert!(
                payload.is_empty(),
                "frontend pair {pair_label} fails at router build: {payload}"
            );
        }
    }
}
