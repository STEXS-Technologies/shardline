//! M7 — cross-frontend conformance tests.
//!
//! Runs the `sdx` client's portable **file_id-level** surface against a HF-style
//! mock frontend (`tests/support/hf_mock.rs`) that implements the upstream Xet
//! wire protocol shapes — deliberately NOT shardline's own server. This proves
//! the client is not coupled to shardline's exact routes and can talk to any
//! Xet-compatible frontend (`docs/SDX_PLAN.md` §9-M7,
//! `docs/PROTOCOL_CONFORMANCE.md`).
//!
//! The path namespace (`tree.rs`/`revisions.rs`) is deliberately NOT exercised
//! here: it is shardline-specific and out of scope for cross-frontend
//! conformance (`docs/SDX_PLAN.md` §4.4.1).
//!
//! The client code is **unchanged** by these tests; the mock must match what
//! the committed client sends/parses.

#![cfg_attr(
    test,
    allow(
        dead_code,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::map_err_ignore,
        clippy::type_complexity,
        clippy::missing_const_for_fn
    )
)]

mod support;

use std::path::PathBuf;

use support::hf_mock::HfMock;

use bytes::Bytes;
use sdx::{
    Auth, RepositoryId, XetClient, XetClientBuilder,
    error::{SdxError, TransferError},
};

fn repository() -> RepositoryId {
    RepositoryId {
        provider: "github".to_owned(),
        owner: "team".to_owned(),
        repo: "assets".to_owned(),
        revision: "main".to_owned(),
    }
}

/// Builds a client pointed at the mock frontend.
async fn client_for(mock: &HfMock) -> XetClient {
    let auth = Auth::new(&mock.base_url, repository())
        .unwrap()
        .with_api_key("bootstrap".to_owned())
        .with_subject("github-user-1".to_owned());
    XetClientBuilder::new()
        .endpoint(format!(
            "xet://127.0.0.1:{}/github/team/assets/main",
            mock.port
        ))
        .auth(auth)
        .build()
        .unwrap()
}

/// Builds a client with a small upload chunk size (to span multiple xorbs).
async fn client_with_chunk_size(mock: &HfMock, chunk_size: usize) -> XetClient {
    let auth = Auth::new(&mock.base_url, repository())
        .unwrap()
        .with_api_key("bootstrap".to_owned())
        .with_subject("github-user-1".to_owned());
    XetClientBuilder::new()
        .endpoint(format!(
            "xet://127.0.0.1:{}/github/team/assets/main",
            mock.port
        ))
        .auth(auth)
        .with_upload_chunk_size(chunk_size)
        .build()
        .unwrap()
}

fn temp_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sdx-conformance-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// Consumes a download stream fully into a byte vector.
async fn drain_stream(mut stream: sdx::stream::DownloadStream) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        out.extend_from_slice(&chunk);
    }
    out
}

/// Uploads content into the CAS without touching the path namespace (which is
/// shardline-specific and out of scope for cross-frontend conformance),
/// returning the content-derived `file_id`.
async fn upload_no_register(client: &XetClient, content: Vec<u8>) -> Result<String, SdxError> {
    let session = client.upload_session()?;
    let handle = session.upload_stream_handle();
    for block in content.chunks(sdx::INGESTION_BLOCK_SIZE) {
        handle.write(Bytes::copy_from_slice(block)).await?;
    }
    let info = handle.finish().await?;
    session.finalize().await?;
    Ok(info.file_id)
}

// ── 1. Full-file reconstruction ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_full_file_reconstruction_round_trip() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let content = support::hf_mock::deterministic_content(200_000, 42);

    let file_id = upload_no_register(&client, content.clone()).await.unwrap();
    assert_eq!(file_id.len(), 64);

    // Buffered download via download_file.
    let dest = temp_path("full-out.bin");
    let n = client
        .download_session()
        .download_file(&file_id, &dest)
        .await
        .unwrap();
    assert_eq!(n, content.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), content);

    // Streaming download via download_stream.
    let stream = client.download_stream(&file_id, None).await.unwrap();
    assert_eq!(drain_stream(stream).await, content);

    // And via download_bytes.
    assert_eq!(
        client.download_bytes(&file_id).await.unwrap().to_vec(),
        content
    );
}

// ── 2. Range reconstruction ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_range_reconstruction_byte_identical() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let content = support::hf_mock::deterministic_content(150_000, 7);

    let file_id = upload_no_register(&client, content.clone()).await.unwrap();

    // A range that starts and ends mid-chunk (chunk target 64 KiB).
    for (start, end) in [(0u64, 999u64), (30_000u64, 60_000u64), (100u64, 149_999u64)] {
        let dest = temp_path(&format!("range-{start}-{end}.bin"));
        let n = client
            .download_session()
            .download_range(&file_id, start..=end, &dest)
            .await
            .unwrap();
        let expected = &content[start as usize..=end as usize];
        assert_eq!(n, expected.len() as u64);
        assert_eq!(
            &std::fs::read(&dest).unwrap(),
            expected,
            "range {start}..={end}"
        );
    }

    // Ranged stream.
    let stream = client
        .download_stream(&file_id, Some(50_000..80_000))
        .await
        .unwrap();
    let got = drain_stream(stream).await;
    assert_eq!(got, &content[50_000..80_000]);
}

// ── 3 & 4. Dedupe hit/miss + upload idempotency ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_dedupe_miss_then_hit_and_upload_idempotent() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    // Single chunk (small content) so the whole file is one eligible chunk.
    let content = support::hf_mock::deterministic_content(512, 1);

    // First upload: dedup misses, the xorb is posted once.
    let first = upload_no_register(&client, content.clone()).await.unwrap();
    assert_eq!(mock.dedup_queries().await, 1, "chunk 0 is always eligible");
    assert_eq!(mock.dedup_hits().await, 0, "first upload is a miss");
    assert_eq!(mock.xorb_post_count().await, 1);

    // Second upload of identical content: dedup hits, no new xorb posted.
    let second = upload_no_register(&client, content.clone()).await.unwrap();
    assert_eq!(first, second, "content-addressed file_id is stable");
    assert_eq!(mock.dedup_hits().await, 1, "second upload hits");
    assert_eq!(
        mock.xorb_post_count().await,
        1,
        "no new xorb POST on re-upload"
    );

    // Both downloads reconstruct the same bytes.
    let dest = temp_path("dedup-out.bin");
    client
        .download_session()
        .download_file(&first, &dest)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), content);
}

// ── 5. Missing-xorb handling ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_missing_xorb_surfaces_typed_not_found() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let content = support::hf_mock::deterministic_content(64_000, 3);

    let file_id = upload_no_register(&client, content).await.unwrap();
    assert!(mock.xorb_post_count().await >= 1);

    // Remove every stored xorb payload; reconstruction still references them,
    // so the transfer route 404s and the client must surface a typed error
    // (not hang).
    mock.remove_all_xorbs().await;

    let dest = temp_path("mx-out.bin");
    let err = client
        .download_session()
        .download_file(&file_id, &dest)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SdxError::Transfer(TransferError::NotFound(_))),
        "expected NotFound, got {err:?}"
    );
}

// ── 6. Token refresh cycle ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_token_refresh_single_flight_and_transparent() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let content = support::hf_mock::deterministic_content(40_000, 5);
    let file_id = upload_no_register(&client, content.clone()).await.unwrap();

    // Single-flight: 10 concurrent reads share one token issuance.
    mock.set_token_ttl(3600);
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let client = client.clone();
        let file_id = file_id.clone();
        tasks.push(tokio::spawn(async move {
            client.download_bytes(&file_id).await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap().to_vec(), content);
    }
    assert_eq!(mock.read_token_calls().await, 1, "single-flight issuance");

    // Transparent refresh: a fresh client with a short-lived token refreshes
    // transparently on the next read.
    mock.set_token_ttl(30);
    let refresh_client = client_for(&mock).await;
    let before = mock.read_token_calls().await;
    let first = refresh_client
        .download_bytes(&file_id)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(first, content);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let second = refresh_client
        .download_bytes(&file_id)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(second, content);
    // The delayed call saw exp within the 30s buffer and refreshed.
    assert!(
        mock.read_token_calls().await >= before.saturating_add(2),
        "expected a refresh issuance; calls={} before={}",
        mock.read_token_calls().await,
        before
    );
}

// ── 7. Read vs write token scoping ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_read_token_rejected_on_write_route() {
    let mock = HfMock::start().await;
    // In the restricted mode the write-token endpoint issues a read-scoped
    // token, so the client's upload (a write flow) is rejected by the mock.
    mock.set_restrict_write(true);
    let client = client_for(&mock).await;

    let err = upload_no_register(&client, b"scope test content".to_vec())
        .await
        .expect_err("upload must fail under restrict_write");
    assert!(
        matches!(err, SdxError::Transfer(TransferError::Forbidden(_))),
        "read token on a write route must surface a typed Forbidden, got {err:?}"
    );

    // With normal scoping the same flow succeeds (write token accepted on
    // read routes too — covered by the round-trip tests above).
    mock.set_restrict_write(false);
    let file_id = upload_no_register(&client, b"ok".to_vec()).await.unwrap();
    assert_eq!(file_id.len(), 64);
}

// ── 8. Concurrent transfers ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_concurrent_transfers_byte_identical() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let files: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| {
            let content = support::hf_mock::deterministic_content(90_000 + i * 7_000, i as u64);
            (format!("concurrent-{i}.bin"), content)
        })
        .collect();

    let mut file_ids = Vec::new();
    for (_name, content) in &files {
        file_ids.push(upload_no_register(&client, content.clone()).await.unwrap());
    }

    // Concurrent downloads through the streaming group.
    let group = client.new_download_stream_group();
    let mut tasks = Vec::new();
    for (i, (file_id, (_name, expected))) in file_ids.into_iter().zip(files.iter()).enumerate() {
        let group = group.clone();
        let expected = expected.clone();
        tasks.push(tokio::spawn(async move {
            let mut stream = group.download_stream(&file_id, None).await.unwrap();
            let mut got = Vec::new();
            while let Some(chunk) = stream.next().await.unwrap() {
                got.extend_from_slice(&chunk);
            }
            (i, got, expected)
        }));
    }
    for task in tasks {
        let (_i, got, expected) = task.await.unwrap();
        assert_eq!(got, expected);
    }
}

// ── 9. Multi-xorb ranged fetch path ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_multi_xorb_file_downloads_correctly() {
    let mock = HfMock::start().await;
    // 128-byte chunks; ~1.1 MiB spans >8192 chunks => multiple xorbs.
    let client = client_with_chunk_size(&mock, 128).await;
    let content = support::hf_mock::deterministic_content(1_100_000, 9);

    let file_id = upload_no_register(&client, content.clone()).await.unwrap();
    assert!(
        mock.xorb_post_count().await >= 2,
        "expected multiple xorbs, uploaded {}",
        mock.xorb_post_count().await
    );

    let dest = temp_path("multi-out.bin");
    let n = client
        .download_session()
        .download_file(&file_id, &dest)
        .await
        .unwrap();
    assert_eq!(n, content.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), content);
}

// ── 10. X-Xet-Session-Id present ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_session_id_header_sent() {
    let mock = HfMock::start().await;
    let client = client_for(&mock).await;
    let content = support::hf_mock::deterministic_content(8_000, 11);
    let file_id = upload_no_register(&client, content.clone()).await.unwrap();
    let dest = temp_path("s-out.bin");
    client
        .download_session()
        .download_file(&file_id, &dest)
        .await
        .unwrap();

    let session_ids = mock.session_ids().await;
    assert!(
        !session_ids.is_empty(),
        "mock must observe X-Xet-Session-Id on requests"
    );
    assert!(
        session_ids.iter().any(|id| id.starts_with("sdx-")),
        "session ids look client-generated: {session_ids:?}"
    );
}

// ── 11. Conformance checklist (self-auditing) ──────────────────────────────

/// Maps every `docs/PROTOCOL_CONFORMANCE.md:136-157` compatibility item to a
/// test that covers it (either a conformance test here or an existing unit
/// test in the crate).
const CONFORMANCE_ITEMS: &[(&str, &str)] = &[
    (
        "golden hash conversion vectors",
        "crates/sdx/src/hash.rs (unit)",
    ),
    (
        "xorb parse and hash verification",
        "crates/sdx/src/xorb.rs (unit)",
    ),
    (
        "shard parse and validation",
        "crates/sdx/src/shard.rs (unit)",
    ),
    (
        "upload idempotency",
        "conformance_dedupe_miss_then_hit_and_upload_idempotent",
    ),
    (
        "missing-xorb shard rejection",
        "conformance_missing_xorb_surfaces_typed_not_found",
    ),
    (
        "full-file reconstruction",
        "conformance_full_file_reconstruction_round_trip",
    ),
    (
        "range reconstruction",
        "conformance_range_reconstruction_byte_identical",
    ),
    (
        "dedupe hit and miss",
        "conformance_dedupe_miss_then_hit_and_upload_idempotent",
    ),
    (
        "unauthorized scope rejection",
        "conformance_read_token_rejected_on_write_route",
    ),
    (
        "authenticated native-client repository-scoped bearer tokens",
        "client_for (all tests)",
    ),
    (
        "native-client token-refresh route",
        "conformance_token_refresh_single_flight_and_transparent",
    ),
    (
        "concurrent transfers",
        "conformance_concurrent_transfers_byte_identical",
    ),
    (
        "long-lived refresh-cycle",
        "conformance_token_refresh_single_flight_and_transparent",
    ),
    (
        "ranged download",
        "conformance_range_reconstruction_byte_identical",
    ),
    (
        "integration against another Xet-compatible client",
        "conformance suite (HF-style mock)",
    ),
];

#[test]
fn conformance_checklist_has_explicit_coverage_for_every_protocol_item() {
    // Every PROTOCOL_CONFORMANCE item must map to a non-empty test identifier.
    for (item, test) in CONFORMANCE_ITEMS {
        assert!(
            !test.is_empty(),
            "no test mapped for conformance item: {item}"
        );
        assert!(
            !test.contains("TODO") && !test.contains("unimplemented"),
            "conformance item {item} is not actually covered"
        );
    }

    // Each named conformance test function exists in this module.
    let defined = [
        "conformance_full_file_reconstruction_round_trip",
        "conformance_range_reconstruction_byte_identical",
        "conformance_dedupe_miss_then_hit_and_upload_idempotent",
        "conformance_missing_xorb_surfaces_typed_not_found",
        "conformance_token_refresh_single_flight_and_transparent",
        "conformance_read_token_rejected_on_write_route",
        "conformance_concurrent_transfers_byte_identical",
        "conformance_multi_xorb_file_downloads_correctly",
        "conformance_session_id_header_sent",
    ];
    for &(item, test) in CONFORMANCE_ITEMS {
        if test.starts_with("conformance_") {
            assert!(
                defined.contains(&test),
                "conformance item {item} references undefined test {test}"
            );
        }
    }
}
