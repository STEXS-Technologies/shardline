//! Exercise the file-specific proof through the complete transfer handler.
use super::*;
use crate::backend::{
    forget_repository_reference_probe_count, repository_reference_probe_count,
    reset_repository_reference_probe_count_for_hash,
};

#[tokio::test]
async fn signed_transfer_uses_file_proof_and_retains_historical_fallback() {
    let (app, root) = test_app(&[ServerFrontend::Xet]).await;
    let content = b"file-specific HTTP authorization must preserve verified bytes";
    let (encoded, hash) = test_fixtures::single_chunk_xorb(content);
    let uploaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/transfer/xorb/default/{hash}"))
                .body(Body::from(encoded.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "hint", None).unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let mut record = FileRecord {
        file_id: "b".repeat(64),
        content_hash: "c".repeat(64),
        total_bytes: content.len() as u64,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length: content.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: encoded.len() as u64,
        }],
    };
    store.commit_file_version_metadata(&record).await.unwrap();
    // Put an incomplete proof before the named file in candidate order.
    let mut earlier = record.clone();
    earlier.file_id = "a".repeat(64);
    earlier.chunks[0].packed_end = 1;
    store.commit_file_version_metadata(&earlier).await.unwrap();
    let claims = TokenClaims::new("local", "reader", TokenScope::Read, scope, u64::MAX).unwrap();
    let signer = super::super::xorb_transfer_capability::XorbReadCapabilitySigner::new(
        b"0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    let end = encoded.len() as u64 - 1;
    let capability = signer
        .issue(&claims, &record.file_id, &hash, 1, end)
        .unwrap();
    let uri = format!("/transfer/xorb/default/{hash}?xorb_read_capability={capability}");
    reset_repository_reference_probe_count_for_hash(&hash);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(header::RANGE, format!("bytes=1-{end}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body_bytes(response).await.as_slice(),
        encoded.get(1..).unwrap()
    );
    assert_eq!(
        repository_reference_probe_count(&hash),
        0,
        "a complete named-file proof must avoid repository discovery"
    );
    // The old signed URL remains usable through the retained version after
    // replacement. The new current-file proof cannot contribute partial terms.
    record.content_hash = "d".repeat(64);
    record.chunks[0].packed_end = 1;
    store.commit_file_version_metadata(&record).await.unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(header::RANGE, format!("bytes=1-{end}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body_bytes(response).await.as_slice(),
        encoded.get(1..).unwrap()
    );
    assert_eq!(
        repository_reference_probe_count(&hash),
        1,
        "replacement must use the existing complete historical proof"
    );
    forget_repository_reference_probe_count(&hash);
}
