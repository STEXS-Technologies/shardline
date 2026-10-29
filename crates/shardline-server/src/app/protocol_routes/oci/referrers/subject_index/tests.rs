use super::*;
use crate::app::protocol_routes::oci::{
    helpers::oci_manifest_key,
    test_helpers::{build_oci_test_state, oci_test_router},
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt as _;

#[tokio::test]
async fn stale_inventory_after_reclamation_does_not_fail_reconciliation() {
    let context = build_oci_test_state().await;
    let state = &context.state;
    let auth = AuthorizedRepository::anonymous_full_access();
    let repository = "index/reclaimed";
    let document = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[],"subject":{"digest":format!("sha256:{}", "a".repeat(64)),"size":1}});
    let bytes = serde_json::to_vec(&document).unwrap();
    let digest = hex::encode(Sha256::digest(&bytes));
    let key = oci_manifest_key(repository, &digest, &auth).unwrap();
    state
        .backend
        .put_object_bytes_if_absent(&key, bytes.clone())
        .await
        .unwrap();
    state.backend.delete_object_if_present(&key).await.unwrap();
    // A cached inventory entry can name bytes GC has removed before the
    // reconciliation read. It must not manufacture a mapping or return 404.
    reconcile_manifest(state, repository, &auth, &digest, &key)
        .await
        .unwrap();
    assert!(
        page(state, repository, &auth, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    state
        .backend
        .put_object_bytes_if_absent(&key, b"wrong content address".to_vec())
        .await
        .unwrap();
    assert!(matches!(
        reconcile_manifest(state, repository, &auth, &digest, &key).await,
        Err(ServerError::InvalidManifestReference)
    ));
    assert!(
        page(state, repository, &auth, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    state.backend.delete_object_if_present(&key).await.unwrap();
    state
        .backend
        .put_object_bytes_if_absent(&key, bytes)
        .await
        .unwrap();
    reconcile_manifest(state, repository, &auth, &digest, &key)
        .await
        .unwrap();
    assert_eq!(
        page(state, repository, &auth, None, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_merges_multiple_inventory_and_mapping_pages() {
    let context = build_oci_test_state().await;
    let state = &context.state;
    let auth = AuthorizedRepository::anonymous_full_access();
    let repository = "index/pages";
    let subject = "a".repeat(64);
    let mut expected = BTreeSet::new();
    for number in 0..1002 {
        let document = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json", "manifests":[], "annotations":{"fixture":number.to_string()}, "subject":{"digest":format!("sha256:{subject}"),"size":1}});
        let bytes = serde_json::to_vec(&document).unwrap();
        let digest = hex::encode(Sha256::digest(&bytes));
        let key = oci_manifest_key(repository, &digest, &auth).unwrap();
        state
            .backend
            .put_object_bytes_if_absent(&key, bytes)
            .await
            .unwrap();
        if number % 3 != 0 {
            retain(
                state,
                mapping(repository, &auth, &digest, &document).unwrap(),
            )
            .await
            .unwrap();
        }
        expected.insert(digest);
    }
    // Missing legacy rows and known rows interleave across both page boundaries.
    reconcile(state, repository, &auth).await.unwrap();
    reconcile(state, repository, &auth).await.unwrap();
    let first = page(state, repository, &auth, Some(&subject), None)
        .await
        .unwrap();
    assert_eq!(first.len(), PAGE);
    let second = page(
        state,
        repository,
        &auth,
        Some(&subject),
        Some(&first.last().unwrap().digest_hex),
    )
    .await
    .unwrap();
    assert_eq!(second.len(), 2);
    let actual: BTreeSet<_> = first
        .into_iter()
        .chain(second)
        .map(|entry| entry.digest_hex)
        .collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn indexed_referrer_still_verifies_authoritative_manifest_bytes() {
    let context = build_oci_test_state().await;
    let state = &context.state;
    let auth = AuthorizedRepository::anonymous_full_access();
    let repository = "index/integrity";
    let subject = "a".repeat(64);
    let document = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json", "manifests":[], "subject":{"digest":format!("sha256:{subject}"),"size":1}});
    let bytes = serde_json::to_vec(&document).unwrap();
    let digest = hex::encode(Sha256::digest(&bytes));
    let key = oci_manifest_key(repository, &digest, &auth).unwrap();
    state
        .backend
        .put_object_bytes_if_absent(&key, bytes.clone())
        .await
        .unwrap();
    retain(
        state,
        mapping(repository, &auth, &digest, &document).unwrap(),
    )
    .await
    .unwrap();
    let app = oci_test_router(state);
    let request = || {
        Request::builder()
            .uri(format!("/v2/{repository}/referrers/sha256:{subject}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        app.clone().oneshot(request()).await.unwrap().status(),
        StatusCode::OK
    );
    // Replace the stored bytes and their adapter checksum. The independent
    // manifest-address digest must still prevent a successful corrupt response.
    state
        .backend
        .put_object_bytes_overwrite(&key, b"{}".to_vec())
        .await
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request()).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    state
        .backend
        .put_object_bytes_overwrite(&key, bytes)
        .await
        .unwrap();
    assert_eq!(
        app.oneshot(request()).await.unwrap().status(),
        StatusCode::OK
    );
}
