use sha2::{Digest, Sha256};

use super::*;
use crate::{LocalBackend, ServerBackend, upload_ingest::RequestBodyReader};
use shardline_storage::ObjectKey;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn digest_protocol_uses_containers_and_preserves_digest_rejection() {
    let provider = Provider::new(0).await;
    let root = tempfile::tempdir().unwrap();
    let local = LocalBackend::new_with_object_store(
        root.path().to_owned(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(64 * 1024).unwrap(),
        provider.objects.clone(),
    )
    .await
    .unwrap();
    let backend = ServerBackend::Local(local);
    let bytes: Vec<_> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
    let digest = hex::encode(Sha256::digest(&bytes));
    let key = ObjectKey::parse(&format!("lfs/repo/objects/{digest}")).unwrap();
    assert_eq!(
        backend
            .put_sha256_addressed_object_stream_if_absent(
                &key,
                &digest,
                RequestBodyReader::from_bytes(Bytes::copy_from_slice(&bytes)),
            )
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(backend.read_object(&key).await.unwrap(), bytes);
    assert!(
        provider
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .all(|key| key.contains("/xorbs/"))
    );
    assert_eq!(
        backend
            .put_sha256_addressed_object_stream_if_absent(
                &key,
                &digest,
                RequestBodyReader::from_bytes(Bytes::copy_from_slice(&bytes)),
            )
            .await
            .unwrap(),
        PutOutcome::AlreadyExists
    );
    // Existing objects must still consume and validate the complete body.
    assert!(
        backend
            .put_sha256_addressed_object_stream_if_absent(
                &key,
                &digest,
                RequestBodyReader::from_bytes(Bytes::from_static(b"wrong")),
            )
            .await
            .is_err()
    );
    let wrong_digest = "0".repeat(64);
    let wrong_key = ObjectKey::parse(&format!("lfs/repo/objects/{wrong_digest}")).unwrap();
    assert!(
        backend
            .put_sha256_addressed_object_stream_if_absent(
                &wrong_key,
                &wrong_digest,
                RequestBodyReader::from_bytes(Bytes::copy_from_slice(&bytes)),
            )
            .await
            .is_err()
    );
    assert!(matches!(
        backend.read_object(&wrong_key).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_upload_keeps_standalone_chunk_objects() {
    let provider = Provider::new(0).await;
    let root = tempfile::tempdir().unwrap();
    let local = LocalBackend::new_with_object_store(
        root.path().to_owned(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(64 * 1024).unwrap(),
        provider.objects.clone(),
    )
    .await
    .unwrap();
    let bytes = Bytes::from(vec![b'a'; 100 * 1024]);
    let response = local
        .upload_file("public-upload", bytes, None)
        .await
        .unwrap();
    let state = provider.state.lock().unwrap();
    for chunk in response.chunks {
        let object_key = crate::chunk_store::chunk_object_key(&chunk.hash).unwrap();
        assert!(
            state
                .objects
                .keys()
                .any(|key| key.ends_with(object_key.as_str()))
        );
    }
}
