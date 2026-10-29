use crate::{local_backend::LocalBackend, test_fixtures::footerless_xorb};
use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
use std::num::NonZeroUsize;

#[tokio::test]
async fn footerless_xorb_retry_accepts_its_canonical_serialization() {
    for canonical_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            root.path().to_path_buf(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(128).unwrap(),
        )
        .await
        .unwrap();
        let (wire, canonical, hash) = footerless_xorb().unwrap();
        assert!(canonical.len() > wire.len());
        let legacy = UploadIntent::new(
            format!("xorb-{hash}"),
            crate::xet_adapter::xorb_object_key(&hash)
                .unwrap()
                .as_str()
                .to_owned(),
            hash.clone(),
            wire.len() as u64,
        );
        backend.index_store.create_intent(&legacy).await.unwrap();
        let before = serde_json::to_value(
            backend
                .index_store
                .reliability_events(legacy.intent_id())
                .await
                .unwrap(),
        )
        .unwrap();
        let bodies = if canonical_first {
            [canonical.clone(), wire.clone(), canonical.clone()]
        } else {
            [wire.clone(), canonical.clone(), wire]
        };
        for body in bodies {
            backend.upload_xorb(&hash, body).await.unwrap();
        }
        let id = crate::upload_identity::xorb(&hash, canonical.len() as u64);
        let intent = backend
            .index_store
            .intent_by_id(&id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(intent.state(), UploadIntentState::Visible);
        assert_eq!(
            intent.object_length(),
            backend.xorb_length(&hash).await.unwrap()
        );
        assert_eq!(
            backend
                .index_store
                .reliability_events(&id)
                .await
                .unwrap()
                .len(),
            5
        );
        assert_eq!(
            serde_json::to_value(
                backend
                    .index_store
                    .reliability_events(legacy.intent_id())
                    .await
                    .unwrap()
            )
            .unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn footerless_xorb_recovery_uses_stored_length_at_every_checkpoint() {
    recover_encoding_at_every_checkpoint(footerless_xorb).await;
}

#[tokio::test]
async fn alternate_xorb_encoding_recovery_preserves_every_checkpoint() {
    recover_encoding_at_every_checkpoint(crate::test_fixtures::alternate_xorb_encodings).await;
}

async fn recover_encoding_at_every_checkpoint(
    fixture: fn() -> std::io::Result<(axum::body::Bytes, axum::body::Bytes, String)>,
) {
    for (ordinal, state) in [
        UploadIntentState::Created,
        UploadIntentState::Storing,
        UploadIntentState::Stored,
        UploadIntentState::MetadataCommitted,
    ]
    .into_iter()
    .enumerate()
    {
        let root = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            root.path().to_path_buf(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(128).unwrap(),
        )
        .await
        .unwrap();
        let (wire, canonical, hash) = fixture().unwrap();
        let intent = UploadIntent::new(
            crate::upload_identity::xorb(&hash, canonical.len() as u64),
            crate::xet_adapter::xorb_object_key(&hash)
                .unwrap()
                .as_str()
                .to_owned(),
            hash.clone(),
            canonical.len() as u64,
        );
        backend.index_store.create_intent(&intent).await.unwrap();
        for next in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ]
        .into_iter()
        .take(ordinal)
        {
            assert!(
                backend
                    .index_store
                    .transition_intent(intent.intent_id(), next)
                    .await
                    .unwrap()
            );
        }
        crate::xet_adapter::store_uploaded_xorb_bytes(&backend.object_store(), &hash, &wire)
            .await
            .unwrap();
        backend
            .reconcile_upload_intents(
                std::time::Duration::ZERO,
                &crate::upload_intent_recovery::startup::RecoveryPass::Complete,
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .index_store
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            if state == UploadIntentState::MetadataCommitted {
                UploadIntentState::Visible
            } else {
                state
            }
        );
        backend.upload_xorb(&hash, canonical).await.unwrap();
        assert_eq!(
            backend
                .index_store
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert_eq!(
            backend
                .index_store
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            5
        );
    }
}
