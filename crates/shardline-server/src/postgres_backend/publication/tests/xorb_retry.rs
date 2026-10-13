use crate::{
    maintenance_barrier::RequestSession, test_fixtures::footerless_xorb,
    test_support::postgres_fixture::PostgresFixture,
};
use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};

#[tokio::test]
async fn footerless_and_canonical_xorb_retries_preserve_legacy_history() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: canonical xorb retries require DATABASE_URL");
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
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
    fixture.index().create_intent(&legacy).await.unwrap();
    let before = serde_json::to_value(
        fixture
            .index()
            .reliability_events(legacy.intent_id())
            .await
            .unwrap(),
    )
    .unwrap();
    let session = RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            for body in [wire.clone(), canonical.clone(), wire] {
                backend.upload_xorb(&hash, body).await.unwrap();
            }
        })
        .await;
    session.finish().await;
    let id = crate::upload_identity::xorb(&hash, canonical.len() as u64);
    let intent = fixture.index().intent_by_id(&id).await.unwrap().unwrap();
    assert_eq!(intent.state(), UploadIntentState::Visible);
    assert_eq!(
        intent.object_length(),
        backend.xorb_length(&hash).await.unwrap()
    );
    assert_eq!(
        fixture.index().reliability_events(&id).await.unwrap().len(),
        5
    );
    assert_eq!(
        serde_json::to_value(
            fixture
                .index()
                .reliability_events(legacy.intent_id())
                .await
                .unwrap()
        )
        .unwrap(),
        before
    );
    fixture.finish().await;
}

#[tokio::test]
async fn footerless_xorb_recovery_uses_stored_length_at_every_checkpoint() {
    for (ordinal, state) in [
        UploadIntentState::Created,
        UploadIntentState::Storing,
        UploadIntentState::Stored,
        UploadIntentState::MetadataCommitted,
    ]
    .into_iter()
    .enumerate()
    {
        let Some(fixture) = PostgresFixture::new().await else {
            eprintln!("SKIPPED: canonical xorb recovery requires DATABASE_URL");
            return;
        };
        fixture
            .index()
            .initialize_remote_delete_guard()
            .await
            .unwrap();
        let backend = fixture.postgres();
        let (wire, canonical, hash) = footerless_xorb().unwrap();
        let intent = UploadIntent::new(
            crate::upload_identity::xorb(&hash, canonical.len() as u64),
            crate::xet_adapter::xorb_object_key(&hash)
                .unwrap()
                .as_str()
                .to_owned(),
            hash.clone(),
            canonical.len() as u64,
        );
        fixture.index().create_intent(&intent).await.unwrap();
        for next in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ]
        .into_iter()
        .take(ordinal)
        {
            assert!(
                fixture
                    .index()
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
            fixture
                .index()
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
            fixture
                .index()
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert_eq!(
            fixture
                .index()
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            5
        );
        fixture.finish().await;
    }
}
