use crate::{
    maintenance_barrier::RequestSession, test_fixtures::footerless_xorb,
    test_support::postgres_fixture::PostgresFixture,
};
use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};

#[tokio::test]
async fn native_upload_and_retry_do_not_checkout_metadata_beside_their_owner() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(b"owned native guard");
    let session = RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    // The fixture has a separate, single-connection metadata pool. Occupy it
    // completely: an admitted owner must not need another checkout to progress.
    let reserved = fixture.index().pool().acquire().await.unwrap();
    let result = session
        .scope(tokio::time::timeout(
            std::time::Duration::from_secs(3),
            async {
                for _ in 0..2 {
                    backend.upload_xorb(&hash, xorb.clone()).await?;
                }
                Ok::<_, crate::ServerError>(())
            },
        ))
        .await;
    drop(reserved);
    session.finish().await;
    fixture.finish().await;
    result
        .expect("native uploads must progress without a second metadata checkout")
        .expect("new and completed native intents must use their original owner");
}

#[tokio::test]
async fn failed_native_attempts_preserve_history_and_allow_verified_retries() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: failed native retries require DATABASE_URL");
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let payload = b"failed native attempt retry";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload);
    let xorb_intent = UploadIntent::new(
        crate::upload_identity::xorb(&hash, xorb.len() as u64),
        crate::xet_adapter::xorb_object_key(&hash)
            .unwrap()
            .as_str()
            .to_owned(),
        hash.clone(),
        xorb.len() as u64,
    );
    let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(payload, &hash)]);
    let body_hash = blake3::hash(&shard).to_hex().to_string();
    let (shard_id, _) = crate::upload_identity::native_shard(&body_hash, None).unwrap();
    let shard_intent = UploadIntent::new(
        shard_id,
        format!("shards/{}/{body_hash}.shard", &body_hash[..2]),
        body_hash,
        shard.len() as u64,
    );
    let mut evidence = Vec::new();
    for intent in [&xorb_intent, &shard_intent] {
        fixture.index().create_intent(intent).await.unwrap();
        assert!(
            fixture
                .index()
                .transition_intent(intent.intent_id(), UploadIntentState::Failed)
                .await
                .unwrap()
        );
        evidence.push(
            serde_json::to_value(
                fixture
                    .index()
                    .reliability_events(intent.intent_id())
                    .await
                    .unwrap(),
            )
            .unwrap(),
        );
    }
    backend.upload_xorb(&hash, xorb).await.unwrap();
    backend
        .upload_shard_stream(
            crate::upload_ingest::RequestBodyReader::from_bytes(shard),
            None,
            crate::ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    for (intent, before) in [&xorb_intent, &shard_intent].into_iter().zip(evidence) {
        assert_eq!(
            fixture
                .index()
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Failed
        );
        assert_eq!(
            serde_json::to_value(
                fixture
                    .index()
                    .reliability_events(intent.intent_id())
                    .await
                    .unwrap()
            )
            .unwrap(),
            before
        );
        assert_eq!(
            fixture
                .index()
                .intent_by_id(&format!("{}-retry-1", intent.intent_id()))
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
    }
    assert!(backend.read_record(&file_hash, None, None).await.is_ok());
    fixture.finish().await;
}

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
    recover_encoding_at_every_checkpoint(footerless_xorb).await;
}

#[tokio::test]
async fn alternate_xorb_encoding_recovery_preserves_every_checkpoint() {
    recover_encoding_at_every_checkpoint(crate::test_fixtures::alternate_xorb_encodings).await;
}

async fn recover_encoding_at_every_checkpoint(
    encoding: fn() -> std::io::Result<(axum::body::Bytes, axum::body::Bytes, String)>,
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
        let (wire, canonical, hash) = encoding().unwrap();
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
