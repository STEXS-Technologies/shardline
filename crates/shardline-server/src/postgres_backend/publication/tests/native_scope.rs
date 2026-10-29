use crate::{
    ShardMetadataLimits,
    maintenance_barrier::RequestSession,
    test_fixtures::{single_chunk_xorb, single_file_shard},
    test_support::postgres_fixture::PostgresFixture,
    upload_ingest::RequestBodyReader,
};
use shardline_index::{UploadIntentState, UploadIntentStore};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

#[tokio::test]
async fn native_shard_publication_and_retries_have_independent_repository_evidence() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: native repository reuse requires DATABASE_URL");
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let payload = b"shared native content across repositories";
    let (xorb, hash) = single_chunk_xorb(payload);
    backend.upload_xorb(&hash, xorb).await.unwrap();
    let (shard, file_hash) = single_file_shard(&[(payload, &hash)]);
    let body_hash = blake3::hash(&shard).to_hex().to_string();
    let legacy = shardline_index::UploadIntent::new(
        format!("shard-{body_hash}"),
        "shards/legacy".to_owned(),
        body_hash.clone(),
        shard.len() as u64,
    );
    fixture
        .index()
        .create_intent_scoped(&legacy, "shardline", "github:legacy/assets/main")
        .await
        .unwrap();
    fixture
        .index()
        .transition_intent(legacy.intent_id(), UploadIntentState::Storing)
        .await
        .unwrap();
    let legacy_events = serde_json::to_value(
        fixture
            .index()
            .reliability_events(legacy.intent_id())
            .await
            .unwrap(),
    )
    .unwrap();
    let scopes = [
        None,
        Some(
            RepositoryScope::new(RepositoryProvider::GitHub, "team-a", "assets", Some("main"))
                .unwrap(),
        ),
        Some(
            RepositoryScope::new(RepositoryProvider::GitHub, "team-b", "assets", Some("main"))
                .unwrap(),
        ),
        Some(RepositoryScope::new(RepositoryProvider::GitHub, "team/a", "assets", None).unwrap()),
        Some(RepositoryScope::new(RepositoryProvider::GitHub, "team", "a/assets", None).unwrap()),
        Some(
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "a/assets", Some("-"))
                .unwrap(),
        ),
    ];
    let mut ids = std::collections::HashSet::new();
    for scope in &scopes {
        let (id, repository) =
            crate::upload_identity::native_shard(&body_hash, scope.as_ref()).unwrap();
        assert!(ids.insert(id.clone()));
        for _retry in 0..2 {
            let session = RequestSession::acquire(
                &backend.gc_barrier_pool,
                backend.request_session_identity.clone(),
            )
            .await
            .unwrap();
            session
                .scope(backend.upload_shard_stream(
                    RequestBodyReader::from_bytes(shard.clone()),
                    scope.as_ref(),
                    ShardMetadataLimits::default(),
                ))
                .await
                .unwrap();
            session.finish().await;
            assert_eq!(
                fixture
                    .index()
                    .intent_by_id(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state(),
                UploadIntentState::Visible
            );
            let events = fixture.index().reliability_events(&id).await.unwrap();
            assert_eq!(events.len(), 5);
            assert!(
                events
                    .iter()
                    .all(|event| event.operation.repository == repository)
            );
        }
        assert_eq!(
            backend
                .download_file(&file_hash, None, scope.as_ref())
                .await
                .unwrap()
                .as_slice(),
            payload
        );
    }
    assert_eq!(
        fixture
            .index()
            .intent_by_id(legacy.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
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
        legacy_events
    );
    fixture.finish().await;
}

#[tokio::test]
async fn historical_server_preserves_every_new_native_checkpoint() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_LEGACY_RECOVERY_BINARY")
    else {
        return;
    };
    verify_released_native_recovery(binary, true).await;
}

#[tokio::test]
async fn released_server_recovers_every_native_checkpoint() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_N_MINUS_ONE_BINARY")
    else {
        return;
    };
    verify_released_native_recovery(binary, false).await;
}

async fn verify_released_native_recovery(binary: String, historical: bool) {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: native rollback requires DATABASE_URL");
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let cases = crate::test_support::native_upload_rollback::seed(
        fixture.index(),
        &fixture.postgres().object_store(),
    )
    .await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute'",
        fixture.schema
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    let logs = crate::test_support::released_startup::run(
        binary.into(),
        fixture.config.root_dir().to_path_buf(),
        Some(fixture.config.index_postgres_url().unwrap().to_owned()),
    )
    .await;
    if historical {
        crate::test_support::native_upload_rollback::assert_preserved(
            fixture.index(),
            &cases,
            &logs,
        )
        .await;
    } else {
        crate::test_support::native_upload_rollback::assert_recovered(
            fixture.index(),
            &cases,
            &logs,
        )
        .await;
    }
    for case in &cases {
        if historical && case.state == UploadIntentState::Created {
            assert!(
                fixture
                    .index()
                    .transition_intent(case.intent.intent_id(), UploadIntentState::Storing)
                    .await
                    .unwrap()
            );
        }
    }
    fixture.finish().await;
}
