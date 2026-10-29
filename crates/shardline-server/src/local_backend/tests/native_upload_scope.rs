//! The same content-addressed shard may publish into distinct repositories.
use axum::body::Bytes;
use shardline_index::{UploadIntentState, UploadIntentStore};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use std::num::NonZeroUsize;

use crate::{
    ShardMetadataLimits,
    local_backend::LocalBackend,
    test_fixtures::{single_chunk_xorb, single_file_shard},
    upload_ingest::RequestBodyReader,
};

#[tokio::test]
async fn same_native_shard_publishes_independently_in_default_and_repository_scopes() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
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
    backend
        .index_store
        .create_intent_scoped(&legacy, "shardline", "github:legacy/assets/main")
        .await
        .unwrap();
    backend
        .index_store
        .transition_intent(legacy.intent_id(), UploadIntentState::Storing)
        .await
        .unwrap();
    let legacy_events = serde_json::to_value(
        backend
            .index_store
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
            backend
                .upload_shard_stream(
                    RequestBodyReader::from_bytes(Bytes::clone(&shard)),
                    scope.as_ref(),
                    ShardMetadataLimits::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                backend
                    .index_store
                    .intent_by_id(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state(),
                UploadIntentState::Visible
            );
            let events = backend.index_store.reliability_events(&id).await.unwrap();
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
        backend
            .index_store
            .intent_by_id(legacy.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
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
        legacy_events
    );
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
async fn released_server_recovers_native_checkpoints_and_allows_identical_retry() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_N_MINUS_ONE_BINARY")
    else {
        return;
    };
    verify_released_native_recovery(binary, false).await;
}

async fn verify_released_native_recovery(binary: String, historical: bool) {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let cases = crate::test_support::native_upload_rollback::seed(
        &backend.index_store,
        &backend.object_store(),
    )
    .await;
    rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60",
            [],
        )
        .unwrap();
    let logs =
        crate::test_support::released_startup::run(binary.into(), root.path().to_path_buf(), None)
            .await;
    if historical {
        crate::test_support::native_upload_rollback::assert_preserved(
            &backend.index_store,
            &cases,
            &logs,
        )
        .await;
    } else {
        crate::test_support::native_upload_rollback::assert_recovered(
            &backend.index_store,
            &cases,
            &logs,
        )
        .await;
        for ordinal in 0..4 {
            let payload = format!("native rollback request false {ordinal}");
            let (xorb, hash) = single_chunk_xorb(payload.as_bytes());
            backend.upload_xorb(&hash, xorb).await.unwrap();
        }
    }
    for case in &cases {
        if historical && case.state == UploadIntentState::Created {
            assert!(
                backend
                    .index_store
                    .transition_intent(case.intent.intent_id(), UploadIntentState::Storing)
                    .await
                    .unwrap()
            );
        }
    }
    assert_eq!(
        rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
            .unwrap()
            .query_row(
                "SELECT count(*) FROM shardline_upload_writers_v2",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}
