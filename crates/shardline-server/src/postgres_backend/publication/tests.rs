#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Publication must not expose chunks removed after immutable staging.
mod library_ownership;
mod native_scope;
mod xorb_retry;
use crate::{test_support::postgres_fixture::PostgresFixture, upload_ingest::RequestBodyReader};
use axum::body::Bytes;
use shardline_index::{ResourceLockKey, S3ObjectEntry, S3PublishCondition};
use shardline_storage::{ObjectKey, ObjectStore};

#[tokio::test]
async fn public_upload_preserves_advertised_chunks_in_postgres() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: advertised chunk fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let content = Bytes::from_static(b"postgres advertised chunk compatibility");
    let uploaded = backend
        .upload_file("advertised-chunks.bin", content.clone(), None)
        .await
        .unwrap();
    assert!(!uploaded.chunks.is_empty());
    for chunk in &uploaded.chunks {
        let stored = backend.read_chunk(&chunk.hash).await.unwrap();
        assert_eq!(
            backend.chunk_length(&chunk.hash).await.unwrap(),
            stored.len() as u64
        );
        let raw = lz4_flex::decompress_size_prepended(&stored).unwrap();
        let start = usize::try_from(chunk.offset).unwrap();
        let end = start + usize::try_from(chunk.length).unwrap();
        assert_eq!(raw, content.get(start..end).unwrap());
    }
    assert_eq!(
        backend
            .download_file("advertised-chunks.bin", None, None)
            .await
            .unwrap(),
        content
    );
    fixture.finish().await;
}

#[tokio::test]
async fn healthy_upload_owner_preserves_body_and_digest_errors() {
    use axum::response::IntoResponse;
    use shardline_index::{UploadIntentState, UploadIntentStore};
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: healthy upload ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session.scope(async {
        let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
            &backend.request_session_identity,
        ).await.unwrap();
        let original_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut **owner.as_mut().unwrap()).await.unwrap();
        drop(owner);
        let stream = futures_util::stream::once(async {
            Err(crate::ServerError::Io(std::io::Error::from(std::io::ErrorKind::InvalidData)))
        });
        let error = backend.upload_file_stream(
            "failed-body-file", RequestBodyReader::from_stream(stream), None, None,
        ).await.unwrap_err();
        assert!(matches!(&error, crate::ServerError::Io(io) if io.kind() == std::io::ErrorKind::InvalidData));
        let response = error.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key(axum::http::header::RETRY_AFTER));
        let error = backend.upload_file_stream(
            "failed-checksum-file", RequestBodyReader::from_bytes(Bytes::from_static(b"bad checksum")),
            None, Some(&"00".repeat(32)),
        ).await.unwrap_err();
        assert!(matches!(error, crate::ServerError::ExpectedBodyHashMismatch));
        let response = error.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(axum::http::header::RETRY_AFTER));
        backend.upload_file("healthy-owner-file", Bytes::from_static(b"healthy owner"), None)
            .await.unwrap();
        let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
            &backend.request_session_identity,
        ).await.unwrap();
        let current_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut **owner.as_mut().unwrap()).await.unwrap();
        assert_eq!(current_pid, original_pid);
    }).await;
    session.finish().await;
    for file_id in ["failed-body-file", "failed-checksum-file"] {
        assert!(matches!(
            backend.read_record(file_id, None, None).await,
            Err(crate::ServerError::NotFound)
        ));
    }
    let intent_id: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT intent_id FROM {}.shardline_upload_intents WHERE object_key='record/failed-checksum-file'",
        fixture.schema
    ))).fetch_one(&fixture.admin).await.unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
    );
    assert_eq!(
        fixture
            .index()
            .reliability_events(&intent_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        backend
            .read_record("healthy-owner-file", None, None)
            .await
            .is_ok()
    );
    backend
        .upload_file(
            "failed-checksum-file",
            Bytes::from_static(b"later valid upload"),
            None,
        )
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1",
        fixture.schema
    )))
    .bind(&intent_id)
    .execute(&fixture.admin)
    .await
    .unwrap();
    backend.reconcile_stuck_upload_intents().await.unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing,
        "an unrelated later upload cannot complete a failed checksum attempt"
    );
    assert_eq!(
        fixture
            .index()
            .reliability_events(&intent_id)
            .await
            .unwrap()
            .len(),
        2
    );
    fixture.finish().await;
}

#[tokio::test]
async fn file_upload_without_digest_returns_retryable_owner_loss_without_intents() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: plain file ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    for lost_before_body in [true, false] {
        let session = crate::maintenance_barrier::RequestSession::acquire(
            &backend.gc_barrier_pool,
            backend.request_session_identity.clone(),
        )
        .await
        .unwrap();
        let polled = Arc::new(AtomicBool::new(false));
        session
            .scope(async {
                let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                    &backend.request_session_identity,
                )
                .await
                .unwrap();
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut **owner.as_mut().unwrap())
                    .await
                    .unwrap();
                drop(owner);
                if lost_before_body {
                    assert!(
                        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                            .bind(pid)
                            .fetch_one(&fixture.admin)
                            .await
                            .unwrap()
                    );
                }
                let admin = fixture.admin.clone();
                let polled_body = polled.clone();
                let stream = futures_util::stream::once(async move {
                    polled_body.store(true, Ordering::Relaxed);
                    if !lost_before_body {
                        assert!(
                            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                                .bind(pid)
                                .fetch_one(&admin)
                                .await
                                .unwrap()
                        );
                    }
                    Ok(Bytes::from_static(b"plain ownership boundary"))
                });
                let error = backend
                    .upload_file_stream(
                        "lost-plain-file",
                        RequestBodyReader::from_stream(stream),
                        None,
                        None,
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error, crate::ServerError::StaleResourceFence),
                    "{error:?}"
                );
            })
            .await;
        session.finish().await;
        assert_eq!(polled.load(Ordering::Relaxed), !lost_before_body);
        assert!(matches!(
            backend.read_record("lost-plain-file", None, None).await,
            Err(crate::ServerError::NotFound)
        ));
    }
    let intents: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_upload_intents",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(intents, 0);
    let fresh = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    fresh
        .scope(backend.upload_file(
            "fresh-plain-file",
            Bytes::from_static(b"plain ownership boundary"),
            None,
        ))
        .await
        .unwrap();
    fresh.finish().await;
    assert!(
        backend
            .read_record("fresh-plain-file", None, None)
            .await
            .is_ok()
    );
    fixture.finish().await;
}

#[tokio::test]
async fn stored_file_intent_recovery_requires_matching_published_bytes() {
    use sha2::Digest;
    use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: file recovery identity fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    for (file_id, expected, publish) in [
        (
            "matching-recovery-file",
            b"published bytes".as_slice(),
            true,
        ),
        ("unrelated-recovery-file", b"other attempt".as_slice(), true),
        ("absent-recovery-file", b"published bytes".as_slice(), false),
    ] {
        if publish {
            backend
                .upload_file(file_id, Bytes::from_static(b"published bytes"), None)
                .await
                .unwrap();
        }
        let intent = UploadIntent::new(
            format!("recovery-{file_id}"),
            format!("record/{file_id}"),
            hex::encode(sha2::Sha256::digest(expected)),
            0,
        );
        fixture.index().create_intent(&intent).await.unwrap();
        for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
            assert!(
                fixture
                    .index()
                    .transition_intent(intent.intent_id(), state)
                    .await
                    .unwrap()
            );
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1",
            fixture.schema
        ))).bind(intent.intent_id()).execute(&fixture.admin).await.unwrap();
        backend.reconcile_stuck_upload_intents().await.unwrap();
        let completed = file_id == "matching-recovery-file";
        assert_eq!(
            fixture
                .index()
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            if completed {
                UploadIntentState::Visible
            } else {
                UploadIntentState::Stored
            }
        );
        assert_eq!(
            fixture
                .index()
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            if completed { 5 } else { 3 }
        );
        assert_eq!(
            backend.read_record(file_id, None, None).await.is_ok(),
            publish
        );
    }
    fixture.finish().await;
}

#[tokio::test]
async fn scoped_file_recovery_distinguishes_colliding_legacy_labels() {
    use sha2::Digest;
    use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: scoped recovery identity fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let scopes = [
        RepositoryScope::new(RepositoryProvider::GitHub, "team/a", "b", Some("main")).unwrap(),
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "a/b", Some("main")).unwrap(),
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap(),
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("-")).unwrap(),
    ];
    let payloads = [
        b"first".as_slice(),
        b"second".as_slice(),
        b"no revision".as_slice(),
        b"dash revision".as_slice(),
    ];
    assert_eq!(
        crate::protocol_support::reliability_repository_scope(Some(&scopes[0])),
        crate::protocol_support::reliability_repository_scope(Some(&scopes[1]))
    );
    assert_eq!(
        crate::protocol_support::reliability_repository_scope(Some(&scopes[2])),
        crate::protocol_support::reliability_repository_scope(Some(&scopes[3]))
    );
    backend
        .upload_file("scoped-recovery", Bytes::from_static(b"second"), None)
        .await
        .unwrap();
    for (ordinal, (scope, bytes)) in scopes.iter().zip(payloads).enumerate() {
        backend
            .upload_file(
                "scoped-recovery",
                Bytes::copy_from_slice(bytes),
                Some(scope),
            )
            .await
            .unwrap();
        let before = serde_json::to_value(
            backend
                .read_record("scoped-recovery", None, Some(scope))
                .await
                .unwrap(),
        )
        .unwrap();
        let intent = UploadIntent::new(
            format!("scoped-recovery-{ordinal}"),
            "record/scoped-recovery".to_owned(),
            hex::encode(sha2::Sha256::digest(bytes)),
            0,
        );
        let repository =
            crate::upload_intent_recovery::file_intent_repository_scope(Some(scope)).unwrap();
        fixture
            .index()
            .create_intent_scoped(&intent, "shardline", &repository)
            .await
            .unwrap();
        for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
            assert!(
                fixture
                    .index()
                    .transition_intent(intent.intent_id(), state)
                    .await
                    .unwrap()
            );
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1", fixture.schema
        ))).bind(intent.intent_id()).execute(&fixture.admin).await.unwrap();
        backend.reconcile_stuck_upload_intents().await.unwrap();
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
        assert_eq!(
            serde_json::to_value(
                backend
                    .read_record("scoped-recovery", None, Some(scope))
                    .await
                    .unwrap()
            )
            .unwrap(),
            before
        );
    }
    for (id, repository) in [
        (
            "wrong-exact-scope",
            crate::upload_intent_recovery::file_intent_repository_scope(Some(&scopes[0])).unwrap(),
        ),
        ("ambiguous-legacy-scope", "github:team/a/b/main".to_owned()),
    ] {
        let intent = UploadIntent::new(
            id.to_owned(),
            "record/scoped-recovery".to_owned(),
            hex::encode(sha2::Sha256::digest(b"second")),
            0,
        );
        fixture
            .index()
            .create_intent_scoped(&intent, "shardline", &repository)
            .await
            .unwrap();
        for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
            assert!(fixture.index().transition_intent(id, state).await.unwrap());
        }
        let before =
            serde_json::to_value(fixture.index().reliability_events(id).await.unwrap()).unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1", fixture.schema
        ))).bind(id).execute(&fixture.admin).await.unwrap();
        backend.reconcile_stuck_upload_intents().await.unwrap();
        assert_eq!(
            fixture
                .index()
                .intent_by_id(id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Stored
        );
        assert_eq!(
            serde_json::to_value(fixture.index().reliability_events(id).await.unwrap()).unwrap(),
            before
        );
    }
    fixture.finish().await;
}

#[tokio::test]
async fn historical_server_preserves_unfinished_scoped_file_evidence() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_LEGACY_RECOVERY_BINARY")
    else {
        return;
    };
    verify_released_file_recovery(binary, true).await;
}

#[tokio::test]
async fn released_server_recovers_scoped_file_checkpoints_without_inventing_completion() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_N_MINUS_ONE_BINARY")
    else {
        return;
    };
    verify_released_file_recovery(binary, false).await;
}

async fn verify_released_file_recovery(binary: String, historical: bool) {
    use sha2::Digest;
    use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: rollback fixture requires DATABASE_URL");
        return;
    };
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let payload = Bytes::from_static(b"scoped rollback evidence");
    fixture
        .postgres()
        .upload_file("rollback-scoped", payload.clone(), Some(&scope))
        .await
        .unwrap();
    fixture
        .postgres()
        .upload_file(
            "rollback-scoped",
            Bytes::from_static(b"unrelated default file"),
            None,
        )
        .await
        .unwrap();
    let mut checkpoints = Vec::new();
    for (repository_scope, bytes) in [
        (Some(&scope), payload.as_ref()),
        (None, b"unrelated default file".as_slice()),
    ] {
        let repository =
            crate::upload_intent_recovery::file_intent_repository_scope(repository_scope).unwrap();
        for (ordinal, checkpoint) in [
            UploadIntentState::Created,
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ]
        .into_iter()
        .enumerate()
        {
            let intent = UploadIntent::new(
                if repository_scope.is_some() {
                    format!("rollback-scoped-evidence-{ordinal}")
                } else {
                    crate::upload_ingest::upload_attempt_id("rollback-scoped")
                },
                "record/rollback-scoped".to_owned(),
                hex::encode(sha2::Sha256::digest(bytes)),
                0,
            );
            fixture
                .index()
                .create_intent_scoped(&intent, "shardline", &repository)
                .await
                .unwrap();
            for state in [
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
                        .transition_intent(intent.intent_id(), state)
                        .await
                        .unwrap()
                );
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1", fixture.schema
    ))).bind(intent.intent_id()).execute(&fixture.admin).await.unwrap();
            let before = serde_json::to_value(
                fixture
                    .index()
                    .reliability_events(intent.intent_id())
                    .await
                    .unwrap(),
            )
            .unwrap();
            checkpoints.push((intent, checkpoint, before));
        }
    }
    let legacy = UploadIntent::new(
        "legacy-file-rollback".to_owned(),
        "record/rollback-scoped".to_owned(),
        hex::encode(sha2::Sha256::digest(b"unrelated default file")),
        0,
    );
    fixture.index().create_intent(&legacy).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1", fixture.schema))).bind(legacy.intent_id()).execute(&fixture.admin).await.unwrap();
    let root = fixture.config.root_dir().to_path_buf();
    let database_url = fixture.config.index_postgres_url().unwrap().to_owned();
    let logs =
        crate::test_support::released_startup::run(binary.into(), root, Some(database_url)).await;
    assert!(
        logs.contains("reconciling stuck intent"),
        "released recovery did not inspect the fixture: {logs}"
    );
    for (intent, checkpoint, before) in &checkpoints {
        assert!(
            logs.contains(intent.intent_id()),
            "released recovery did not inspect {}: {logs}",
            intent.intent_id()
        );
        let state = fixture
            .index()
            .intent_by_id(intent.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state();
        let events = fixture
            .index()
            .reliability_events(intent.intent_id())
            .await
            .unwrap();
        shardline_reliability::verify_lifecycle_chain_ends_at(&events, state).unwrap();
        let after = serde_json::to_value(events).unwrap();
        if historical
            || !matches!(
                checkpoint,
                UploadIntentState::Stored | UploadIntentState::MetadataCommitted
            )
        {
            assert_eq!(state, *checkpoint);
        } else {
            assert!(state == *checkpoint || state == UploadIntentState::Visible);
        }
        let original = before.as_array().unwrap();
        assert_eq!(
            after.as_array().unwrap().get(..original.len()).unwrap(),
            original.as_slice()
        );
        if state == *checkpoint {
            assert_eq!(after, *before);
        }
    }
    assert_eq!(
        fixture
            .index()
            .intent_by_id(legacy.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state(),
        if historical {
            UploadIntentState::Visible
        } else {
            UploadIntentState::Created
        }
    );
    assert_eq!(
        fixture
            .index()
            .reliability_events(legacy.intent_id())
            .await
            .unwrap()
            .len(),
        if historical { 5 } else { 1 }
    );
    fixture
        .postgres()
        .reconcile_stuck_upload_intents()
        .await
        .unwrap();
    for (intent, checkpoint, _) in checkpoints {
        let expected = if matches!(
            checkpoint,
            UploadIntentState::Stored | UploadIntentState::MetadataCommitted
        ) {
            UploadIntentState::Visible
        } else {
            checkpoint
        };
        assert_eq!(
            fixture
                .index()
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            expected
        );
    }
    for line in logs.lines().filter(|line| {
        line.contains("intent recovery")
            || line.contains("intent transition")
            || line.contains("reconciling stuck intent")
    }) {
        eprintln!("released recovery: {line}");
    }
    let record = fixture
        .postgres()
        .read_record("rollback-scoped", None, Some(&scope))
        .await
        .unwrap();
    fixture.finish().await;
    assert_eq!(record.file_id, "rollback-scoped");
}

#[tokio::test]
async fn digest_file_upload_keeps_progress_with_the_original_request_owner() {
    use sha2::Digest;
    use shardline_index::{UploadIntentState, UploadIntentStore};
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: digest file ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let payload = Bytes::from_static(b"digest file ownership boundary");
    let digest = hex::encode(sha2::Sha256::digest(&payload));
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let admin = fixture.admin.clone();
            let bytes = payload.clone();
            // Polling begins after intent admission and the Storing transition.
            let stream = futures_util::stream::once(async move {
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                        .bind(pid)
                        .fetch_one(&admin)
                        .await
                        .unwrap()
                );
                Ok(bytes)
            });
            let error = backend
                .upload_file_stream(
                    "lost-digest-file",
                    RequestBodyReader::from_stream(stream),
                    None,
                    Some(&digest),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, crate::ServerError::StaleResourceFence));
        })
        .await;
    session.finish().await;
    let intent_id: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT intent_id FROM {}.shardline_upload_intents WHERE object_key='record/lost-digest-file'",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
    );
    assert_eq!(
        fixture
            .index()
            .reliability_events(&intent_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(matches!(
        backend.read_record("lost-digest-file", None, None).await,
        Err(crate::ServerError::NotFound)
    ));

    let scope = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )
    .unwrap();
    let fresh = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    fresh
        .scope(backend.upload_file_stream(
            "fresh-digest-file",
            RequestBodyReader::from_bytes(payload),
            Some(&scope),
            Some(&digest),
        ))
        .await
        .unwrap();
    fresh.finish().await;
    let fresh_id: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT intent_id FROM {}.shardline_upload_intents WHERE object_key='record/fresh-digest-file'",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    let events = fixture.index().reliability_events(&fresh_id).await.unwrap();
    assert_eq!(events.len(), 5);
    assert!(events.iter().all(|event| event.operation.repository
        == format!("scope-json-v1:{}", serde_json::to_string(&scope).unwrap())));
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&fresh_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Visible
    );
    assert!(
        backend
            .read_record("fresh-digest-file", None, Some(&scope))
            .await
            .is_ok()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_upload_owner_loss_during_publication_returns_a_fresh_request_error() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: publication ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    let request_pid: i32 = session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap()
        })
        .await;
    let mut blocker = fixture.admin.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {}.shardline_file_records IN SHARE MODE",
        fixture.schema
    )))
    .execute(&mut *blocker)
    .await
    .unwrap();
    let upload_session = session.clone();
    let upload_backend = backend.clone();
    let upload = tokio::spawn(async move {
        upload_session
            .scope(upload_backend.upload_file(
                "publication-owner-loss",
                Bytes::from_static(b"publication boundary"),
                None,
            ))
            .await
    });
    // Observe a real publication lock wait after both liveness checks succeeded.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(request_pid)
                .fetch_one(&fixture.admin)
                .await
                .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upload must reach its publication transaction");
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(request_pid)
            .fetch_one(&fixture.admin)
            .await
            .unwrap()
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), upload)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, crate::ServerError::StaleResourceFence),
        "{error:?}"
    );
    blocker.rollback().await.unwrap();
    session.finish().await;
    assert!(matches!(
        backend
            .read_record("publication-owner-loss", None, None)
            .await,
        Err(crate::ServerError::NotFound)
    ));
    let fresh = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    fresh
        .scope(backend.upload_file(
            "publication-owner-loss",
            Bytes::from_static(b"publication boundary"),
            None,
        ))
        .await
        .unwrap();
    fresh.finish().await;
    assert!(
        backend
            .read_record("publication-owner-loss", None, None)
            .await
            .is_ok()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_upload_recovery_preserves_each_named_boundary() {
    use shardline_cas::{UploadLifecycleBoundary as Boundary, arm_upload_interruption};
    use shardline_index::{UploadIntentState, UploadIntentStore};
    for boundary in [
        Boundary::AfterIntentCreated,
        Boundary::AfterStoring,
        Boundary::AfterObjectWork,
        Boundary::AfterStored,
        Boundary::AfterMetadataCommitted,
        Boundary::AfterVisible,
    ] {
        let Some(fixture) = PostgresFixture::new().await else {
            return;
        };
        fixture
            .index()
            .initialize_remote_delete_guard()
            .await
            .unwrap();
        let backend = fixture.postgres();
        let payload = format!("native recovery boundary {boundary:?}");
        let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload.as_bytes());
        backend.upload_xorb(&hash, xorb).await.unwrap();
        let (shard, file_hash) =
            crate::test_fixtures::single_file_shard(&[(payload.as_bytes(), &hash)]);
        let (intent_id, _repository) =
            crate::upload_identity::native_shard(blake3::hash(&shard).to_hex().as_ref(), None)
                .unwrap();
        let session = crate::maintenance_barrier::RequestSession::acquire(
            &backend.gc_barrier_pool,
            backend.request_session_identity.clone(),
        )
        .await
        .unwrap();
        let fault = arm_upload_interruption(intent_id.clone(), boundary);
        assert!(
            session
                .scope(backend.upload_shard_stream(
                    RequestBodyReader::from_bytes(shard.clone()),
                    None,
                    crate::ShardMetadataLimits::default()
                ))
                .await
                .is_err()
        );
        drop(fault);
        let expected = match boundary {
            Boundary::AfterIntentCreated => UploadIntentState::Created,
            Boundary::AfterStoring | Boundary::AfterObjectWork => UploadIntentState::Storing,
            // Completion events share one transaction after domain publication;
            // these hooks observe the same committed Visible state.
            Boundary::AfterStored | Boundary::AfterMetadataCommitted | Boundary::AfterVisible => {
                UploadIntentState::Visible
            }
        };
        assert_eq!(
            fixture
                .index()
                .intent_by_id(&intent_id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            expected
        );
        let published = !matches!(
            boundary,
            Boundary::AfterIntentCreated | Boundary::AfterStoring
        );
        assert_eq!(
            backend.read_record(&file_hash, None, None).await.is_ok(),
            published
        );
        session
            .scope(async {
                let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                    &backend.request_session_identity,
                )
                .await
                .unwrap();
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut **owner.as_mut().unwrap())
                    .await
                    .unwrap();
                drop(owner);
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                        .bind(pid)
                        .fetch_one(&fixture.admin)
                        .await
                        .unwrap()
                );
                assert!(
                    backend
                        .upload_shard_stream(
                            RequestBodyReader::from_bytes(shard.clone()),
                            None,
                            crate::ShardMetadataLimits::default()
                        )
                        .await
                        .is_err(),
                    "stale retry must revalidate ownership at {boundary:?}"
                );
            })
            .await;
        session.finish().await;
        assert_eq!(
            backend.read_record(&file_hash, None, None).await.is_ok(),
            published
        );
        sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {}.shardline_upload_intents SET created_at = now() - interval '1 minute' WHERE intent_id = $1", fixture.schema)))
            .bind(&intent_id).execute(&fixture.admin).await.unwrap();
        backend.reconcile_stuck_upload_intents().await.unwrap();
        let recovered = fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state();
        assert_ne!(
            recovered,
            UploadIntentState::Failed,
            "normalization must not poison retry at {boundary:?}"
        );
        if !published {
            assert_ne!(recovered, UploadIntentState::Visible);
        }
        assert_eq!(
            backend.read_record(&file_hash, None, None).await.is_ok(),
            published
        );
        backend
            .upload_shard_stream(
                RequestBodyReader::from_bytes(shard),
                None,
                crate::ShardMetadataLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .index()
                .intent_by_id(&intent_id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert!(backend.read_record(&file_hash, None, None).await.is_ok());
        fixture.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_xorb_upload_rejects_an_already_lost_request_owner_before_creating_intent() {
    use shardline_index::UploadIntentStore;
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: raw xorb ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(b"lost raw xorb request owner");
    let intent_id = crate::upload_identity::xorb(&hash, xorb.len() as u64);
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .fetch_one(&fixture.admin)
                    .await
                    .unwrap()
            );
            let error = backend.upload_xorb(&hash, xorb.clone()).await.unwrap_err();
            assert!(matches!(error, crate::ServerError::StaleResourceFence));
            let response = axum::response::IntoResponse::into_response(error);
            assert_eq!(
                response.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        })
        .await;
    assert!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .is_none()
    );
    session.finish().await;
    backend.upload_xorb(&hash, xorb).await.unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        shardline_index::UploadIntentState::Visible
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_xorb_upload_rejects_owner_loss_while_reading_the_request_body() {
    use shardline_index::UploadIntentStore;
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: streaming xorb ownership fixture requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let (xorb, hash) =
        crate::test_fixtures::single_chunk_xorb(b"owner lost during xorb body staging");
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let admin = fixture.admin.clone();
            let body = xorb.clone();
            // This stream is first polled after the initial owner check succeeds.
            let stream = futures_util::stream::once(async move {
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                        .bind(pid)
                        .fetch_one(&admin)
                        .await
                        .unwrap()
                );
                Ok(body)
            });
            let error = backend
                .upload_xorb_stream(&hash, RequestBodyReader::from_stream(stream))
                .await
                .unwrap_err();
            assert!(matches!(error, crate::ServerError::StaleResourceFence));
        })
        .await;
    assert!(
        fixture
            .index()
            .intent_by_id(&crate::upload_identity::xorb(&hash, xorb.len() as u64))
            .await
            .unwrap()
            .is_none()
    );
    assert!(backend.xorb_length(&hash).await.is_err());
    session.finish().await;
    backend.upload_xorb(&hash, xorb).await.unwrap();
    assert!(backend.xorb_length(&hash).await.is_ok());
    fixture.finish().await;
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_xorb_owner_loss_after_provider_put_preserves_unfinished_intent_and_bytes() {
    use shardline_index::{UploadIntentState, UploadIntentStore};
    use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
    use std::time::Duration;
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: provider-backed xorb ownership fixture requires DATABASE_URL");
        return;
    };
    let stack = tokio::task::spawn_blocking(|| {
        DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
            .expect("owned MinIO fixture required")
    })
    .await
    .unwrap();
    let raw = stack.s3_raw_config(Some("xorb-owner-loss")).unwrap();
    let proxy = S3FaultProxy::start(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::DelaySuccessfulPutResponse(Duration::from_secs(10)),
    )
    .unwrap();
    let config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            raw.session_token.map(shardline_protocol::SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http);
    let mut backend = fixture.postgres().clone();
    backend.object_store = crate::ServerObjectStore::s3(config).unwrap();
    let (xorb, hash) =
        crate::test_fixtures::single_chunk_xorb(b"provider accepted xorb before owner loss");
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let interrupt = async {
                tokio::time::timeout(Duration::from_secs(8), async {
                    while !proxy.fault_was_injected() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                        .bind(pid)
                        .fetch_one(&fixture.admin)
                        .await
                        .unwrap()
                );
            };
            let (result, ()) = tokio::join!(backend.upload_xorb(&hash, xorb.clone()), interrupt);
            assert!(matches!(
                result,
                Err(crate::ServerError::StaleResourceFence)
            ));
        })
        .await;
    assert!(proxy.fault_was_injected());
    let intent_id = crate::upload_identity::xorb(&hash, xorb.len() as u64);
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
    );
    let key = crate::xet_adapter::xorb_object_key(&hash).unwrap();
    let stored = backend
        .object_store()
        .read_range(
            &key,
            shardline_protocol::ByteRange::new(
                0,
                u64::try_from(xorb.len()).unwrap().checked_sub(1).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(stored.as_slice(), xorb.as_ref());
    session.finish().await;
    backend.upload_xorb(&hash, xorb.clone()).await.unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Visible
    );
    fixture.finish().await;
    tokio::task::spawn_blocking(move || {
        drop(proxy);
        drop(stack);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_native_upload_owner_cannot_be_recovered_from_intent_bookkeeping() {
    use shardline_cas::{UploadLifecycleBoundary, arm_upload_interruption};
    use shardline_index::{UploadIntentState, UploadIntentStore};
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let payload = b"native ownership recovery boundary";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload);
    backend.upload_xorb(&hash, xorb).await.unwrap();
    let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(payload, &hash)]);
    let (intent_id, _repository) =
        crate::upload_identity::native_shard(blake3::hash(&shard).to_hex().as_ref(), None).unwrap();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let interruption =
                arm_upload_interruption(intent_id.clone(), UploadLifecycleBoundary::AfterStoring);
            assert!(
                backend
                    .upload_shard_stream(
                        RequestBodyReader::from_bytes(shard.clone()),
                        None,
                        crate::ShardMetadataLimits::default(),
                    )
                    .await
                    .is_err()
            );
            drop(interruption);
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .fetch_one(&fixture.admin)
                    .await
                    .unwrap()
            );
            let result = backend
                .upload_shard_stream(
                    RequestBodyReader::from_bytes(shard.clone()),
                    None,
                    crate::ShardMetadataLimits::default(),
                )
                .await;
            assert!(
                result.is_err(),
                "lost owner must not publish native metadata"
            );
        })
        .await;
    session.finish().await;
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
    );
    let metadata_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT (SELECT count(*) FROM {}.shardline_file_records) + (SELECT count(*) FROM {}.shardline_dedupe_shards)", fixture.schema, fixture.schema
    ))).fetch_one(&fixture.admin).await.unwrap();
    assert_eq!(metadata_count, 0);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {}.shardline_upload_intents SET created_at = now() - interval '1 minute' WHERE intent_id = $1", fixture.schema
    ))).bind(&intent_id).execute(&fixture.admin).await.unwrap();
    backend.reconcile_stuck_upload_intents().await.unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Storing
    );
    assert!(matches!(
        backend.read_record(&file_hash, None, None).await,
        Err(crate::ServerError::NotFound)
    ));
    backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            crate::ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .index()
            .intent_by_id(&intent_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Visible
    );
    assert!(backend.read_record(&file_hash, None, None).await.is_ok());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_record_reads_do_not_reenter_the_request_connection() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    let mut backend = fixture.postgres().clone();
    backend
        .upload_file("guarded-read", Bytes::from_static(b"guarded bytes"), None)
        .await
        .unwrap();
    // Production has a separate record-read pool. Keep one connection per pool
    // to expose mutex re-entry without requiring a second ownership connection.
    let read_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            backend
                .record_store
                .pool()
                .connect_options()
                .as_ref()
                .clone(),
        )
        .await
        .unwrap();
    backend.record_store = shardline_index::PostgresRecordStore::new(read_pool.clone());
    let session = crate::maintenance_barrier::RequestSession::acquire(
        backend.index_store.pool(),
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let guard = crate::maintenance_barrier::acquire_postgres_request_resources(
                backend.index_store.pool(),
                &backend.request_session_identity,
                &[ResourceLockKey::s3_object("guarded-scope", "guarded-read")],
            )
            .await
            .unwrap();
            let record = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                backend.read_record("guarded-read", None, None),
            )
            .await
            .expect("record reads must not re-enter the held request mutex")
            .unwrap();
            assert_eq!(record.total_bytes, 13);
            drop(guard);
        })
        .await;
    session.finish().await;
    read_pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_reference_copy_cannot_replace_a_lost_request_owner() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    let backend = fixture.postgres();
    backend
        .upload_file("copy-source", Bytes::from_static(b"copy ownership"), None)
        .await
        .unwrap();
    let source = backend
        .read_record("copy-source", None, None)
        .await
        .unwrap();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        backend.index_store.pool(),
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-owned")
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .delete_file_reference_if_latest("copy-owned", &source.content_hash)
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-delete")
                    .await
                    .unwrap()
            );
            assert!(backend.delete_file_reference("copy-delete").await.unwrap());
            assert!(!backend.delete_file_reference("copy-delete").await.unwrap());
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&fixture.admin)
                .await
                .unwrap();
            assert!(terminated);
            assert!(backend.delete_file_reference("copy-source").await.is_err());
            assert!(
                backend
                    .delete_file_reference_if_latest("copy-source", &source.content_hash)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-owner-lost")
                    .await
                    .is_err()
            );
        })
        .await;
    session.finish().await;
    assert!(backend.read_record("copy-source", None, None).await.is_ok());
    assert!(matches!(
        backend.read_record("copy-owner-lost", None, None).await,
        Err(crate::ServerError::NotFound)
    ));
    assert!(
        backend
            .copy_file_reference("copy-source", "copy-fresh")
            .await
            .unwrap()
    );
    assert!(
        !backend
            .copy_file_reference("copy-source", "copy-fresh")
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_transaction_snapshot_cannot_hide_a_pending_delete() {
    for (protocol, isolation) in ["REPEATABLE READ", "SERIALIZABLE", "REPEATABLE READ"]
        .into_iter()
        .enumerate()
    {
        let Some(fixture) = PostgresFixture::new().await else {
            return;
        };
        fixture
            .index()
            .initialize_remote_delete_guard()
            .await
            .unwrap();
        let backend = fixture.postgres();
        let (record, _) = backend
            .prepare_file_stream(
                "snapshot-file",
                RequestBodyReader::from_bytes(Bytes::from_static(
                    b"pending deletion after snapshot",
                )),
                None,
            )
            .await
            .unwrap();
        let physical = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
        assert!(
            backend
                .object_store()
                .metadata(&physical)
                .unwrap()
                .is_some()
        );
        let mut snapshot = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *snapshot)
        .await
        .unwrap();
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete'")
            .fetch_one(&mut *snapshot).await.unwrap();
        assert_eq!(before, 0);
        let epoch: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_begin_v1($1, $2)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind(physical.as_str())
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let publication = backend
            .commit_staged_file_on_connection(&mut snapshot, &record)
            .await;
        if publication.is_ok() {
            snapshot.commit().await.unwrap();
        } else {
            snapshot.rollback().await.unwrap();
        }
        let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {}.shardline_file_records",
            fixture.schema,
        )))
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let settled: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_finish_v1($1, $2, $3)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind(physical.as_str())
        .bind(epoch)
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let mut retry = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *retry)
        .await
        .unwrap();
        let retried = backend
            .commit_staged_file_on_connection(&mut retry, &record)
            .await;
        let retained_isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *retry)
            .await
            .unwrap();
        retry.commit().await.unwrap();
        fixture.finish().await;
        assert!(settled);
        assert!(
            publication.is_err(),
            "{isolation} must not publish through an invisible pending marker: {publication:?}"
        );
        assert_eq!(visible, 0, "{isolation} must preserve publication safety");
        assert!(
            retried.is_ok(),
            "a fresh {isolation} retry must succeed: {retried:?}"
        );
        assert_eq!(retained_isolation, isolation.to_lowercase());
        let error = publication.unwrap_err();
        assert!(!error.is_remote_delete_pending());
        use axum::response::IntoResponse;
        let response = match protocol {
            0 => error.into_response(),
            1 => crate::error::OciError(error).into_response(),
            _ => shardline_s3_adapter::S3Error::from(error).into_response(),
        };
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("retry the request in a new transaction")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_publication_does_not_wait_on_a_delete_holding_physical_locks() {
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        let Some(fixture) = PostgresFixture::new().await else {
            return;
        };
        fixture
            .index()
            .initialize_remote_delete_guard()
            .await
            .unwrap();
        let mut publication = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *publication)
        .await
        .unwrap();
        sqlx::query("SELECT shardline_remote_delete_guard_v1(current_schema()::text, $1)")
            .bind("snapshot-order/first")
            .execute(&mut *publication)
            .await
            .unwrap();
        let statement = sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_begin_v1($1, $2)",
            fixture.schema,
        ));
        let mut admission = Box::pin(
            sqlx::query_scalar::<_, i64>(statement)
                .bind(&fixture.schema)
                .bind("snapshot-order/second")
                .fetch_one(&fixture.admin),
        );
        let waiting = tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
            .await
            .is_err();
        // While admission waits on the snapshot lock, this same transaction
        // must still be able to publish a second physical key.
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query("SELECT shardline_remote_delete_guard_v1(current_schema()::text, $1)")
                .bind("snapshot-order/second")
                .execute(&mut *publication),
        )
        .await;
        publication.rollback().await.unwrap();
        let epoch = tokio::time::timeout(std::time::Duration::from_secs(5), admission)
            .await
            .unwrap()
            .unwrap();
        let settled: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_finish_v1($1, $2, $3)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind("snapshot-order/second")
        .bind(epoch)
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        fixture.finish().await;
        assert!(
            waiting,
            "{isolation} must hold delete admission until publication ends"
        );
        assert!(
            matches!(second, Ok(Ok(_))),
            "{isolation} must avoid reversed lock ordering: {second:?}"
        );
        assert!(settled);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_shard_publication_rejects_a_deleted_validated_xorb() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let payload = b"validated native backing bytes";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload);
    backend.upload_xorb(&hash, xorb.clone()).await.unwrap();
    let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(payload, &hash)]);
    let mut validated = None;
    crate::xet_adapter::register_uploaded_shard_bytes_with_member_references(
        &backend.object_store(),
        &shard,
        None,
        crate::ShardMetadataLimits::default(),
        |records, mappings, references, identities| {
            validated = Some((records, mappings, references, identities));
            async { Ok(()) }
        },
    )
    .await
    .unwrap();
    let (records, mappings, references, identities) = validated.unwrap();
    assert!(!records.is_empty());
    assert!(!mappings.is_empty());
    let physical = crate::xet_adapter::xorb_object_key(&hash).unwrap();
    assert!(
        backend
            .object_store()
            .metadata(&physical)
            .unwrap()
            .is_some()
    );
    let permit = fixture
        .index()
        .begin_remote_delete(&physical)
        .await
        .unwrap();
    let pending = backend
        .commit_validated_native_shard_with_descriptors(
            &records,
            &mappings,
            &references,
            &identities,
            &[],
        )
        .await;
    let deleted = backend.object_store().delete_if_present(&physical).unwrap();
    assert_eq!(deleted, shardline_storage::DeleteOutcome::Deleted);
    assert!(fixture.index().finish_remote_delete(&permit).await.unwrap());
    let publication = backend
        .commit_validated_native_shard(&records, &mappings, &references)
        .await;
    let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_file_records",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    backend.upload_xorb(&hash, xorb).await.unwrap();
    let shard_key = mappings.first().unwrap().shard_object_key().clone();
    assert!(
        backend
            .object_store()
            .metadata(&shard_key)
            .unwrap()
            .is_some()
    );
    let shard_permit = fixture
        .index()
        .begin_remote_delete(&shard_key)
        .await
        .unwrap();
    assert_eq!(
        backend
            .object_store()
            .delete_if_present(&shard_key)
            .unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    );
    assert!(
        fixture
            .index()
            .finish_remote_delete(&shard_permit)
            .await
            .unwrap()
    );
    let missing_shard = backend
        .commit_validated_native_shard(&records, &mappings, &references)
        .await;
    let partial_rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT (SELECT count(*) FROM {}.shardline_file_records) + (SELECT count(*) FROM {}.shardline_dedupe_shards)",
        fixture.schema, fixture.schema,
    ))).fetch_one(&fixture.admin).await.unwrap();
    let retried = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            crate::ShardMetadataLimits::default(),
        )
        .await;
    let downloaded = backend.download_file(&file_hash, None, None).await;
    fixture.finish().await;
    assert!(crate::ServerError::from(pending.unwrap_err()).is_remote_delete_pending());
    assert!(
        publication.is_err(),
        "native metadata must not expose deleted backing bytes: {publication:?}"
    );
    assert_eq!(visible, 0);
    assert_eq!(
        partial_rows, 0,
        "missing normalized shards must roll back file and mapping rows"
    );
    let missing_shard = crate::ServerError::from(missing_shard.unwrap_err());
    assert!(!missing_shard.is_remote_delete_pending());
    assert_eq!(
        missing_shard.status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let error = crate::ServerError::from(publication.unwrap_err());
    assert!(!error.is_remote_delete_pending());
    use axum::response::IntoResponse;
    let response = error.into_response();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("upload missing backing content again, then retry the shard")
    );
    assert!(
        retried.is_ok(),
        "a fresh native upload retry must succeed: {retried:?}"
    );
    assert_eq!(downloaded.unwrap().as_slice(), payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_publication_rejects_a_chunk_deleted_after_staging() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let namespace = "01".repeat(32);
    let logical = ObjectKey::parse(&format!("protocols/s3/{namespace}/staged-object")).unwrap();
    let bytes = Bytes::from_static(b"orphan bytes prepared before the delete completed");
    let seed = fixture
        .backend
        .prepare_s3_object_stream(&logical, RequestBodyReader::from_bytes(bytes.clone()))
        .await
        .unwrap();
    let hash = &seed.record.chunks[0].hash;
    let physical = crate::xet_adapter::xorb_object_key(hash).unwrap();
    let backend = fixture.postgres();
    let objects = backend.object_store();
    assert!(
        objects.metadata(&physical).unwrap().is_some(),
        "delete must target the actual staged backing object"
    );
    let permit = fixture
        .index()
        .begin_remote_delete(&physical)
        .await
        .unwrap();
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&namespace, "staged-object"),
        )
        .await
        .unwrap();
    let prepared = fixture
        .backend
        .prepare_s3_object_stream(&logical, RequestBodyReader::from_bytes(bytes))
        .await
        .unwrap();
    let entry = S3ObjectEntry {
        scope_namespace: namespace,
        object_key: "staged-object".to_owned(),
        file_id: prepared.record.file_id.clone(),
        size_bytes: prepared.record.total_bytes,
        content_hash: prepared.record.content_hash.clone(),
        etag: "02".repeat(16),
        user_metadata: Vec::new(),
        updated_at_unix_seconds: 1,
    };
    let pending = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    guard.release().await.unwrap();
    assert_eq!(
        objects.delete_if_present(&physical).unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    );
    assert!(fixture.index().finish_remote_delete(&permit).await.unwrap());
    assert!(objects.metadata(&physical).unwrap().is_none());
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&entry.scope_namespace, "staged-object"),
        )
        .await
        .unwrap();
    let publication = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    let conditional = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::IfUnchanged(Some(entry.clone())),
            None,
        )
        .await;
    guard.release().await.unwrap();
    let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_s3_objects",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    let mut connection = fixture.index().pool().acquire().await.unwrap();
    let file_publication = backend
        .commit_staged_file_on_connection(&mut connection, &prepared.record)
        .await;
    connection.close().await.unwrap();
    let visible_files: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_file_records",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    let retry = fixture
        .backend
        .prepare_s3_object_stream(
            &logical,
            RequestBodyReader::from_bytes(Bytes::from_static(
                b"orphan bytes prepared before the delete completed",
            )),
        )
        .await
        .unwrap();
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&entry.scope_namespace, "staged-object"),
        )
        .await
        .unwrap();
    let retried = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &retry,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    guard.release().await.unwrap();
    fixture.finish().await;
    assert!(pending.unwrap_err().is_remote_delete_pending());
    assert!(
        !conditional.unwrap(),
        "a failed precondition must retain its existing result"
    );
    assert!(
        publication.is_err(),
        "deleted staged content must not become visible: {publication:?}"
    );
    let error = publication.unwrap_err();
    assert!(
        !error.is_remote_delete_pending(),
        "the completed delete is no longer pending"
    );
    assert_eq!(
        error.status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(error.to_string().contains("retry the upload"));
    assert_eq!(visible, 0);
    assert_eq!(
        visible_files, 0,
        "both publication paths must roll back file metadata"
    );
    assert_eq!(
        file_publication.unwrap_err().status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        retried.unwrap(),
        "a fresh retry restores the chunks and publishes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_content_locks_last_until_the_publication_transaction_ends() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let (record, _) = backend
        .prepare_file_stream(
            "locked-file",
            RequestBodyReader::from_bytes(Bytes::from_static(b"locked backing bytes")),
            None,
        )
        .await
        .unwrap();
    let physical = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
    let mut publication = fixture.index().pool().begin().await.unwrap();
    backend
        .verify_staged_file_on_connection(&mut publication, &record)
        .await
        .unwrap();
    let mut contender = fixture.admin.begin().await.unwrap();
    let statement = "SELECT pg_try_advisory_xact_lock(1397507140, (hashtextextended('shardline:remote-delete:' || $1, 0) & 255)::INTEGER)";
    let before: bool = sqlx::query_scalar(statement)
        .bind(physical.as_str())
        .fetch_one(&mut *contender)
        .await
        .unwrap();
    publication.rollback().await.unwrap();
    let after: bool = sqlx::query_scalar(statement)
        .bind(physical.as_str())
        .fetch_one(&mut *contender)
        .await
        .unwrap();
    contender.rollback().await.unwrap();
    fixture.finish().await;
    assert!(
        !before,
        "DELETE admission must wait through metadata publication"
    );
    assert!(
        after,
        "the backing-object lock must release with the transaction"
    );
}
