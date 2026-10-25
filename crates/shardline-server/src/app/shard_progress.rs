//! Xet shard finalization events and ownership for their response lifetime.

use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    body::{Body, Bytes},
    extract::State,
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};

use super::{AppState, reconstruction_routes::XetWriteRepository};
use crate::{
    ServerError,
    admission::weights,
    maintenance_barrier::{MaintenanceBarrierGuard, RequestSession},
    upload_ingest::RequestBodyReader,
};

/// Only this internal marker transfers middleware ownership to a response body.
#[derive(Clone)]
pub(super) struct StreamingPublication;

pub(super) fn retain_postgres_owner(response: Response, session: RequestSession) -> Response {
    let (parts, body) = response.into_parts();
    let stream = stream::unfold(
        (body.into_data_stream(), session),
        |(mut body, session)| async move {
            match session.scope(body.next()).await {
                Some(frame) => Some((frame, (body, session))),
                None => {
                    session.finish().await;
                    None
                }
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

pub(super) fn retain_local_owner(response: Response, guard: MaintenanceBarrierGuard) -> Response {
    let (parts, body) = response.into_parts();
    let stream = stream::unfold(
        (body.into_data_stream(), guard),
        |(mut body, guard)| async move { body.next().await.map(|frame| (frame, (body, guard))) },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

pub(super) async fn upload(
    State(state): State<Arc<AppState>>,
    repo: XetWriteRepository,
    body: Body,
) -> Result<Response, ServerError> {
    let admit = state
        .admission
        .try_acquire(weights::SHARD_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    // No detached task: dropping the response cancels publication and releases
    // admission together with its non-replaceable middleware owner.
    let publication = async move {
        let (_admit, _parsing) = (admit, parsing);
        tokio::time::timeout(
            super::timeouts::REQUEST_TOTAL,
            state.backend.upload_shard_stream(
                body,
                repo.capability().namespace(),
                state.config.shard_metadata_limits(),
            ),
        )
        .await
        .unwrap_or(Err(ServerError::RequestTimedOut))
    };
    let events = publication_events(publication, Duration::from_secs(5));
    let mut response = (
        [
            ("content-type", "application/x-ndjson"),
            ("cache-control", "no-cache"),
        ],
        Body::from_stream(events),
    )
        .into_response();
    response.extensions_mut().insert(StreamingPublication);
    Ok(response)
}

fn publication_events(
    publication: impl std::future::Future<Output = Result<crate::ShardUploadResponse, ServerError>>
    + Send,
    heartbeat_interval: Duration,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> + Send {
    let publication = Box::pin(publication);
    let heartbeat = Bytes::from_static(b"{\"type\":\"validating\",\"verified\":0,\"total\":0}\n");
    stream::unfold(
        (publication, false, false),
        move |(mut publication, started, done)| {
            let heartbeat = heartbeat.clone();
            async move {
                if done {
                    return None;
                }
                if !started {
                    return Some((Ok::<_, Infallible>(heartbeat), (publication, true, false)));
                }
                tokio::select! {
                    result = &mut publication => {
                        let event = match result {
                            Ok(result) => {
                                crate::metrics::record_shard_stored();
                                serde_json::json!({"type":"result", "result":result.result})
                            }
                            Err(error) => {
                                let retryable = matches!(
                                    error.status_code(),
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                                        | axum::http::StatusCode::TOO_MANY_REQUESTS
                                );
                                tracing::warn!(%error, "streaming shard publication failed");
                                serde_json::json!({"type":"error", "message":"shard validation or publication failed", "retryable":retryable})
                            }
                        };
                        let mut bytes = event.to_string();
                        bytes.push('\n');
                        Some((Ok(Bytes::from(bytes)), (publication, true, true)))
                    }
                    () = tokio::time::sleep(heartbeat_interval) => {
                        Some((Ok(heartbeat), (publication, true, false)))
                    }
                }
            }
        },
    )
}

#[cfg(test)]
#[cfg(feature = "docker")]
mod cancellation_http;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn heartbeats_continue_during_running_worker_and_body_drop_cancels_it() {
        use crate::admission::{BoundedPool, cancellable_work_with_permit};
        let pool = BoundedPool::new(std::num::NonZeroUsize::new(1).unwrap());
        let permit = pool.acquire().await.unwrap();
        let (started, running) = tokio::sync::oneshot::channel();
        let (resume, wait) = std::sync::mpsc::channel();
        let (stopped, observed) = tokio::sync::oneshot::channel();
        let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let publication_marker = published.clone();
        let publication = async move {
            let (_prepared, _permit) = cancellable_work_with_permit(permit, move |cancel| {
                started.send(()).unwrap();
                wait.recv().unwrap();
                let result = cancel.check();
                stopped.send(result.is_err()).unwrap();
                result
            })
            .await?;
            publication_marker.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::ShardUploadResponse { result: 1 })
        };
        let mut events = Box::pin(publication_events(publication, Duration::from_millis(5)));
        assert!(events.next().await.is_some());
        // Polling the second frame starts real blocking work on this single
        // async-thread runtime, and must still deliver the heartbeat promptly.
        let frame = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        running.await.unwrap();
        let event: serde_json::Value = serde_json::from_slice(&frame).unwrap();
        assert_eq!(event["type"], "validating");
        drop(events);
        assert!(
            pool.try_acquire().is_none(),
            "running work retains admission"
        );
        resume.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), observed)
                .await
                .unwrap()
                .unwrap()
        );
        let _released = tokio::time::timeout(Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        assert!(!published.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn stalled_publication_heartbeats_then_emits_one_terminal_result() {
        let (complete, pending) = tokio::sync::oneshot::channel();
        let events = publication_events(
            async move { pending.await.unwrap() },
            Duration::from_secs(5),
        );
        tokio::pin!(events);
        for _ in 0..3 {
            let frame = tokio::time::timeout(Duration::from_secs(6), events.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let event: serde_json::Value = serde_json::from_slice(&frame).unwrap();
            assert_eq!(event["type"], "validating");
        }
        complete
            .send(Ok(crate::ShardUploadResponse { result: 1 }))
            .unwrap();
        let frame = events.next().await.unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&frame).unwrap();
        assert_eq!(event, serde_json::json!({"type":"result", "result":1}));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn cancellation_drops_pending_publication_and_its_admission() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let publication = async move {
            let _permit = permit;
            std::future::pending::<Result<crate::ShardUploadResponse, ServerError>>().await
        };
        let mut events = Box::pin(publication_events(publication, Duration::from_millis(5)));
        assert!(events.next().await.is_some());
        assert!(events.next().await.is_some());
        assert_eq!(slots.available_permits(), 0);
        drop(events);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn publication_failure_has_only_one_sanitized_terminal_event() {
        let events = publication_events(
            async { Err(ServerError::RequestTimedOut) },
            Duration::from_secs(5),
        );
        tokio::pin!(events);
        assert!(events.next().await.is_some());
        let frame = events.next().await.unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&frame).unwrap();
        assert_eq!(event["type"], "error");
        assert!(event.get("result").is_none());
        assert_eq!(event["message"], "shard validation or publication failed");
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn streaming_failures_preserve_retryable_http_classification() {
        for (error, retryable) in [
            (ServerError::RequestTimedOut, true),
            (ServerError::WorkQueueSaturated, true),
            (ServerError::TooManyUploadSessions, true),
            (ServerError::StaleResourceFence, true),
            (
                ServerError::InvalidSerializedShard(
                    crate::InvalidSerializedShardError::ParserRejectedMetadata,
                ),
                false,
            ),
            (ServerError::NotFound, false),
            (
                ServerError::Io(std::io::Error::other("private storage details")),
                false,
            ),
        ] {
            let events = publication_events(async move { Err(error) }, Duration::from_secs(5));
            tokio::pin!(events);
            assert!(events.next().await.is_some());
            let frame = events.next().await.unwrap().unwrap();
            let event: serde_json::Value = serde_json::from_slice(&frame).unwrap();
            assert_eq!(event["type"], "error");
            assert_eq!(event["retryable"], retryable);
            assert_eq!(event["message"], "shard validation or publication failed");
            assert!(event.get("result").is_none());
            assert!(events.next().await.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_shard_publication_is_visible_on_second_postgres_node() {
        use tower::ServiceExt;
        if std::env::var("DATABASE_URL").is_err() {
            return;
        }
        let cluster = crate::app::build_oci_postgres_test_cluster()
            .await
            .expect("configured PostgreSQL cluster must initialize");
        let content = b"streaming shard publication on shared PostgreSQL";
        let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(content);
        let backend = match &cluster.node_a.backend {
            crate::ServerBackend::Postgres(backend) => Some(backend),
            crate::ServerBackend::Local(_) => None,
        }
        .expect("PostgreSQL fixture must use PostgreSQL");
        backend.upload_xorb(&hash, xorb).await.unwrap();
        let (shard, file) = crate::test_fixtures::single_file_shard(&[(content, &hash)]);
        let router = |state: Arc<AppState>| {
            axum::Router::new()
                .route("/v2/shards", axum::routing::post(upload))
                .route(
                    "/v1/reconstructions/{file_id}",
                    axum::routing::get(super::super::reconstruction_routes::reconstruction),
                )
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    super::super::gc_write_barrier_middleware,
                ))
                .with_state(state)
        };
        let response = router(cluster.node_a.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/shards")
                    .body(Body::from(shard))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let result: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
        assert_eq!(result["type"], "result");
        let response = router(cluster.node_b.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/v1/reconstructions/{file}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let reconstruction: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(!reconstruction["terms"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn streaming_local_owner_is_held_until_body_drop() {
        let root = tempfile::tempdir().unwrap();
        let guard = crate::maintenance_barrier::acquire_local_shared(root.path())
            .await
            .unwrap();
        let response = retain_local_owner(
            Body::from_stream(stream::pending::<Result<Bytes, Infallible>>()).into_response(),
            guard,
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                crate::maintenance_barrier::acquire_local_exclusive(root.path())
            )
            .await
            .is_err()
        );
        drop(response);
        let _exclusive = tokio::time::timeout(
            Duration::from_secs(2),
            crate::maintenance_barrier::acquire_local_exclusive(root.path()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn streaming_postgres_owner_scopes_body_and_rejects_lost_session() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 2).unwrap();
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let identity = Arc::new(());
        for terminate in [false, true] {
            let owner = RequestSession::acquire_bounded(&pool, identity.clone(), &slots)
                .await
                .unwrap();
            let pid: i32 = owner
                .scope(async {
                    let mut connection = RequestSession::connection_for(&identity).await.unwrap();
                    sqlx::query_scalar("SELECT pg_backend_pid()")
                        .fetch_one(&mut **connection.as_mut().unwrap())
                        .await
                        .unwrap()
                })
                .await;
            let check_identity = identity.clone();
            let response = Body::from_stream(stream::once(async move {
                // Verifies that polling the response retains task-local ownership,
                // not merely a connection that the backend can silently replace.
                let mut connection = RequestSession::connection_for(&check_identity)
                    .await
                    .expect("body must inherit owner");
                let actual: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut **connection.as_mut().unwrap())
                    .await
                    .map_err(shardline_index::PostgresMetadataStoreError::from)?;
                assert_eq!(actual, pid);
                Ok::<_, ServerError>(Bytes::from_static(b"done"))
            }))
            .into_response();
            let response = retain_postgres_owner(response, owner);
            assert_eq!(slots.available_permits(), 0);
            if terminate {
                sqlx::query("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            let result = axum::body::to_bytes(response.into_body(), 64).await;
            assert_eq!(result.is_err(), terminate);
            assert_eq!(slots.available_permits(), 1);
        }
        let owner = RequestSession::acquire_bounded(&pool, identity, &slots)
            .await
            .unwrap();
        let response = retain_postgres_owner(
            Body::from_stream(stream::pending::<Result<Bytes, Infallible>>()).into_response(),
            owner,
        );
        assert_eq!(slots.available_permits(), 0);
        drop(response);
        assert_eq!(slots.available_permits(), 1);
        pool.close().await;
    }
}
