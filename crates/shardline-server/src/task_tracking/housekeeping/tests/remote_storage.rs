#![allow(clippy::unwrap_used)]
use super::*;
use crate::ServerObjectStore;
use shardline_protocol::SecretString;
use shardline_storage::{S3ObjectStore, S3ObjectStoreConfig};
use std::sync::Mutex;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

struct StopDuringResponse {
    guard: Arc<Mutex<Option<HousekeepingGuard>>>,
    response: ResponseTemplate,
}

impl Respond for StopDuringResponse {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        // The real S3 request has arrived, but its response has not reached the
        // caller. Drop the router lifetime here to avoid timing-based races.
        drop(self.guard.lock().unwrap().take());
        self.response.clone()
    }
}

fn s3_config(endpoint: String) -> S3ObjectStoreConfig {
    S3ObjectStoreConfig::new("bucket".into(), "us-east-1".into())
        .with_endpoint(Some(endpoint))
        .with_allow_http(true)
        .with_credentials(
            Some(SecretString::new("key".into())),
            Some(SecretString::new("secret".into())),
            None,
        )
}

async fn verify_remote_read_shutdown(stop_during_metadata: bool) {
    let remote = MockServer::start().await;
    let s3 = s3_config(remote.uri());
    let ordinary = tokio::task::spawn_blocking(move || {
        TaskEvidenceStore::from_objects(ServerObjectStore::S3(S3ObjectStore::new(s3).unwrap()))
    })
    .await
    .unwrap();
    let (guard, stopped) = HousekeepingGuard::new();
    let guard = Arc::new(Mutex::new(Some(guard)));
    let housekeeping = ordinary.clone().with_housekeeping_stop(stopped);
    let metadata = ResponseTemplate::new(200)
        .insert_header("content-length", "8")
        .insert_header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
        .insert_header("etag", "\"original\"");
    let body = ResponseTemplate::new(206)
        .insert_header("content-range", "bytes 0-7/8")
        .set_body_bytes(b"original".to_vec());
    if stop_during_metadata {
        Mock::given(method("HEAD"))
            .respond_with(StopDuringResponse {
                guard: guard.clone(),
                response: metadata,
            })
            .expect(2)
            .mount(&remote)
            .await;
        Mock::given(method("GET"))
            .respond_with(body)
            .expect(1)
            .mount(&remote)
            .await;
    } else {
        Mock::given(method("HEAD"))
            .respond_with(metadata)
            .expect(2)
            .mount(&remote)
            .await;
        Mock::given(method("GET"))
            .respond_with(StopDuringResponse {
                guard: guard.clone(),
                response: body,
            })
            .expect(2)
            .mount(&remote)
            .await;
    }
    let (result, subsequent) = tokio::task::spawn_blocking(move || {
        let result = housekeeping.read("fixture", 8);
        let subsequent = housekeeping.read("fixture", 8);
        (result, subsequent)
    })
    .await
    .unwrap();
    if stop_during_metadata {
        assert!(
            matches!(result, Err(crate::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted)
        );
    } else {
        assert_eq!(result.unwrap(), Some(b"original".to_vec()));
    }
    assert!(
        matches!(subsequent, Err(crate::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    let before = remote.received_requests().await.unwrap();
    assert_eq!(before.len(), if stop_during_metadata { 1 } else { 2 });
    assert!(
        before
            .iter()
            .all(|request| request.url.path() == "/bucket/_shardline_tasks/v1/fixture")
    );
    let fresh = tokio::task::spawn_blocking(move || ordinary.read("fixture", 8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fresh, Some(b"original".to_vec()));
    remote.verify().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_remote_metadata_stops_before_body_download() {
    verify_remote_read_shutdown(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_remote_body_allows_inflight_read_and_stops_next_call() {
    verify_remote_read_shutdown(false).await;
}

async fn verify_remote_metadata_mutation_shutdown(deletion: bool) {
    let remote = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_object_storage(
        crate::ObjectStorageAdapter::S3,
        Some(s3_config(remote.uri())),
    );
    let ordinary = tokio::task::spawn_blocking(move || {
        let mut store = TaskEvidenceStore::from_config(&config)?;
        // Match from_backend's SQLite/S3 composition without starting unrelated
        // upload recovery: upgraded history is metadata-backed, legacy is S3.
        store.local_metadata = Some(shardline_index::LocalIndexStore::open(
            config.root_dir().to_owned(),
        ));
        Ok::<_, crate::ServerError>(store)
    })
    .await
    .unwrap()
    .unwrap();
    let metadata = ordinary.local_metadata.clone().unwrap();
    let key = TaskEvidenceStore::key("fixture").unwrap();
    if deletion {
        metadata
            .publish_task_artifact(key.as_str(), b"original")
            .unwrap();
    }
    let (guard, stopped) = HousekeepingGuard::new();
    let housekeeping = ordinary.clone().with_housekeeping_stop(stopped);
    let response = if deletion {
        ResponseTemplate::new(200).set_body_string(
            "<DeleteResult><Deleted><Key>_shardline_tasks/v1/fixture</Key></Deleted></DeleteResult>",
        )
    } else {
        ResponseTemplate::new(404)
    };
    // The existing S3 adapter uses DeleteObjects even for one history key.
    Mock::given(method(if deletion { "POST" } else { "HEAD" }))
        .respond_with(StopDuringResponse {
            guard: Arc::new(Mutex::new(Some(guard))),
            response: response.clone(),
        })
        .expect(1)
        .mount(&remote)
        .await;
    let result = tokio::task::spawn_blocking(move || {
        if deletion {
            housekeeping.delete_artifact(&TaskEvidenceStore::key("fixture").unwrap())
        } else {
            housekeeping.publish("fixture", b"original")
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&result, Err(crate::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted),
        "unexpected mutation result: {result:?}"
    );
    assert_eq!(
        metadata.read_task_artifact(key.as_str(), 8).unwrap(),
        deletion.then(|| b"original".to_vec())
    );
    let requests = remote.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].url.path(),
        if deletion {
            "/bucket"
        } else {
            "/bucket/_shardline_tasks/v1/fixture"
        }
    );
    if deletion {
        assert!(
            String::from_utf8_lossy(&requests[0].body)
                .contains("<Key>_shardline_tasks/v1/fixture</Key>")
        );
    }
    remote.verify().await;
    remote.reset().await;
    Mock::given(method(if deletion { "POST" } else { "HEAD" }))
        .respond_with(response)
        .expect(1)
        .mount(&remote)
        .await;
    tokio::task::spawn_blocking(move || {
        if deletion {
            ordinary.delete_artifact(&TaskEvidenceStore::key("fixture").unwrap())
        } else {
            ordinary.publish("fixture", b"original")
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        metadata.read_task_artifact(key.as_str(), 8).unwrap(),
        (!deletion).then(|| b"original".to_vec())
    );
    remote.verify().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_remote_absence_check_stops_before_metadata_publication() {
    verify_remote_metadata_mutation_shutdown(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_remote_delete_preserves_metadata_for_fresh_retirement() {
    verify_remote_metadata_mutation_shutdown(true).await;
}
