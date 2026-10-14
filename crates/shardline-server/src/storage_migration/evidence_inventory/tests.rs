#![allow(clippy::unwrap_used)]
use super::*;
use crate::{StorageMigrationEndpoint, StorageMigrationOptions, run_storage_migration};
use shardline_protocol::SecretString;
use shardline_storage::{ObjectPrefix, ObjectStore, S3ObjectStoreConfig};
use std::{collections::BTreeMap, sync::Arc};
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{method, path, path_regex, query_param},
};

#[derive(Clone)]
struct SourceObjects(Arc<BTreeMap<String, Vec<u8>>>);

impl Respond for SourceObjects {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let Some(bytes) = request
            .url
            .path()
            .strip_prefix("/bucket/")
            .and_then(|key| self.0.get(key))
        else {
            return ResponseTemplate::new(404);
        };
        let metadata = ResponseTemplate::new(if request.method.as_str() == "HEAD" {
            200
        } else {
            206
        })
        .insert_header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
        .insert_header("etag", "\"original\"");
        if request.method.as_str() == "HEAD" {
            metadata.insert_header("content-length", bytes.len().to_string())
        } else {
            metadata
                .insert_header(
                    "content-range",
                    format!(
                        "bytes 0-{}/{}",
                        bytes.len().checked_sub(1).unwrap(),
                        bytes.len()
                    ),
                )
                .set_body_bytes(bytes.clone())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_copy_reuses_legacy_manifest_and_preserves_unrecognized_bytes() {
    let source_root = tempfile::tempdir().unwrap();
    let destination_root = tempfile::tempdir().unwrap();
    let objects = super::super::endpoint_store(&StorageMigrationEndpoint::LocalStateRoot(
        source_root.path().to_owned(),
    ))
    .unwrap();
    let deployment = penelope::core::task::Digest::of(b"legacy-remote-copy");
    let unfinished = crate::task_tracking::EmbeddedTaskTracker::new(objects.clone(), deployment)
        .begin(
            "unfinished",
            b"input",
            shardline_reliability::MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let legacy = TaskEvidenceStore::from_objects(objects.clone());
    let id = legacy.summaries(deployment, None, 10).unwrap().remove(0).id;
    for number in 0..64 {
        let bytes = format!("saved legacy artifact {number}").into_bytes();
        legacy
            .publish(
                &format!("runs/{id}/blobs/{}", blake3::hash(&bytes).to_hex()),
                &bytes,
            )
            .unwrap();
    }
    let payloads = BTreeMap::from([
        (
            "user-payload".to_owned(),
            b"protected user payload".to_vec(),
        ),
        (
            "_shardline_tasks/v1/customer-data".to_owned(),
            b"arbitrary legacy namespace bytes".to_vec(),
        ),
        (
            "_shardline_tasks/v1/manifests/damaged".to_owned(),
            b"unrecognized legacy manifest".to_vec(),
        ),
        (
            "_shardline_tasks/v1/runs/damaged/blobs/unknown".to_owned(),
            b"unrecognized legacy artifact".to_vec(),
        ),
    ]);
    for (key, bytes) in &payloads {
        let path = source_root.path().join("chunks").join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let snapshot = objects
        .list_prefix(&ObjectPrefix::parse("").unwrap())
        .unwrap()
        .into_iter()
        .map(|entry| {
            let bytes =
                crate::object_store::read_full_object(&objects, entry.key(), entry.length())
                    .unwrap();
            (entry.key().as_str().to_owned(), bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let remote = MockServer::start().await;
    let contents = snapshot.iter().map(|(key, bytes)| format!(
        "<Contents><Key>{key}</Key><LastModified>2026-10-14T00:00:00.000Z</LastModified><ETag>original</ETag><Size>{}</Size></Contents>", bytes.len()
    )).collect::<String>();
    Mock::given(method("GET")).and(path("/bucket")).and(query_param("list-type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "<ListBucketResult><Name>bucket</Name><Prefix></Prefix><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
        ))).mount(&remote).await;
    let responses = SourceObjects(Arc::new(snapshot.clone()));
    for verb in ["HEAD", "GET"] {
        Mock::given(method(verb))
            .and(path_regex("^/bucket/.+"))
            .respond_with(responses.clone())
            .mount(&remote)
            .await;
    }
    let source = S3ObjectStoreConfig::new("bucket".into(), "us-east-1".into())
        .with_endpoint(Some(remote.uri()))
        .with_allow_http(true)
        .with_credentials(
            Some(SecretString::new("key".into())),
            Some(SecretString::new("secret".into())),
            None,
        );
    let options = StorageMigrationOptions::new(
        StorageMigrationEndpoint::S3(source),
        StorageMigrationEndpoint::LocalStateRoot(destination_root.path().to_owned()),
    );
    for dry_run in [true, false] {
        let offset = remote.received_requests().await.unwrap().len();
        let call = options.clone().with_dry_run(dry_run);
        let report = tokio::task::spawn_blocking(move || run_storage_migration(&call))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.scanned_objects, 4);
        assert_eq!(
            report.scanned_bytes,
            payloads
                .values()
                .map(|bytes| u64::try_from(bytes.len()).unwrap())
                .sum::<u64>()
        );
        assert_eq!(report.inserted_objects, if dry_run { 0 } else { 4 });
        let requests = remote.received_requests().await.unwrap();
        let current = &requests[offset..];
        let manifest_path = format!("/bucket/_shardline_tasks/v1/manifests/{id}");
        let manifest_reads = current
            .iter()
            .filter(|request| request.url.path() == manifest_path)
            .count();
        assert_eq!(
            manifest_reads, 2,
            "one HEAD and one GET per legacy run, dry_run={dry_run}"
        );
        assert!(
            current
                .iter()
                .all(|request| matches!(request.method.as_str(), "HEAD" | "GET"))
        );
        let destination = super::super::endpoint_store(&options.destination).unwrap();
        let copied = destination
            .list_prefix(&ObjectPrefix::parse("").unwrap())
            .unwrap();
        assert_eq!(copied.len(), if dry_run { 0 } else { 4 });
        if !dry_run {
            for (key, bytes) in &payloads {
                assert_eq!(
                    crate::object_store::read_full_object(
                        &destination,
                        &ObjectKey::parse(key).unwrap(),
                        u64::try_from(bytes.len()).unwrap()
                    )
                    .unwrap(),
                    *bytes
                );
            }
        }
    }
    for (key, bytes) in snapshot {
        assert_eq!(
            std::fs::read(source_root.path().join("chunks").join(key)).unwrap(),
            bytes
        );
    }
    drop(unfinished);
}
