#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::maintenance::MaintenanceStorageAdapter;
use shardline_server::ObjectStorageAdapter;
use std::{num::NonZeroUsize, os::unix::fs::PermissionsExt, path::Path};

fn config(root: &Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_index_postgres_url("postgres://unused:unused@127.0.0.1:1/unused".to_owned())
    .unwrap()
    .with_object_storage(
        ObjectStorageAdapter::S3,
        Some(
            shardline_storage::S3ObjectStoreConfig::new(
                "unused-copy-bucket".to_owned(),
                "unused-region".to_owned(),
            )
            .with_endpoint(Some("http://127.0.0.1:1".to_owned()))
            .with_allow_http(true),
        ),
    )
}

fn operation(
    root: &Path,
    to: MaintenanceStorageAdapter,
    destination: Option<&Path>,
) -> MaintenanceOperation {
    MaintenanceOperation::StorageMigrate {
        root: root.to_path_buf(),
        from: MaintenanceStorageAdapter::Local,
        from_root: Some(root.to_path_buf()),
        to,
        to_root: destination.map(Path::to_path_buf),
        prefix: String::new(),
        dry_run: false,
    }
}

#[test]
fn s3_copy_history_uses_private_metadata_without_contacting_payload_or_postgres() {
    let source = tempfile::tempdir().unwrap();
    let archive = tempfile::tempdir().unwrap();
    std::fs::set_permissions(archive.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let configured = config(source.path());
    let copy = operation(source.path(), MaintenanceStorageAdapter::S3, None);
    let history = store(&copy, &configured, Some(archive.path())).unwrap();
    let key = "runs/private-copy/artifacts/report";
    assert!(history.publish(key, b"orphan result").is_err());
    history
        .publish("manifests/private-copy", b"admitted copy")
        .unwrap();
    history.publish(key, b"exact copy result").unwrap();
    drop(history);
    let reopened = store(&copy, &configured, Some(archive.path())).unwrap();
    assert_eq!(
        reopened.read(key, 1024).unwrap(),
        Some(b"exact copy result".to_vec())
    );
    assert!(reopened.publish(key, b"conflicting result").is_err());
    assert_eq!(
        reopened.read(key, 1024).unwrap(),
        Some(b"exact copy result".to_vec())
    );
    assert_eq!(std::fs::read_dir(source.path()).unwrap().count(), 0);
    assert_eq!(
        std::fs::read_dir(archive.path().join("chunks"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn s3_copy_without_private_history_does_not_fall_back_to_payload_publication() {
    let source = tempfile::tempdir().unwrap();
    let configured = config(source.path());
    let copy = operation(source.path(), MaintenanceStorageAdapter::S3, None);
    assert!(store(&copy, &configured, None).is_err());
    assert_eq!(std::fs::read_dir(source.path()).unwrap().count(), 0);
}

#[test]
fn local_copy_history_does_not_inherit_an_unrelated_server_database() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let configured = config(source.path());
    let copy = operation(
        source.path(),
        MaintenanceStorageAdapter::Local,
        Some(destination.path()),
    );
    let publication = crate::maintenance::publication_config(&copy, &configured).unwrap();
    assert!(publication.index_postgres_url().is_none());
    assert_eq!(
        publication.object_storage_adapter(),
        ObjectStorageAdapter::Local
    );
    let history = store(&copy, &publication, None).unwrap();
    assert!(
        history
            .publish("runs/local-copy/artifacts/report", b"orphan result")
            .is_err()
    );
    history
        .publish("manifests/local-copy", b"admitted copy")
        .unwrap();
    history
        .publish("runs/local-copy/artifacts/report", b"local result")
        .unwrap();
    let reopened = TaskEvidenceStore::from_config(&publication).unwrap();
    assert_eq!(
        reopened
            .read("runs/local-copy/artifacts/report", 1024)
            .unwrap(),
        Some(b"local result".to_vec())
    );
    assert_eq!(std::fs::read_dir(source.path()).unwrap().count(), 0);
    assert_eq!(
        std::fs::read_dir(destination.path().join("chunks"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn local_to_s3_history_is_inspectable_in_source_without_unrelated_database_access() {
    let source = tempfile::tempdir().unwrap();
    let configured = config(source.path());
    let copy = operation(source.path(), MaintenanceStorageAdapter::S3, None);
    let snapshot = crate::maintenance::configuration::Configuration {
        config: configured.clone(),
        command: crate::CliCommand::StorageMigrate {
            from: ObjectStorageAdapter::Local,
            from_root: Some(source.path().to_path_buf()),
            to: ObjectStorageAdapter::S3,
            to_root: None,
            prefix: String::new(),
            dry_run: false,
        },
        config_path: None,
        migration: Some((
            shardline_server::StorageMigrationEndpoint::LocalStateRoot(source.path().to_path_buf()),
            shardline_server::StorageMigrationEndpoint::S3(
                configured.s3_object_store_config().unwrap().clone(),
            ),
        )),
    };
    crate::maintenance::configuration::scope(snapshot, || {
        let publication = crate::maintenance::publication_config(&copy, &configured).unwrap();
        assert!(publication.index_postgres_url().is_none());
        assert_eq!(publication.root_dir(), source.path());
        assert_eq!(
            publication.object_storage_adapter(),
            ObjectStorageAdapter::Local
        );
        let history = store(&copy, &publication, None).unwrap();
        history
            .publish("manifests/local-to-s3", b"admitted copy")
            .unwrap();
        history
            .publish("runs/local-to-s3/artifacts/report", b"saved outcome")
            .unwrap();
        let reopened = TaskEvidenceStore::from_config(&publication).unwrap();
        assert_eq!(
            reopened
                .read("runs/local-to-s3/artifacts/report", 1024)
                .unwrap(),
            Some(b"saved outcome".to_vec())
        );
        assert!(source.path().join("metadata.sqlite3").is_file());
        assert_eq!(
            std::fs::read_dir(source.path().join("chunks"))
                .unwrap()
                .count(),
            0
        );
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn unavailable_postgres_history_does_not_publish_into_payload_storage() {
    let root = tempfile::tempdir().unwrap();
    let configured = config(root.path()).with_object_storage(ObjectStorageAdapter::Local, None);
    let operation = MaintenanceOperation::IndexRebuild {
        root: root.path().to_path_buf(),
    };
    tokio::task::spawn_blocking(move || {
        let history = store(&operation, &configured, None).unwrap();
        assert!(history.publish("new-history", b"optional result").is_err());
        assert!(history.read("new-history", 1024).unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(root.path().join("chunks"))
                .unwrap()
                .count(),
            0
        );
    })
    .await
    .unwrap();
}

#[test]
fn s3_only_copy_history_uses_local_project_metadata_and_rejects_unsafe_paths() {
    for unsafe_root in [false, true] {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        if unsafe_root {
            std::os::unix::fs::symlink(outside.path(), project.path().join(".shardline")).unwrap();
        }
        let configured = config(project.path());
        let copy = MaintenanceOperation::StorageMigrate {
            root: project.path().to_path_buf(),
            from: MaintenanceStorageAdapter::S3,
            from_root: None,
            to: MaintenanceStorageAdapter::S3,
            to_root: None,
            prefix: String::new(),
            dry_run: false,
        };
        let endpoint = shardline_server::StorageMigrationEndpoint::S3(
            configured.s3_object_store_config().unwrap().clone(),
        );
        let snapshot = crate::maintenance::configuration::Configuration {
            config: configured.clone(),
            command: crate::CliCommand::StorageMigrate {
                from: ObjectStorageAdapter::S3,
                from_root: None,
                to: ObjectStorageAdapter::S3,
                to_root: None,
                prefix: String::new(),
                dry_run: false,
            },
            config_path: None,
            migration: Some((endpoint.clone(), endpoint)),
        };
        crate::maintenance::configuration::scope(snapshot, || {
            // Resolving optional history must not fail the copy before work.
            let publication = crate::maintenance::publication_config(&copy, &configured).unwrap();
            assert!(publication.index_postgres_url().is_none());
            assert_eq!(
                publication.root_dir(),
                project.path().join(".shardline/data")
            );
            let history = store(&copy, &publication, None);
            if unsafe_root {
                assert!(history.is_err());
                assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
            } else {
                let history = history.unwrap();
                history
                    .publish("manifests/s3-only", b"admitted copy")
                    .unwrap();
                history
                    .publish("runs/s3-only/artifacts/report", b"saved outcome")
                    .unwrap();
                let reopened = TaskEvidenceStore::from_config(&publication).unwrap();
                assert_eq!(
                    reopened
                        .read("runs/s3-only/artifacts/report", 1024)
                        .unwrap(),
                    Some(b"saved outcome".to_vec())
                );
                assert_eq!(
                    std::fs::read_dir(publication.root_dir().join("chunks"))
                        .unwrap()
                        .count(),
                    0
                );
            }
        });
    }
}
