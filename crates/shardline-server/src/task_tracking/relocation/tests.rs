#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::{ServerConfig, object_store_from_config, task_tracking::EmbeddedTaskTracker};
use shardline_reliability::MaintenanceArtifactKind;
use std::{collections::BTreeMap, num::NonZeroUsize};

struct Fixture {
    _root: tempfile::TempDir,
    config: ServerConfig,
    store: TaskEvidenceStore,
    legacy: TaskEvidenceStore,
    deployment: Digest,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(4096).unwrap(),
        );
        let deployment = super::super::deployment_fingerprint(&config).unwrap();
        let legacy = TaskEvidenceStore::from_objects(object_store_from_config(&config).unwrap());
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        Self {
            _root: root,
            config,
            store,
            legacy,
            deployment,
        }
    }

    fn complete(&self, operation: &str) -> String {
        EmbeddedTaskTracker::new(self.legacy.objects.clone(), self.deployment)
            .begin(
                operation,
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"original report")
            .unwrap();
        self.legacy
            .summaries(self.deployment, None, 100)
            .unwrap()
            .into_iter()
            .find(|run| run.operation == operation)
            .unwrap()
            .id
    }

    fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.legacy
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .into_iter()
            .map(|artifact| {
                let suffix = artifact
                    .key()
                    .as_str()
                    .strip_prefix(TASK_EVIDENCE_PREFIX)
                    .unwrap()
                    .to_owned();
                let bytes = self
                    .legacy
                    .read(&suffix, artifact.length())
                    .unwrap()
                    .unwrap();
                (suffix, bytes)
            })
            .collect()
    }
}

#[test]
fn shutdown_stops_relocation_before_skipping_oversized_legacy_artifacts() {
    let fixture = Fixture::new();
    let id = fixture.complete("finished");
    let original = fixture.snapshot();
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let housekeeping = fixture.store.clone().with_housekeeping_stop(stopped);
    let artifact = ObjectMetadata::new(
        TaskEvidenceStore::key(&format!("runs/{id}/artifacts/oversized")).unwrap(),
        4 * 1024 * 1024 + 1,
        None,
    );
    let mut budget = Budget::default();
    assert!(matches!(
        housekeeping.relocate_artifact(&artifact, &mut budget),
        Err(VisitError::Storage(ServerError::Io(error)))
            if error.kind() == std::io::ErrorKind::Interrupted
    ));
    assert_eq!(budget.moved, 0);
    assert_eq!(budget.bytes, 0);
    assert_eq!(fixture.snapshot(), original);
    assert!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .unwrap()
            > 0
    );
}

#[test]
fn finished_history_moves_exact_bytes_while_unfinished_and_arbitrary_objects_stay() {
    let fixture = Fixture::new();
    let id = fixture.complete("finished");
    let corrupt = fixture.complete("corrupt");
    let artifact = format!(
        "runs/{corrupt}/artifacts/{}",
        hex::encode(Digest::of(b"original report").0)
    );
    fixture
        .legacy
        .objects
        .delete_if_present(&TaskEvidenceStore::key(&artifact).unwrap())
        .unwrap();
    fixture
        .legacy
        .publish(&artifact, b"tampered report")
        .unwrap();
    let bad_binding = fixture.complete("bad-binding");
    fixture
        .legacy
        .publish(
            &format!("scope-bindings/{bad_binding}"),
            b"invalid association",
        )
        .unwrap();
    EmbeddedTaskTracker::new(
        fixture.legacy.objects.clone(),
        Digest::of(b"another deployment"),
    )
    .begin(
        "foreign",
        b"input",
        MaintenanceArtifactKind::LifecycleRepair,
    )
    .unwrap()
    .finish(true, b"foreign report")
    .unwrap();
    drop(
        EmbeddedTaskTracker::new(fixture.legacy.objects.clone(), fixture.deployment)
            .begin(
                "unfinished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap(),
    );
    fixture
        .legacy
        .publish("manifests/customer-data", b"user-owned data")
        .unwrap();
    fixture
        .legacy
        .publish("runs/no-manifest/customer-data", b"more user-owned data")
        .unwrap();
    let before = fixture.snapshot();
    let moved = fixture
        .store
        .relocate_finished_history(fixture.deployment)
        .unwrap();
    let after = fixture.snapshot();
    assert!(moved > 0);
    assert_eq!(before.len() - after.len(), moved);
    assert_eq!(
        after.get("manifests/customer-data"),
        Some(&b"user-owned data".to_vec())
    );
    assert_eq!(
        after.get("runs/no-manifest/customer-data"),
        Some(&b"more user-owned data".to_vec())
    );
    assert!(!after.contains_key(&format!("manifests/{id}")));
    assert!(after.contains_key(&format!("manifests/{corrupt}")));
    assert!(after.contains_key(&format!("manifests/{bad_binding}")));
    let reopened = TaskEvidenceStore::from_config(&fixture.config).unwrap();
    for (key, bytes) in before {
        assert_eq!(
            reopened
                .read(&key, u64::try_from(bytes.len()).unwrap())
                .unwrap(),
            Some(bytes)
        );
    }
    assert_eq!(
        reopened
            .embedded_report(fixture.deployment, None, &id)
            .unwrap()
            .unwrap()["report"],
        "original report"
    );
    assert_eq!(
        reopened
            .relocate_finished_history(fixture.deployment)
            .unwrap(),
        0
    );
}

#[test]
fn interrupted_archive_publication_and_partial_deletion_resume_without_new_records() {
    for delete_source in [false, true] {
        let fixture = Fixture::new();
        let id = fixture.complete("finished");
        let before = fixture.snapshot();
        for (suffix, bytes) in before
            .iter()
            .filter(|(suffix, _)| suffix.starts_with("runs/"))
            .take(3)
        {
            let key = TaskEvidenceStore::key(suffix).unwrap();
            fixture
                .store
                .local_metadata
                .as_ref()
                .unwrap()
                .publish_task_artifact(key.as_str(), bytes)
                .unwrap();
            if delete_source {
                fixture.legacy.objects.delete_if_present(&key).unwrap();
            }
        }
        let reopened = TaskEvidenceStore::from_config(&fixture.config).unwrap();
        assert!(
            reopened
                .relocate_finished_history(fixture.deployment)
                .unwrap()
                > 0
        );
        assert!(fixture.snapshot().is_empty());
        for (suffix, bytes) in &before {
            assert_eq!(
                reopened
                    .read(suffix, u64::try_from(bytes.len()).unwrap())
                    .unwrap()
                    .as_ref(),
                Some(bytes)
            );
        }
        assert_eq!(
            reopened.summaries(fixture.deployment, None, 10).unwrap()[0].state,
            TaskRunState::Completed
        );
        assert_eq!(
            reopened
                .embedded_report(fixture.deployment, None, &id)
                .unwrap()
                .unwrap()["report"],
            "original report"
        );
    }
}

#[test]
fn artifact_budget_keeps_the_manifest_until_later_invocations_finish() {
    let fixture = Fixture::new();
    let id = fixture.complete("bounded");
    for index in 0..90 {
        let bytes = format!("additional evidence {index}").into_bytes();
        fixture
            .legacy
            .publish(
                &format!("runs/{id}/blobs/{}", hex::encode(Digest::of(&bytes).0)),
                &bytes,
            )
            .unwrap();
    }
    let before = fixture.snapshot();
    assert_eq!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .unwrap(),
        64
    );
    assert!(fixture.snapshot().contains_key(&format!("manifests/{id}")));
    assert_eq!(
        fixture
            .store
            .summaries(fixture.deployment, None, 10)
            .unwrap()[0]
            .state,
        TaskRunState::Completed
    );
    assert!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .unwrap()
            > 0
    );
    assert!(fixture.snapshot().is_empty());
    for (suffix, bytes) in before {
        assert_eq!(
            fixture
                .store
                .read(&suffix, u64::try_from(bytes.len()).unwrap())
                .unwrap(),
            Some(bytes)
        );
    }
}

#[test]
fn conflicting_metadata_does_not_overwrite_or_remove_the_legacy_bytes() {
    let fixture = Fixture::new();
    let id = fixture.complete("conflict");
    let suffix = format!(
        "runs/{id}/blobs/{}",
        hex::encode(Digest::of(b"original bytes").0)
    );
    fixture.legacy.publish(&suffix, b"original bytes").unwrap();
    let key = TaskEvidenceStore::key(&suffix).unwrap();
    fixture
        .store
        .local_metadata
        .as_ref()
        .unwrap()
        .publish_task_artifact(key.as_str(), b"conflicting bytes")
        .unwrap();
    assert!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .is_err()
    );
    assert_eq!(
        fixture.legacy.read(&suffix, 100).unwrap(),
        Some(b"original bytes".to_vec())
    );
    assert_eq!(
        fixture
            .store
            .local_metadata
            .as_ref()
            .unwrap()
            .read_task_artifact(key.as_str(), 100)
            .unwrap(),
        Some(b"conflicting bytes".to_vec())
    );
    assert!(
        fixture
            .legacy
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .is_some()
    );
}

#[test]
fn byte_budget_moves_large_output_in_batches_without_truncation() {
    let fixture = Fixture::new();
    let id = fixture.complete("large-output");
    let bytes = vec![42_u8; 4 * 1024 * 1024];
    for index in 0..5 {
        fixture
            .legacy
            .publish(&format!("runs/{id}/streams/stdout/{index:010}"), &bytes)
            .unwrap();
    }
    let before = fixture.snapshot();
    assert!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .unwrap()
            > 0
    );
    let after = fixture.snapshot();
    let removed_bytes: usize = before
        .iter()
        .filter(|(key, _)| !after.contains_key(*key))
        .map(|(_, bytes)| bytes.len())
        .sum();
    assert!(removed_bytes <= 16 * 1024 * 1024);
    assert!(after.contains_key(&format!("manifests/{id}")));
    assert!(
        fixture
            .store
            .relocate_finished_history(fixture.deployment)
            .unwrap()
            > 0
    );
    assert!(fixture.snapshot().is_empty());
    for (suffix, bytes) in before {
        assert_eq!(
            fixture
                .store
                .read(&suffix, u64::try_from(bytes.len()).unwrap())
                .unwrap(),
            Some(bytes)
        );
    }
}
