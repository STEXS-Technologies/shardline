#![allow(clippy::unwrap_used)]
use super::*;
use crate::{
    ServerConfig,
    task_tracking::{TaskEvidenceStore, deployment_fingerprint},
};
use shardline_storage::ObjectPrefix;
use std::num::NonZeroUsize;

#[test]
fn dropping_last_router_guard_stops_only_its_housekeeping_store() {
    let root = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let ordinary = TaskEvidenceStore::from_config(&config).unwrap();
    let (guard, stopped) = HousekeepingGuard::new();
    let guard = Arc::new(guard);
    let clone = guard.clone();
    let housekeeping = ordinary.clone().with_housekeeping_stop(stopped.clone());
    drop(guard);
    assert!(!stopped.load(Ordering::Relaxed));
    housekeeping.publish("fixture", b"original").unwrap();
    drop(clone);
    assert!(stopped.load(Ordering::Relaxed));
    assert!(
        matches!(housekeeping.read("fixture", 8), Err(crate::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    assert!(housekeeping.publish("late", b"late").is_err());
    let deployment = deployment_fingerprint(&config).unwrap();
    assert!(housekeeping.prune_completed(deployment, u64::MAX).is_err());
    assert!(housekeeping.relocate_finished_history(deployment).is_err());
    assert_eq!(ordinary.read("fixture", 8).unwrap().unwrap(), b"original");
    assert!(ordinary.read("late", 8).unwrap().is_none());
    ordinary.publish("fresh", b"available").unwrap();
}

#[test]
fn dropping_router_guard_stops_an_inventory_scan_between_items() {
    let root = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let ordinary = TaskEvidenceStore::from_config(&config).unwrap();
    for number in 0..100 {
        ordinary
            .publish(&format!("fixture/{number:03}"), b"original")
            .unwrap();
    }
    let (guard, stopped) = HousekeepingGuard::new();
    let mut guard = Some(guard);
    let housekeeping = ordinary.clone().with_housekeeping_stop(stopped);
    let prefix = ObjectPrefix::parse("_shardline_tasks/v1/fixture/").unwrap();
    let mut visited = 0;
    let result = housekeeping.visit_prefix(&prefix, |_| {
        visited += 1;
        drop(guard.take());
        Ok(())
    });
    assert!(
        matches!(result, Err(crate::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    assert_eq!(visited, 1);
    let mut remaining = 0;
    ordinary
        .visit_prefix(&prefix, |_| {
            remaining += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(remaining, 100);
}

#[test]
fn cancelled_housekeeping_preserves_partial_retirement_for_a_fresh_store() {
    let root = tempfile::tempdir().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let ordinary = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    crate::task_tracking::EmbeddedTaskTracker::from_store(ordinary.clone(), deployment)
        .begin(
            "finished",
            b"input",
            shardline_reliability::MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"report")
        .unwrap();
    let id = ordinary
        .summaries(deployment, None, 10)
        .unwrap()
        .remove(0)
        .id;
    let manifest = ordinary
        .read(&format!("manifests/{id}"), 65536)
        .unwrap()
        .unwrap();
    let identity: crate::task_tracking::TaskRunIdentity =
        serde_json::from_slice(&manifest).unwrap();
    assert!(ordinary.begin_retirement(&identity).unwrap());
    ordinary
        .delete_artifact(&TaskEvidenceStore::key(&format!("manifests/{id}")).unwrap())
        .unwrap();
    let (guard, stopped) = HousekeepingGuard::new();
    let housekeeping = ordinary.clone().with_housekeeping_stop(stopped);
    drop(guard);
    assert!(housekeeping.prune_completed(deployment, u64::MAX).is_err());
    assert_eq!(
        ordinary
            .read(&format!("retiring/{id}"), 65536)
            .unwrap()
            .unwrap(),
        manifest
    );
    assert_eq!(ordinary.prune_completed(deployment, u64::MAX).unwrap(), 1);
    assert!(ordinary.summaries(deployment, None, 10).unwrap().is_empty());
    assert!(
        ordinary
            .read(&format!("retiring/{id}"), 65536)
            .unwrap()
            .is_none()
    );
}
