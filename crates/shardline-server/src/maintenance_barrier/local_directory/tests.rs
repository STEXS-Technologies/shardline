#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::maintenance_barrier::{
    acquire_local_exclusive, acquire_local_shared, prepare_local_barrier,
};
use std::{os::unix::fs::PermissionsExt, time::Duration};

struct RestorePermissions(std::path::PathBuf);
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[tokio::test]
async fn restricted_owner_still_blocks_gc_after_legacy_lock_file_becomes_available() {
    let root = tempfile::tempdir().unwrap();
    let _restore = RestorePermissions(root.path().to_path_buf());
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    assert!(std::fs::write(root.path().join("permission-probe"), b"denied").is_err());
    let restricted = acquire_local_shared(root.path()).await.unwrap();
    assert!(
        !root
            .path()
            .join(super::super::LOCAL_BARRIER_FILE_NAME)
            .exists()
    );
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    prepare_local_barrier(root.path()).await.unwrap();
    let ordinary = acquire_local_shared(root.path()).await.unwrap();
    drop(ordinary);
    let gc_root = root.path().to_path_buf();
    let mut gc = tokio::spawn(async move { acquire_local_exclusive(&gc_root).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut gc)
            .await
            .is_err()
    );
    drop(restricted);
    let guard = tokio::time::timeout(Duration::from_secs(2), gc)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(guard);
}

#[tokio::test]
async fn cancelled_restricted_directory_waiter_does_not_retain_ownership() {
    let root = tempfile::tempdir().unwrap();
    let _restore = RestorePermissions(root.path().to_path_buf());
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let owner = acquire_local_shared(root.path()).await.unwrap();
    let gc_root = root.path().to_path_buf();
    let mut waiter = tokio::spawn(async move { acquire_local_exclusive(&gc_root).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut waiter)
            .await
            .is_err()
    );
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    drop(owner);
    let next = tokio::time::timeout(Duration::from_secs(2), acquire_local_exclusive(root.path()))
        .await
        .unwrap()
        .unwrap();
    drop(next);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn read_only_legacy_file_retains_exclusive_gc_ownership() {
    let root = tempfile::tempdir().unwrap();
    prepare_local_barrier(root.path()).await.unwrap();
    let path = root.path().join(super::super::LOCAL_BARRIER_FILE_NAME);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    assert!(File::options().write(true).open(&path).is_err());
    let gc = acquire_local_exclusive(root.path()).await.unwrap();
    let legacy = File::open(&path).unwrap();
    assert!(matches!(
        legacy.try_lock_shared(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(gc);
    legacy.try_lock_shared().unwrap();
    legacy.unlock().unwrap();
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o444
    );
}

#[tokio::test]
async fn existing_legacy_file_owner_is_not_bypassed_by_directory_ownership() {
    let root = tempfile::tempdir().unwrap();
    prepare_local_barrier(root.path()).await.unwrap();
    let legacy = File::options()
        .read(true)
        .write(true)
        .open(root.path().join(super::super::LOCAL_BARRIER_FILE_NAME))
        .unwrap();
    legacy.lock().unwrap();
    let upload_root = root.path().to_path_buf();
    let mut upload = tokio::spawn(async move { acquire_local_shared(&upload_root).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut upload)
            .await
            .is_err()
    );
    drop(legacy);
    let guard = tokio::time::timeout(Duration::from_secs(2), upload)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(guard);
}

#[tokio::test]
async fn directory_barrier_rejects_a_symlinked_root_without_outside_changes() {
    let fixture = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = fixture.path().join("linked-root");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    assert!(acquire_local_shared(&link).await.is_err());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}
