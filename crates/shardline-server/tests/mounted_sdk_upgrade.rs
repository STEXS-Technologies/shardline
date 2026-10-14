#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use shardline_server::LocalBackend;
use std::num::NonZeroUsize;

/// Run explicitly inside the owned read-only mount fixture, never on a user's root.
#[tokio::test]
#[ignore = "requires an existing fixture with a read-only root and writable metadata/chunks mounts"]
async fn mounted_sdk_upgrade() {
    let root = std::path::PathBuf::from(std::env::var_os("SHARDLINE_MOUNT_FIXTURE").unwrap());
    let error = std::fs::create_dir(root.join("mount-write-probe")).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(30), "fixture must enforce EROFS");
    assert!(!root.join(".gc-write-barrier.lock").exists());
    let backend = LocalBackend::new(
        root.clone(),
        "http://localhost:8080".into(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    // A read-only mount must retain ownership rather than bypass the barrier.
    let gc_owner = std::fs::File::open(&root).unwrap();
    gc_owner.lock().unwrap();
    let upload = backend.upload_file(
        "mounted-upgrade-file",
        Bytes::from_static(b"mounted upload"),
        None,
    );
    tokio::pin!(upload);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut upload)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_dir(root.join("chunks")).unwrap().count(), 0);
    drop(gc_owner);
    tokio::time::timeout(std::time::Duration::from_secs(2), upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend
            .download_file("mounted-upgrade-file", None, None)
            .await
            .unwrap(),
        b"mounted upload"
    );
    assert!(!root.join(".gc-write-barrier.lock").exists());
}
