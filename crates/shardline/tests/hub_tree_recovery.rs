#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{fs, process::Command};

use shardline_index::{
    LocalIndexStore,
    hub::{EMPTY_HUB_REVISION, HubFileEntry, HubRepoType, HubStore},
};
use shardline_protocol::ShardlineHash;
use shardline_storage::{LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

#[test]
fn actual_cli_restores_verified_tree_and_rejects_stale_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let store = LocalIndexStore::new(root.join("hub")).unwrap();
    let legacy = "1234567890abcdef";
    store
        .create_repo(HubRepoType::Model, "alice/model", true)
        .unwrap();
    store
        .create_revision(
            "alice/model",
            Some(EMPTY_HUB_REVISION),
            legacy,
            "main",
            "legacy",
        )
        .unwrap();
    let content = b"verified original bytes";
    let hash = blake3::hash(content);
    let digest = hash.to_hex().to_string();
    let objects = LocalObjectStore::new(root.join("chunks")).unwrap();
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{digest}")).unwrap();
    objects
        .put_if_absent(
            &key,
            ObjectBody::from_slice(content),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*hash.as_bytes()),
                content.len() as u64,
            ),
        )
        .unwrap();
    let file = HubFileEntry {
        path: "original.txt".to_owned(),
        size: content.len() as u64,
        sha: digest,
        is_lfs: false,
    };
    let manifest = shardline::HubTreeRecoveryInput {
        repo_id: "alice/model".to_owned(),
        ref_name: "main".to_owned(),
        expected_head: legacy.to_owned(),
        repository_scope: None,
        files: vec![file.clone()],
    };
    let state_file = root.join("tree.json");
    fs::write(&state_file, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let config_file = root.join("empty.toml");
    fs::write(&config_file, "").unwrap();
    let command = || {
        Command::new(env!("CARGO_BIN_EXE_shardline"))
            .env_clear()
            .current_dir(root)
            .arg("--config")
            .arg(&config_file)
            .args(["repair", "hub-tree", "--root"])
            .arg(root)
            .arg("--state-file")
            .arg(&state_file)
            .output()
            .unwrap()
    };
    let output = command();
    assert!(
        output.status.success(),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let revision = store
        .resolve_revision("alice/model", "main")
        .unwrap()
        .unwrap();
    assert_eq!(revision.len(), 64);
    assert_eq!(store.get_files(&revision).unwrap(), vec![file]);
    assert!(store.get_files(legacy).is_err());
    let stale = command();
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("expected_head"));
    assert_eq!(
        store.resolve_revision("alice/model", "main").unwrap(),
        Some(revision)
    );
}
