use std::{collections::BTreeSet, fs::File, io::Read, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shardline_index::{
    LocalIndexStore, PostgresIndexStore,
    hub::{BoxedHubStore, HubFileEntry, canonical_ref_name},
};
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_protocol_adapters::{scope_namespace, validate_content_hash};
use shardline_server::{ObjectStorageAdapter, ServerConfigError, ServerObjectStore};
use shardline_storage::{ObjectKey, ObjectStore};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;

/// Operator-supplied full tree from a trustworthy backup or original repository.
/// Existing legacy file-entry rows are never used to infer this tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubTreeRecoveryInput {
    /// Exact repository ID, such as `owner/model`.
    pub repo_id: String,
    /// Existing branch or tag to repair.
    pub ref_name: String,
    /// Exact current revision required by the compare-and-swap.
    pub expected_head: String,
    /// Original object namespace. Explicit null selects a permissive global namespace.
    #[serde(deserialize_with = "deserialize_scope")]
    pub repository_scope: Option<RepositoryScope>,
    /// Complete authoritative file tree, including files that were not changed last.
    pub files: Vec<HubFileEntry>,
}

fn deserialize_scope<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<RepositoryScope>, D::Error> {
    Option::<RepositoryScope>::deserialize(deserializer)
}

/// Failure while validating or applying an explicit Hub tree recovery.
#[derive(Debug, Error)]
pub enum HubTreeRepairRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// The manifest could not be read.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The manifest was not valid JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The authoritative manifest failed validation.
    #[error("invalid Hub recovery manifest: {0}")]
    Invalid(String),
    /// Metadata access or ref compare-and-swap failed.
    #[error("Hub recovery metadata failed: {0}")]
    Store(String),
    /// Referenced object bytes could not be verified.
    #[error("Hub recovery object verification failed: {0}")]
    Object(String),
}

/// Restores a Hub ref to a new repository-bound revision after verifying a full tree.
///
/// Legacy rows and history remain quarantined. Run with the deployment's storage
/// configuration; local Hub metadata is under `ROOT/hub`. No object data is created.
/// The manifest is bounded to 64 MiB and blobs are verified in 1 MiB ranges.
/// Once scanning starts, cancellation of the awaiting future does not cancel
/// the blocking recovery task; it retains the GC barrier until it completes.
///
/// # Errors
///
/// Returns an error for an invalid manifest, missing/mismatched objects, or a ref
/// that no longer matches `expected_head`. Validation precedes metadata writes.
pub async fn run_hub_tree_repair(
    root: Option<&Path>,
    state_file: &Path,
) -> Result<String, HubTreeRepairRuntimeError> {
    let config = crate::load_server_config(root, None)?;
    run_hub_tree_repair_with_config(config, state_file.to_path_buf()).await
}

async fn run_hub_tree_repair_with_config(
    config: shardline_server::ServerConfig,
    state_file: std::path::PathBuf,
) -> Result<String, HubTreeRepairRuntimeError> {
    let maintenance_guard = shardline_server::acquire_metadata_write_barrier(&config)
        .await
        .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _maintenance_guard = maintenance_guard;
        recover_from_manifest(&config, &state_file)
    })
    .await
    .map_err(|error| HubTreeRepairRuntimeError::Store(format!("recovery task failed: {error}")))?
}

fn recover_from_manifest(
    config: &shardline_server::ServerConfig,
    state_file: &Path,
) -> Result<String, HubTreeRepairRuntimeError> {
    let mut bytes = Vec::new();
    File::open(state_file)?
        .take(64 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(HubTreeRepairRuntimeError::Invalid(
            "manifest exceeds 64 MiB".to_owned(),
        ));
    }
    let input: HubTreeRecoveryInput = serde_json::from_slice(&bytes)?;
    let store = if let Some(url) = config.index_postgres_url() {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_lazy(url)
            .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
        BoxedHubStore::from_store(PostgresIndexStore::new(pool))
    } else {
        let store = LocalIndexStore::new(config.root_dir().join("hub"))
            .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
        BoxedHubStore::from_store(store)
    };
    let objects = match config.object_storage_adapter() {
        ObjectStorageAdapter::Local => ServerObjectStore::local(config.root_dir().join("chunks")),
        ObjectStorageAdapter::S3 => {
            let s3 = config.s3_object_store_config().ok_or_else(|| {
                HubTreeRepairRuntimeError::Invalid("missing S3 configuration".to_owned())
            })?;
            ServerObjectStore::s3(s3.clone())
        }
    }
    .map_err(|error| HubTreeRepairRuntimeError::Object(error.to_string()))?;
    repair_tree(&store, &objects, input)
}

fn repair_tree(
    store: &BoxedHubStore,
    objects: &ServerObjectStore,
    input: HubTreeRecoveryInput,
) -> Result<String, HubTreeRepairRuntimeError> {
    validate_manifest(&input)?;
    let ref_name = canonical_ref_name(&input.ref_name);
    let refs = store
        .list_refs(&input.repo_id)
        .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
    if !refs
        .iter()
        .any(|reference| reference.ref_name == ref_name && reference.sha == input.expected_head)
    {
        return Err(HubTreeRepairRuntimeError::Invalid(
            "selected ref does not match expected_head".to_owned(),
        ));
    }
    for file in &input.files {
        verify_file(objects, input.repository_scope.as_ref(), file)?;
    }
    publish_verified_tree(store, input)
}

fn publish_verified_tree(
    store: &BoxedHubStore,
    mut input: HubTreeRecoveryInput,
) -> Result<String, HubTreeRepairRuntimeError> {
    let ref_name = canonical_ref_name(&input.ref_name);
    input
        .files
        .sort_unstable_by(|left, right| left.path.cmp(&right.path));
    let tree: Vec<_> = input
        .files
        .iter()
        .map(|file| (&file.path, file.size, &file.sha, file.is_lfs))
        .collect();
    let identity = serde_json::to_vec(&(
        "shardline-hub-authoritative-tree-recovery-v1",
        &input.repo_id,
        ref_name,
        &input.expected_head,
        &input.repository_scope,
        tree,
    ))?;
    let revision = blake3::hash(&identity).to_hex().to_string();
    store
        .store_files(&revision, &input.files)
        .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
    store
        .create_revision(
            &input.repo_id,
            Some(&input.expected_head),
            &revision,
            ref_name,
            "Restore authoritative Hub tree",
        )
        .map_err(|error| HubTreeRepairRuntimeError::Store(error.to_string()))?;
    Ok(revision)
}

fn validate_manifest(input: &HubTreeRecoveryInput) -> Result<(), HubTreeRepairRuntimeError> {
    let invalid = |message: &str| HubTreeRepairRuntimeError::Invalid(message.to_owned());
    let Some((owner, repo)) = input.repo_id.split_once('/') else {
        return Err(invalid("repo_id must be owner/repository"));
    };
    if owner.is_empty() || repo.is_empty() || repo.contains('/') {
        return Err(invalid("repo_id must be owner/repository"));
    }
    if input.ref_name.is_empty()
        || input
            .ref_name
            .bytes()
            .any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(invalid("invalid ref_name"));
    }
    if input.expected_head.len() != 16
        || !input
            .expected_head
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid(
            "expected_head must identify a quarantined 16-hex legacy revision",
        ));
    }
    if let Some(scope) = &input.repository_scope {
        RepositoryScope::new(
            scope.provider(),
            scope.owner(),
            scope.name(),
            scope.revision(),
        )
        .map_err(|_error| invalid("invalid repository_scope"))?;
        if scope.owner() != owner || scope.name() != repo {
            return Err(invalid("repository_scope must belong to repo_id"));
        }
    }
    if input.files.len() > 100_000 {
        return Err(invalid(
            "tree exceeds the 100000-entry metadata read ceiling",
        ));
    }
    let mut paths = BTreeSet::new();
    for file in &input.files {
        if file.path.is_empty()
            || file.path.len() > 4096
            || file.path.starts_with('/')
            || file.path.contains('\\')
            || file.path.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
            || file
                .path
                .split('/')
                .any(|component| component.is_empty() || matches!(component, "." | ".." | ".git"))
        {
            return Err(invalid("unsafe file path"));
        }
        if !paths.insert(file.path.as_str()) {
            return Err(invalid("duplicate file path"));
        }
        validate_content_hash(&file.sha)
            .map_err(|_error| invalid("file hash must be 64 lowercase hex characters"))?;
        if file.size > i64::MAX as u64 {
            return Err(invalid("file size exceeds metadata limit"));
        }
    }
    for path in &paths {
        let mut prefix = *path;
        while let Some((parent, _)) = prefix.rsplit_once('/') {
            if paths.contains(parent) {
                return Err(invalid("file path is also a directory"));
            }
            prefix = parent;
        }
    }
    Ok(())
}

fn verify_file(
    objects: &ServerObjectStore,
    scope: Option<&RepositoryScope>,
    file: &HubFileEntry,
) -> Result<(), HubTreeRepairRuntimeError> {
    let failure =
        |message: String| HubTreeRepairRuntimeError::Object(format!("{}: {message}", file.path));
    let key = ObjectKey::parse(&format!(
        "protocols/lfs/{}/objects/{}",
        scope_namespace(scope),
        file.sha
    ))
    .map_err(|error| failure(error.to_string()))?;
    let metadata = objects
        .metadata(&key)
        .map_err(|error| failure(error.to_string()))?
        .ok_or_else(|| failure("object is missing from selected namespace".to_owned()))?;
    if metadata.length() != file.size {
        return Err(failure("object length disagrees with manifest".to_owned()));
    }
    let mut blake3 = blake3::Hasher::new();
    let mut sha256 = Sha256::new();
    let mut offset = 0;
    while offset < file.size {
        let end = offset.saturating_add(1024 * 1024).min(file.size);
        let end_inclusive = end
            .checked_sub(1)
            .ok_or_else(|| failure("invalid object range".to_owned()))?;
        let expected_length = end
            .checked_sub(offset)
            .ok_or_else(|| failure("invalid object range".to_owned()))?;
        let range =
            ByteRange::new(offset, end_inclusive).map_err(|error| failure(error.to_string()))?;
        let bytes = objects
            .read_range(&key, range)
            .map_err(|error| failure(error.to_string()))?;
        if bytes.len() as u64 != expected_length {
            return Err(failure(
                "object range length disagrees with manifest".to_owned(),
            ));
        }
        if file.is_lfs {
            sha256.update(&bytes);
        } else {
            blake3.update(&bytes);
        }
        offset = end;
    }
    let actual = if file.is_lfs {
        hex::encode(sha256.finalize())
    } else {
        blake3.finalize().to_hex().to_string()
    };
    if actual != file.sha {
        return Err(failure("object digest disagrees with manifest".to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::hub::{EMPTY_HUB_REVISION, HubRepoType, HubStore};
    use shardline_protocol::{RepositoryProvider, ShardlineHash};
    use shardline_storage::{ObjectBody, ObjectIntegrity};
    use tempfile::TempDir;

    const LEGACY: &str = "1234567890abcdef";

    fn fixture() -> (TempDir, LocalIndexStore, ServerObjectStore) {
        let temp = TempDir::new().unwrap();
        let store = LocalIndexStore::new(temp.path().join("hub")).unwrap();
        let objects = ServerObjectStore::local(temp.path().join("chunks")).unwrap();
        (temp, store, objects)
    }

    fn legacy_repo(store: &LocalIndexStore, repo: &str, ref_name: &str) {
        store.create_repo(HubRepoType::Model, repo, true).unwrap();
        store
            .create_revision(
                repo,
                if ref_name == "main" {
                    Some(EMPTY_HUB_REVISION)
                } else {
                    None
                },
                LEGACY,
                ref_name,
                "old",
            )
            .unwrap();
    }

    fn file(
        objects: &ServerObjectStore,
        scope: Option<&RepositoryScope>,
        path: &str,
        bytes: &[u8],
        is_lfs: bool,
    ) -> HubFileEntry {
        let sha = if is_lfs {
            hex::encode(Sha256::digest(bytes))
        } else {
            blake3::hash(bytes).to_hex().to_string()
        };
        let key = ObjectKey::parse(&format!(
            "protocols/lfs/{}/objects/{sha}",
            scope_namespace(scope)
        ))
        .unwrap();
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
            bytes.len() as u64,
        );
        objects
            .put_if_absent(&key, ObjectBody::from_slice(bytes), &integrity)
            .unwrap();
        HubFileEntry {
            path: path.to_owned(),
            size: bytes.len() as u64,
            sha,
            is_lfs,
        }
    }

    fn input(repo: &str, files: Vec<HubFileEntry>) -> HubTreeRecoveryInput {
        HubTreeRecoveryInput {
            repo_id: repo.to_owned(),
            ref_name: "main".to_owned(),
            expected_head: LEGACY.to_owned(),
            repository_scope: None,
            files,
        }
    }

    fn seed_corrupt_tree(root: &Path, paths: &[&str]) {
        let conn = rusqlite::Connection::open(root.join("hub/metadata.sqlite3")).unwrap();
        for path in paths {
            conn.execute("INSERT INTO shardline_hub_file_entries(commit_sha,path,size,sha,is_lfs) VALUES (?1,?2,1,?3,0)", rusqlite::params![LEGACY, path, "f".repeat(64)]).unwrap();
        }
    }

    #[test]
    fn recovery_isolates_collided_repositories_from_authoritative_blobs() {
        let (temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "main");
        legacy_repo(&store, "bob/model", "main");
        seed_corrupt_tree(temp.path(), &["alice-private", "bob-private"]);
        let boxed = BoxedHubStore::from_store(store);
        assert!(
            boxed
                .get_files(LEGACY)
                .unwrap_err()
                .to_string()
                .contains("requires recovery")
        );
        let alice_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", None).unwrap();
        let bob_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", None).unwrap();
        let alice_file = file(
            &objects,
            Some(&alice_scope),
            "alice-private",
            b"same",
            false,
        );
        let bob_file = file(&objects, Some(&bob_scope), "bob-private", b"same", false);
        let mut alice = input("alice/model", vec![alice_file.clone()]);
        alice.repository_scope = Some(alice_scope);
        let mut bob = input("bob/model", vec![bob_file.clone()]);
        bob.repository_scope = Some(bob_scope);
        let alice_revision = repair_tree(&boxed, &objects, alice).unwrap();
        let bob_revision = repair_tree(&boxed, &objects, bob).unwrap();
        assert_ne!(alice_revision, bob_revision);
        assert_eq!(boxed.get_files(&alice_revision).unwrap(), vec![alice_file]);
        assert_eq!(boxed.get_files(&bob_revision).unwrap(), vec![bob_file]);
        assert_eq!(
            boxed.resolve_revision("alice/model", "main").unwrap(),
            Some(alice_revision)
        );
        assert_eq!(
            boxed.resolve_revision("bob/model", "main").unwrap(),
            Some(bob_revision)
        );
        assert!(boxed.get_files(LEGACY).is_err());
        let conn = rusqlite::Connection::open(temp.path().join("hub/metadata.sqlite3")).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM shardline_hub_file_entries WHERE commit_sha=?1",
                [LEGACY],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "raw damaged rows preserved for forensic backup");
    }

    #[test]
    fn recovery_replaces_same_repo_stale_damage_and_preserves_other_refs() {
        let (temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "feature");
        seed_corrupt_tree(temp.path(), &["deleted.txt", "kept.txt"]);
        let boxed = BoxedHubStore::from_store(store);
        let expected = file(&objects, None, "kept.txt", b"correct", false);
        let mut manifest = input("alice/model", vec![expected.clone()]);
        manifest.ref_name = "feature".to_owned();
        let revision = repair_tree(&boxed, &objects, manifest).unwrap();
        assert_eq!(boxed.get_files(&revision).unwrap(), vec![expected]);
        assert_eq!(
            boxed
                .resolve_revision("alice/model", "main")
                .unwrap()
                .as_deref(),
            Some(EMPTY_HUB_REVISION)
        );
        assert_eq!(
            boxed.resolve_revision("alice/model", "feature").unwrap(),
            Some(revision)
        );
    }

    #[test]
    fn invalid_manifests_and_blob_content_do_not_advance_refs() {
        let (_temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "main");
        let boxed = BoxedHubStore::from_store(store);
        let valid = file(&objects, None, "kept.txt", b"correct", false);
        let mut cases = vec![];
        cases.push(input("alice/model", vec![valid.clone(), valid.clone()]));
        let mut unsafe_path = valid.clone();
        unsafe_path.path = "../secret".to_owned();
        cases.push(input("alice/model", vec![unsafe_path]));
        let mut incorrect_size = valid.clone();
        incorrect_size.size += 1;
        cases.push(input("alice/model", vec![incorrect_size]));
        let mut nonexistent = valid.clone();
        nonexistent.sha = "e".repeat(64);
        cases.push(input("alice/model", vec![nonexistent]));
        let mut wrong_digest = valid.clone();
        wrong_digest.is_lfs = true;
        cases.push(input("alice/model", vec![wrong_digest]));
        let mut malformed_oid = valid.clone();
        malformed_oid.sha = "bad".to_owned();
        malformed_oid.is_lfs = true;
        cases.push(input("alice/model", vec![malformed_oid]));
        let mut stale = input("alice/model", vec![valid.clone()]);
        stale.expected_head = "f".repeat(16);
        cases.push(stale);
        let mut wrong_scope = input("alice/model", vec![valid]);
        wrong_scope.repository_scope =
            Some(RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", None).unwrap());
        cases.push(wrong_scope);
        let original_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", None).unwrap();
        let scoped_file = file(
            &objects,
            Some(&original_scope),
            "scoped.txt",
            b"private scoped bytes",
            false,
        );
        let mut wrong_provider = input("alice/model", vec![scoped_file]);
        wrong_provider.repository_scope =
            Some(RepositoryScope::new(RepositoryProvider::GitLab, "alice", "model", None).unwrap());
        cases.push(wrong_provider);
        for manifest in cases {
            assert!(repair_tree(&boxed, &objects, manifest).is_err());
            assert_eq!(
                boxed
                    .resolve_revision("alice/model", "main")
                    .unwrap()
                    .as_deref(),
                Some(LEGACY)
            );
        }
    }

    #[test]
    fn recovery_checks_lfs_sha256_and_empty_inline_digest() {
        let (_temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "main");
        let boxed = BoxedHubStore::from_store(store);
        let files = vec![
            file(&objects, None, "large.bin", b"LFS bytes", true),
            file(&objects, None, "empty.txt", b"", false),
        ];
        let revision = repair_tree(&boxed, &objects, input("alice/model", files.clone())).unwrap();
        assert_eq!(
            boxed.get_files(&revision).unwrap(),
            vec![files[1].clone(), files[0].clone()]
        );
    }

    #[test]
    fn initial_revision_never_exposes_corrupt_global_entries() {
        let (temp, store, _objects) = fixture();
        let conn = rusqlite::Connection::open(temp.path().join("hub/metadata.sqlite3")).unwrap();
        conn.execute("INSERT INTO shardline_hub_file_entries(commit_sha,path,size,sha,is_lfs) VALUES (?1,'secret',1,?2,0)", rusqlite::params![EMPTY_HUB_REVISION, "f".repeat(64)]).unwrap();
        assert!(store.get_files(EMPTY_HUB_REVISION).unwrap().is_empty());
        assert!(
            store
                .store_files(
                    EMPTY_HUB_REVISION,
                    &[HubFileEntry {
                        path: "secret".to_owned(),
                        size: 1,
                        sha: "f".repeat(64),
                        is_lfs: false
                    }]
                )
                .is_err()
        );
        assert!(store.store_files(LEGACY, &[]).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_recovery_isolates_legacy_collision_and_preserves_evidence() {
        let Ok(url) = std::env::var("SHARDLINE_HUB_RECOVERY_TEST_DATABASE_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        let store = PostgresIndexStore::new(pool.clone());
        let temp = TempDir::new().unwrap();
        let objects = ServerObjectStore::local(temp.path().join("chunks")).unwrap();
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let alice_repo = format!("recovery-alice/{suffix}");
        let bob_repo = format!("recovery-bob/{suffix}");
        let mut legacy = blake3::hash(suffix.to_string().as_bytes())
            .to_hex()
            .to_string();
        legacy.truncate(16);
        for repo in [&alice_repo, &bob_repo] {
            store.create_repo(HubRepoType::Model, repo, true).unwrap();
            store
                .create_revision(repo, Some(EMPTY_HUB_REVISION), &legacy, "main", "collided")
                .unwrap();
        }
        for path in ["alice-private", "bob-private"] {
            sqlx::query("INSERT INTO shardline_hub_file_entries(commit_sha,path,size,sha,is_lfs) VALUES ($1,$2,1,$3,false)")
                .bind(&legacy).bind(path).bind("f".repeat(64)).execute(&pool).await.unwrap();
        }
        let boxed = BoxedHubStore::from_store(store.clone());
        check_revision_reuse(&boxed, &format!("recovery-reuse/{suffix}"));
        assert!(
            boxed
                .get_files(&legacy)
                .unwrap_err()
                .to_string()
                .contains("requires recovery")
        );
        assert!(boxed.store_files(&legacy, &[]).is_err());
        let alice_file = file(&objects, None, "alice-private", b"same", false);
        let bob_file = file(&objects, None, "bob-private", b"same", false);
        let mut alice = input(&alice_repo, vec![alice_file.clone()]);
        alice.expected_head = legacy.clone();
        let mut bob = input(&bob_repo, vec![bob_file.clone()]);
        bob.expected_head = legacy.clone();
        let state_file = temp.path().join("tree.json");
        std::fs::write(&state_file, serde_json::to_vec(&alice).unwrap()).unwrap();
        let config = shardline_server::ServerConfig::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 8080)),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        )
        .with_index_postgres_url(url)
        .unwrap();
        let alice_revision = run_hub_tree_repair_with_config(config, state_file)
            .await
            .unwrap();
        let bob_revision = repair_tree(&boxed, &objects, bob).unwrap();
        assert_ne!(alice_revision, bob_revision);
        assert_eq!(boxed.get_files(&alice_revision).unwrap(), vec![alice_file]);
        assert_eq!(boxed.get_files(&bob_revision).unwrap(), vec![bob_file]);
        assert_eq!(
            boxed.resolve_revision(&alice_repo, "main").unwrap(),
            Some(alice_revision)
        );
        assert_eq!(
            boxed.resolve_revision(&bob_repo, "main").unwrap(),
            Some(bob_revision)
        );
        assert!(boxed.get_files(EMPTY_HUB_REVISION).unwrap().is_empty());
        boxed.delete_repo(&alice_repo).unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shardline_hub_file_entries WHERE commit_sha=$1",
        )
        .bind(&legacy)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);
        assert!(boxed.get_files(&legacy).is_err());
        boxed.delete_repo(&bob_repo).unwrap();
        sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha=$1")
            .bind(&legacy)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }

    #[test]
    fn ref_update_after_validation_is_not_overwritten_by_recovery() {
        let (_temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "main");
        let boxed = BoxedHubStore::from_store(store);
        let valid = file(&objects, None, "correct", b"correct", false);
        let manifest = input("alice/model", vec![valid.clone()]);
        validate_manifest(&manifest).unwrap();
        verify_file(&objects, None, &valid).unwrap();
        let moved = "e".repeat(64);
        boxed.store_files(&moved, &[]).unwrap();
        boxed
            .create_revision(
                "alice/model",
                Some(LEGACY),
                &moved,
                "main",
                "concurrent update",
            )
            .unwrap();
        assert!(publish_verified_tree(&boxed, manifest).is_err());
        assert_eq!(
            boxed.resolve_revision("alice/model", "main").unwrap(),
            Some(moved)
        );
    }

    #[test]
    fn recovery_manifest_requires_explicit_object_namespace() {
        let manifest = serde_json::json!({ "repo_id":"alice/model", "ref_name":"main", "expected_head":LEGACY, "files":[] });
        assert!(serde_json::from_value::<HubTreeRecoveryInput>(manifest.clone()).is_err());
        let mut explicit = manifest;
        explicit["repository_scope"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<HubTreeRecoveryInput>(explicit).is_ok());
    }

    fn check_revision_reuse(store: &BoxedHubStore, repo: &str) {
        store.create_repo(HubRepoType::Model, repo, false).unwrap();
        let original_sha = "a".repeat(40);
        let moved_sha = "b".repeat(40);
        let original = store
            .create_revision(
                repo,
                Some(EMPTY_HUB_REVISION),
                &original_sha,
                "main",
                "original immutable message",
            )
            .unwrap();
        let files = vec![
            HubFileEntry {
                path: "a.txt".to_owned(),
                size: 1,
                sha: "a".repeat(64),
                is_lfs: false,
            },
            HubFileEntry {
                path: "b.txt".to_owned(),
                size: 1,
                sha: "b".repeat(64),
                is_lfs: false,
            },
        ];
        store.store_files(&original_sha, &files).unwrap();
        assert_eq!(store.get_files(&original_sha).unwrap(), files);
        assert!(store.get_files_bounded(&original_sha, 1).is_err());
        assert_eq!(store.get_files_bounded(&original_sha, 2).unwrap().len(), 2);
        assert!(
            store
                .get_files_bounded(EMPTY_HUB_REVISION, 0)
                .unwrap()
                .is_empty()
        );
        for target in ["feature", "refs/tags/v1"] {
            let reused = store
                .create_revision(
                    repo,
                    None,
                    &original_sha,
                    target,
                    "must not replace immutable message",
                )
                .unwrap();
            assert_eq!(reused.parent_sha, original.parent_sha);
            assert_eq!(reused.message, original.message);
            assert_eq!(
                reused.created_at_unix_seconds,
                original.created_at_unix_seconds
            );
            assert_eq!(reused.ref_name, original.ref_name);
            assert_eq!(
                store.resolve_revision(repo, target).unwrap(),
                Some(original_sha.clone())
            );
        }
        assert_eq!(store.list_revisions(repo).unwrap().len(), 2);
        assert!(store.list_revisions_bounded(repo, 1).is_err());
        assert_eq!(store.list_revisions_bounded(repo, 2).unwrap().len(), 2);
        assert!(store.list_refs_bounded(repo, 2).is_err());
        assert_eq!(store.list_refs_bounded(repo, 3).unwrap().len(), 3);
        store
            .create_revision(
                repo,
                Some(&original_sha),
                &moved_sha,
                "feature",
                "advance feature",
            )
            .unwrap();
        assert!(
            store
                .create_revision(
                    repo,
                    Some(&original_sha),
                    &original_sha,
                    "feature",
                    "stale update"
                )
                .is_err()
        );
        assert_eq!(
            store.resolve_revision(repo, "feature").unwrap(),
            Some(moved_sha)
        );
        assert_eq!(
            store.resolve_revision(repo, "main").unwrap(),
            Some(original_sha.clone())
        );
        assert_eq!(
            store.resolve_revision(repo, "refs/tags/v1").unwrap(),
            Some(original_sha)
        );
        store.delete_repo(repo).unwrap();
    }

    #[test]
    fn sqlite_reuses_immutable_revision_for_branch_and_lightweight_tag() {
        let (_temp, store, _objects) = fixture();
        check_revision_reuse(&BoxedHubStore::from_store(store), "reuse/model");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_recovery_keeps_current_thread_runtime_responsive() {
        let (temp, store, objects) = fixture();
        legacy_repo(&store, "alice/model", "main");
        let files = vec![file(&objects, None, "original", b"verified", false)];
        let manifest = input("alice/model", files.clone());
        let state_file = temp.path().join("tree.json");
        std::fs::write(&state_file, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let config = shardline_server::ServerConfig::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 8080)),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let revision = run_hub_tree_repair_with_config(config, state_file)
            .await
            .unwrap();
        assert_eq!(store.get_files(&revision).unwrap(), files);
        assert_eq!(
            store.resolve_revision("alice/model", "main").unwrap(),
            Some(revision)
        );
    }
}
