// Assertions describe invariants; Result propagates fixture setup failures.
#![allow(clippy::panic_in_result_fn)]

use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, HubObjectKind, HubStore},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

#[test]
fn native_alias_migration_backfills_and_can_readopt_without_losing_old_bindings()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch(include_str!(
        "../migrations/20261021000000_xet_file_identities.up.sql"
    ))?;
    connection.execute(
        "INSERT INTO shardline_xet_file_identities VALUES ('scope','sha','old',42)",
        [],
    )?;
    let migration = include_str!("../migrations/20261023040000_xet_file_aliases.up.sql");
    connection.execute_batch(migration)?;
    connection.execute("UPDATE shardline_xet_file_identities SET file_id='new'", [])?;
    connection.execute_batch(migration)?;
    let mut query =
        connection.prepare("SELECT file_id FROM shardline_xet_file_aliases ORDER BY file_id")?;
    let ids = query
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(ids, vec!["new", "old"]);
    assert!(
        connection
            .execute(
                "UPDATE shardline_xet_file_identities SET sha256='conflict'",
                []
            )
            .is_err()
    );
    let sha: String = connection.query_row(
        "SELECT sha256 FROM shardline_xet_file_identities",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(sha, "sha");
    Ok(())
}

#[tokio::test]
async fn native_candidate_requires_exact_repository_and_authoritative_commit_membership()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use shardline_index::{
        FileRecord, LocalRecordStore, StorageRepresentation, XetFileIdentity,
        hub::{HubFileEntry, HubRepoType},
    };
    let root = tempfile::tempdir()?;
    let original =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", Some("main"))?;
    let requested = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "alice",
        "model",
        Some("immutable-commit"),
    )?;
    let other = RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", None)?;
    let records = LocalRecordStore::new(root.path().to_owned())?;
    let record = FileRecord {
        file_id: "b".repeat(64),
        content_hash: "b".repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(original.clone()),
        chunks: Vec::new(),
    };
    let identity = XetFileIdentity {
        repository_scope: original,
        sha256: "a".repeat(64),
        file_id: record.file_id.clone(),
        total_bytes: 0,
    };
    records
        .commit_native_shard_metadata_with_identities(
            std::slice::from_ref(&record),
            &[],
            std::slice::from_ref(&identity),
        )
        .await?;
    let found = records.repository_xet_file(&requested, &record.file_id)?;
    assert_eq!(found, Some((identity.sha256.clone(), record.clone())));
    assert!(
        records
            .repository_xet_file(&other, &record.file_id)?
            .is_none()
    );
    // Equivalent bytes can have a different native identity (for example,
    // after a client changes chunking). Cached old identities must survive.
    let mut replacement = record.clone();
    replacement.file_id = "c".repeat(64);
    let mut replacement_identity = identity.clone();
    replacement_identity
        .file_id
        .clone_from(&replacement.file_id);
    records
        .commit_native_shard_metadata_with_identities(
            std::slice::from_ref(&replacement),
            &[],
            std::slice::from_ref(&replacement_identity),
        )
        .await?;
    drop(records);
    let reopened = LocalRecordStore::new(root.path().to_owned())?;
    for expected in [&record, &replacement] {
        assert_eq!(
            reopened.repository_xet_file(&requested, &expected.file_id)?,
            Some((identity.sha256.clone(), expected.clone()))
        );
        assert!(
            reopened
                .repository_xet_file(&other, &expected.file_id)?
                .is_none()
        );
    }
    // A native identity cannot subsequently claim a different Hub digest.
    let mut conflicting = identity.clone();
    conflicting.sha256 = "d".repeat(64);
    assert!(
        reopened
            .commit_native_shard_metadata_with_identities(&[], &[], &[conflicting])
            .await
            .is_err()
    );
    assert!(
        reopened
            .xet_file_identity(&identity.repository_scope, &"d".repeat(64))?
            .is_none()
    );
    assert_eq!(
        reopened.repository_xet_file(&requested, &record.file_id)?,
        Some((identity.sha256.clone(), record.clone()))
    );
    let hub = LocalIndexStore::new(root.path().join("hub"))?;
    let repo = HubStore::create_repo(&hub, HubRepoType::Model, "alice/model", true)?;
    HubStore::create_revision(
        &hub,
        "alice/model",
        Some(&repo.default_branch),
        "immutable-commit",
        "main",
        "native pointer",
    )?;
    HubStore::store_files(
        &hub,
        "immutable-commit",
        &[HubFileEntry {
            path: "file.bin".to_owned(),
            sha: identity.sha256.clone(),
            size: 0,
            is_lfs: true,
        }],
    )?;
    assert!(hub.hub_commit_contains_lfs("alice/model", "immutable-commit", &identity.sha256, 0)?);
    assert!(!hub.hub_commit_contains_lfs("bob/model", "immutable-commit", &identity.sha256, 0)?);
    assert!(!hub.hub_commit_contains_lfs(
        "alice/model",
        &repo.default_branch,
        &identity.sha256,
        0
    )?);
    assert!(!hub.hub_commit_contains_lfs(
        "alice/model",
        "immutable-commit",
        &identity.sha256,
        1
    )?);
    Ok(())
}

#[test]
fn location_lookup_survives_reopen_and_preserves_repository_and_kind_boundaries()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = tempfile::tempdir()?;
    let original =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", Some("main"))?;
    let broad = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", None)?;
    let other_repo = RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", None)?;
    let other_provider = RepositoryScope::new(RepositoryProvider::Generic, "alice", "model", None)?;
    let original_store = LocalIndexStore::new(root.path().to_owned())?;
    HubStore::remember_object_location(&original_store, &original, HubObjectKind::File, "object")?;
    drop(original_store);
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_owned())?);
    assert_eq!(
        store
            .object_location(&broad, HubObjectKind::File, "object")?
            .and_then(|location| location.storage_revision),
        Some("main".to_owned())
    );
    assert!(
        store
            .object_location(&other_repo, HubObjectKind::File, "object")?
            .is_none()
    );
    assert!(
        store
            .object_location(&other_provider, HubObjectKind::File, "object")?
            .is_none()
    );
    assert!(
        store
            .object_location(&broad, HubObjectKind::GitArchive, "object")?
            .is_none()
    );
    store.remember_object_location(&broad, HubObjectKind::File, "object")?;
    let location = store.object_location(&original, HubObjectKind::File, "object")?;
    assert!(location.is_some_and(|location| location.storage_revision.is_none()));
    Ok(())
}
