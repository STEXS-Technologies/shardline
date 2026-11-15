use std::collections::HashSet;

use serde_json::to_vec;
use shardline_index::{FileRecord, RecordStore, RecordTraversal, RepositoryRecordScope};
use shardline_protocol::RepositoryScope;
use shardline_vcs::RepositoryRef;

use crate::ProviderEventsError;
use shardline_server_core::{ServerObjectStore, chunk_object_key, parse_stored_file_record_bytes};
use shardline_xet_adapter::{visit_stored_xorb_chunk_hashes_if_present, xorb_object_key};

pub(super) async fn ensure_absent_or_matching_record<RecordAdapter>(
    record_store: &RecordAdapter,
    locator: &<RecordAdapter as RecordTraversal>::Locator,
    record: &FileRecord,
) -> Result<(), ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
{
    if !RecordTraversal::record_locator_exists(record_store, locator)
        .await
        .map_err(Into::into)?
    {
        return Ok(());
    }

    let expected_bytes = to_vec(record)?;
    let existing_bytes = RecordTraversal::read_record_bytes(record_store, locator)
        .await
        .map_err(Into::into)?;
    if existing_bytes == expected_bytes {
        return Ok(());
    }

    Err(ProviderEventsError::ConflictingRenameTargetRecord)
}

pub(super) async fn prepare_renamed_records<RecordAdapter>(
    record_store: &RecordAdapter,
    version_records: &[FileRecord],
    new_repository: &RepositoryRef,
) -> Result<Vec<FileRecord>, ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
{
    let renamed_records = version_records
        .iter()
        .map(|record| renamed_file_record(record, new_repository))
        .collect::<Result<Vec<_>, _>>()?;
    for record in &renamed_records {
        for locator in [
            RecordTraversal::version_record_locator(record_store, record),
            RecordTraversal::latest_record_locator(record_store, record),
        ] {
            ensure_absent_or_matching_record(record_store, &locator, record).await?;
        }
    }
    Ok(renamed_records)
}

pub(super) struct DeletedRepositoryReferences {
    pub file_versions: u64,
    pub chunk_hashes: HashSet<String>,
    pub held_object_keys: HashSet<String>,
}

pub(super) fn collect_deleted_repository_references<'record>(
    object_store: &ServerObjectStore,
    records: impl Iterator<Item = &'record FileRecord>,
) -> Result<DeletedRepositoryReferences, ProviderEventsError> {
    let mut file_versions = 0_u64;
    let mut chunk_hashes = HashSet::new();
    let mut held_object_keys = HashSet::new();
    let mut seen_record_identities = HashSet::new();
    let mut seen_native_xorbs = HashSet::new();
    for record in records {
        if !seen_record_identities.insert(record_identity_key(record)) {
            continue;
        }
        file_versions = file_versions
            .checked_add(1)
            .ok_or(ProviderEventsError::Overflow)?;
        for chunk in &record.chunks {
            if record.chunk_size == 0 {
                if seen_native_xorbs.contains(chunk.hash.as_str()) {
                    continue;
                }
                let xorb_object_key = xorb_object_key(&chunk.hash)?;
                held_object_keys.insert(xorb_object_key.as_str().to_owned());
                if visit_stored_xorb_chunk_hashes_if_present(
                    object_store,
                    &xorb_object_key,
                    |chunk_hash_hex| {
                        let chunk_object_key = chunk_object_key(&chunk_hash_hex)?;
                        held_object_keys.insert(chunk_object_key.as_str().to_owned());
                        chunk_hashes.insert(chunk_hash_hex);
                        Ok(())
                    },
                )? {
                    // Reuse only successful verification within this scan; a
                    // missing source may appear before a later reference.
                    seen_native_xorbs.insert(chunk.hash.as_str());
                }
                continue;
            }

            chunk_hashes.insert(chunk.hash.clone());
            let chunk_object_key = chunk_object_key(&chunk.hash)?;
            held_object_keys.insert(chunk_object_key.as_str().to_owned());
            let xorb_object_key = xorb_object_key(&chunk.hash)?;
            held_object_keys.insert(xorb_object_key.as_str().to_owned());
        }
    }
    Ok(DeletedRepositoryReferences {
        file_versions,
        chunk_hashes,
        held_object_keys,
    })
}

pub(super) fn parse_record_entry(bytes: &[u8]) -> Result<FileRecord, ProviderEventsError> {
    Ok(parse_stored_file_record_bytes(bytes)?)
}

pub(super) fn record_belongs_to_repository(
    record: &FileRecord,
    repository: &RepositoryRef,
) -> bool {
    let Some(scope) = record.repository_scope.as_ref() else {
        return false;
    };

    scope.provider() == repository.provider().repository_provider()
        && scope.owner() == repository.owner()
        && scope.name() == repository.name()
}

pub(super) fn repository_record_scope(repository: &RepositoryRef) -> RepositoryRecordScope {
    RepositoryRecordScope::new(
        repository.provider().repository_provider(),
        repository.owner(),
        repository.name(),
    )
}

pub(super) fn renamed_file_record(
    record: &FileRecord,
    new_repository: &RepositoryRef,
) -> Result<FileRecord, ProviderEventsError> {
    let Some(existing_scope) = record.repository_scope.as_ref() else {
        return Ok(record.clone());
    };
    let renamed_scope = RepositoryScope::new(
        new_repository.provider().repository_provider(),
        new_repository.owner(),
        new_repository.name(),
        existing_scope.revision(),
    )
    .map_err(|_error| ProviderEventsError::InvalidProviderWebhookPayload)?;
    let mut renamed_record = record.clone();
    renamed_record.repository_scope = Some(renamed_scope);
    Ok(renamed_record)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RecordIdentity<'record> {
    provider: Option<&'static str>,
    owner: Option<&'record str>,
    repository: Option<&'record str>,
    revision: Option<&'record str>,
    file_id: &'record str,
    content_hash: &'record str,
}

fn record_identity_key(record: &FileRecord) -> RecordIdentity<'_> {
    let scope = record.repository_scope.as_ref();
    RecordIdentity {
        provider: scope.map(|scope| scope.provider().as_str()),
        owner: scope.map(RepositoryScope::owner),
        repository: scope.map(RepositoryScope::name),
        revision: scope.and_then(RepositoryScope::revision),
        file_id: &record.file_id,
        content_hash: &record.content_hash,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use shardline_index::{FileChunkRecord, MemoryRecordStore, RecordMutation, RecordTraversal};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_vcs::{ProviderKind, RepositoryRef};

    use super::{
        ensure_absent_or_matching_record, parse_record_entry, record_belongs_to_repository,
        record_identity_key, renamed_file_record, repository_record_scope,
    };

    fn test_record() -> super::FileRecord {
        super::FileRecord {
            deduplication_domain: None,
            file_id: "file.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 8,
            chunk_size: 4,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", Some("main"))
                    .unwrap(),
            ),
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        }
    }

    #[test]
    fn renamed_file_record_updates_scope() {
        let record = test_record();
        let new_repository =
            RepositoryRef::new(ProviderKind::GitHub, "new-owner", "new-repo").unwrap();
        let renamed = renamed_file_record(&record, &new_repository).unwrap();
        let scope = renamed.repository_scope.unwrap();
        assert_eq!(scope.owner(), "new-owner");
        assert_eq!(scope.name(), "new-repo");
        assert_eq!(scope.revision(), Some("main"));
    }

    #[test]
    fn renamed_file_record_no_scope_returns_clone() {
        let record = super::FileRecord {
            repository_scope: None,
            ..test_record()
        };
        let new_repository =
            RepositoryRef::new(ProviderKind::GitHub, "new-owner", "new-repo").unwrap();
        let renamed = renamed_file_record(&record, &new_repository).unwrap();
        assert_eq!(renamed.file_id, record.file_id);
        assert_eq!(renamed.repository_scope, None);
    }

    #[test]
    fn record_identity_preserves_scope_and_file_fields() {
        for (scoped, revision) in [(false, None), (true, None), (true, Some("main"))] {
            for file_id in ["file.bin", "file with spaces (1).bin"] {
                let record = super::FileRecord {
                    repository_scope: scoped.then(|| {
                        RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", revision)
                            .unwrap()
                    }),
                    file_id: file_id.to_owned(),
                    ..test_record()
                };
                let key = record_identity_key(&record);
                assert_eq!(key.provider, scoped.then_some("github"));
                assert_eq!(key.owner, scoped.then_some("owner"));
                assert_eq!(key.repository, scoped.then_some("repo"));
                assert_eq!(key.revision, revision);
                assert_eq!(key.file_id, file_id);
                assert_eq!(key.content_hash, "a".repeat(64));
            }
        }
    }

    #[test]
    fn collect_deleted_no_duplicate_identity_keys() {
        let record = test_record();
        let references = super::collect_deleted_repository_references(
            &shardline_server_core::ServerObjectStore::blackhole(),
            [&record, &record].into_iter(),
        )
        .unwrap();
        assert_eq!(references.file_versions, 1);
        assert_eq!(references.chunk_hashes, HashSet::from(["b".repeat(64)]));
        assert_eq!(references.held_object_keys.len(), 2);
    }

    #[test]
    fn deletion_keeps_distinct_revisions_and_delimited_fields() {
        for ((revision_a, file_a), (revision_b, file_b)) in [
            ((None, "file.bin"), (Some("latest"), "file.bin")),
            (
                (Some("main:part"), "file.bin"),
                (Some("main"), "part:file.bin"),
            ),
        ] {
            let mut first = test_record();
            first.repository_scope = Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", revision_a)
                    .unwrap(),
            );
            first.file_id = file_a.to_owned();
            let mut second = first.clone();
            second.repository_scope = Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", revision_b)
                    .unwrap(),
            );
            second.file_id = file_b.to_owned();
            second.chunks[0].hash = "c".repeat(64);
            let super::DeletedRepositoryReferences {
                file_versions,
                chunk_hashes,
                held_object_keys,
            } = super::collect_deleted_repository_references(
                &shardline_server_core::ServerObjectStore::blackhole(),
                [&first, &second, &first, &second].into_iter(),
            )
            .unwrap();
            assert_eq!(file_versions, 2, "{revision_a:?} / {revision_b:?}");
            assert_eq!(
                chunk_hashes,
                HashSet::from(["b".repeat(64), "c".repeat(64)])
            );
            for hash in &chunk_hashes {
                assert!(held_object_keys.contains(super::chunk_object_key(hash).unwrap().as_str()));
                assert!(held_object_keys.contains(super::xorb_object_key(hash).unwrap().as_str()));
            }
            assert_eq!(held_object_keys.len(), 4);
        }
    }

    #[test]
    fn parse_record_entry_round_trip() {
        let record = test_record();
        let bytes = serde_json::to_vec(&record).unwrap();
        let parsed = parse_record_entry(&bytes).unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn parse_record_entry_invalid_bytes_returns_error() {
        let result = parse_record_entry(b"not valid json");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ensure_absent_or_matching_record_absent_is_ok() {
        let store = MemoryRecordStore::new();
        let record = test_record();
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let result = ensure_absent_or_matching_record(&store, &locator, &record).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_absent_or_matching_record_succeeds_when_matching() {
        let store = MemoryRecordStore::new();
        let record = test_record();

        // Write the record first
        RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();

        // Check that the matching record is accepted
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let result = ensure_absent_or_matching_record(&store, &locator, &record).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ensure_absent_or_matching_record_rejects_conflicting() {
        let store = MemoryRecordStore::new();
        let record = test_record();
        let mut different = test_record();
        different.file_id = "different.bin".to_owned();

        // Write the different record
        RecordMutation::write_version_record(&store, &different)
            .await
            .unwrap();

        // Ensure rejects when bytes don't match
        let locator = RecordTraversal::version_record_locator(&store, &different);
        let result = ensure_absent_or_matching_record(&store, &locator, &record).await;
        assert!(matches!(
            result,
            Err(crate::ProviderEventsError::ConflictingRenameTargetRecord)
        ));
    }

    // ── renamed_file_record preserves revision ────────────────────────────

    #[test]
    fn renamed_file_record_preserves_revision() {
        let record = super::FileRecord {
            repository_scope: Some(
                RepositoryScope::new(
                    RepositoryProvider::GitHub,
                    "old-owner",
                    "old-repo",
                    Some("feature-x"),
                )
                .unwrap(),
            ),
            ..test_record()
        };
        let new_repo = RepositoryRef::new(ProviderKind::GitHub, "new-owner", "new-repo").unwrap();
        let renamed = renamed_file_record(&record, &new_repo).unwrap();
        let scope = renamed.repository_scope.unwrap();
        assert_eq!(scope.revision(), Some("feature-x"));
    }

    #[test]
    fn repository_scope_matching_and_mapping() {
        let providers = [
            (ProviderKind::GitHub, RepositoryProvider::GitHub),
            (ProviderKind::GitLab, RepositoryProvider::GitLab),
            (ProviderKind::Gitea, RepositoryProvider::Gitea),
            (ProviderKind::Codeberg, RepositoryProvider::Codeberg),
            (ProviderKind::Generic, RepositoryProvider::Generic),
        ];
        for (kind, provider) in providers {
            let repository = RepositoryRef::new(kind, "owner", "repo").unwrap();
            let scope = repository_record_scope(&repository);
            assert_eq!(scope.provider(), provider);
            assert_eq!(scope.owner(), "owner");
            assert_eq!(scope.name(), "repo");

            for (other_kind, other_provider) in providers {
                let record = super::FileRecord {
                    repository_scope: Some(
                        RepositoryScope::new(other_provider, "owner", "repo", Some("main"))
                            .unwrap(),
                    ),
                    ..test_record()
                };
                for (owner, name) in [
                    ("owner", "repo"),
                    ("other-owner", "repo"),
                    ("wrong", "repo"),
                    ("owner", "other-repo"),
                    ("owner", "wrong"),
                ] {
                    let target = RepositoryRef::new(kind, owner, name).unwrap();
                    assert_eq!(
                        record_belongs_to_repository(&record, &target),
                        kind == other_kind && owner == "owner" && name == "repo",
                        "record={other_kind:?}, target={kind:?}/{owner}/{name}",
                    );
                }
            }
            let unscoped = super::FileRecord {
                repository_scope: None,
                ..test_record()
            };
            assert!(!record_belongs_to_repository(&unscoped, &repository));
        }
    }

    // ── parse_record_entry with empty bytes ──────────────────────────────

    #[test]
    fn parse_record_entry_empty_bytes_returns_error() {
        let result = parse_record_entry(b"");
        assert!(result.is_err());
    }
}
