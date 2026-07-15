use std::collections::HashSet;

use serde_json::to_vec;
use shardline_index::{FileRecord, RecordStore, RecordTraversal, RepositoryRecordScope};
use shardline_protocol::RepositoryScope;
use shardline_vcs::RepositoryRef;

use crate::ProviderEventsError;
use shardline_server_core::{
    ServerObjectStore, chunk_object_key, parse_stored_file_record_bytes, provider_directory,
};
use shardline_xet_adapter::{visit_stored_xorb_chunk_hashes, xorb_object_key};

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

pub(super) fn collect_deleted_repository_record_references(
    object_store: &ServerObjectStore,
    record: &FileRecord,
    seen_record_identities: &mut HashSet<String>,
    file_versions: &mut u64,
    chunk_hashes: &mut HashSet<String>,
    held_object_keys: &mut HashSet<String>,
) -> Result<(), ProviderEventsError> {
    if !seen_record_identities.insert(record_identity_key(record)) {
        return Ok(());
    }

    *file_versions = file_versions
        .checked_add(1)
        .ok_or(ProviderEventsError::Overflow)?;
    for chunk in &record.chunks {
        if record.chunk_size == 0 {
            let xorb_object_key = xorb_object_key(&chunk.hash)?;
            held_object_keys.insert(xorb_object_key.as_str().to_owned());
            let mut visit_result = Ok(());
            visit_stored_xorb_chunk_hashes(object_store, &xorb_object_key, |chunk_hash_hex| {
                match chunk_object_key(&chunk_hash_hex) {
                    Ok(chunk_object_key) => {
                        held_object_keys.insert(chunk_object_key.as_str().to_owned());
                        chunk_hashes.insert(chunk_hash_hex);
                        Ok(())
                    }
                    Err(e) => {
                        visit_result = Err(e);
                        Err(shardline_xet_adapter::XetAdapterError::NotFound)
                    }
                }
            })?;
            visit_result?;
            continue;
        }

        chunk_hashes.insert(chunk.hash.clone());
        let chunk_object_key = chunk_object_key(&chunk.hash)?;
        held_object_keys.insert(chunk_object_key.as_str().to_owned());
        let xorb_object_key = xorb_object_key(&chunk.hash)?;
        held_object_keys.insert(xorb_object_key.as_str().to_owned());
    }

    Ok(())
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

fn record_identity_key(record: &FileRecord) -> String {
    let repository_scope = record.repository_scope.as_ref().map_or_else(
        || "unscoped".to_owned(),
        |scope| {
            format!(
                "{}:{}/{}@{}",
                provider_directory(scope.provider()),
                scope.owner(),
                scope.name(),
                scope.revision().unwrap_or("latest"),
            )
        },
    );
    format!(
        "{repository_scope}:{}:{}",
        record.file_id, record.content_hash
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use shardline_index::{
        FileChunkRecord, MemoryRecordStore, RecordMutation, RecordTraversal,
    };
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_vcs::{ProviderKind, RepositoryRef};

    use super::{
        ensure_absent_or_matching_record, parse_record_entry, record_belongs_to_repository,
        record_identity_key, renamed_file_record, repository_record_scope,
    };

    fn test_record() -> super::FileRecord {
        super::FileRecord {
            file_id: "file.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: Some(
                RepositoryScope::new(
                    RepositoryProvider::GitHub,
                    "owner",
                    "repo",
                    Some("main"),
                )
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
    fn record_belongs_to_repository_matching() {
        let record = test_record();
        let repository =
            RepositoryRef::new(ProviderKind::GitHub, "owner", "repo").unwrap();
        assert!(record_belongs_to_repository(&record, &repository));
    }

    #[test]
    fn record_belongs_to_repository_mismatched_owner() {
        let record = test_record();
        let repository =
            RepositoryRef::new(ProviderKind::GitHub, "other-owner", "repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &repository));
    }

    #[test]
    fn record_belongs_to_repository_mismatched_name() {
        let record = test_record();
        let repository =
            RepositoryRef::new(ProviderKind::GitHub, "owner", "other-repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &repository));
    }

    #[test]
    fn record_belongs_to_repository_mismatched_provider() {
        let record = test_record();
        let repository =
            RepositoryRef::new(ProviderKind::GitLab, "owner", "repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &repository));
    }

    #[test]
    fn record_belongs_to_repository_check_all_fields() {
        // Verify that all three conditions (provider, owner, name) must match
        let record = test_record();
        // Owner differs
        let wrong_owner = RepositoryRef::new(ProviderKind::GitHub, "wrong", "repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &wrong_owner));
        // Name differs
        let wrong_name = RepositoryRef::new(ProviderKind::GitHub, "owner", "wrong").unwrap();
        assert!(!record_belongs_to_repository(&record, &wrong_name));
        // Provider differs
        let wrong_provider = RepositoryRef::new(ProviderKind::GitLab, "owner", "repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &wrong_provider));
    }

    #[test]
    fn record_belongs_to_repository_no_scope() {
        let record = super::FileRecord {
            repository_scope: None,
            ..test_record()
        };
        let repository =
            RepositoryRef::new(ProviderKind::GitHub, "owner", "repo").unwrap();
        assert!(!record_belongs_to_repository(&record, &repository));
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
    fn record_identity_key_includes_scope() {
        let record = test_record();
        let key = record_identity_key(&record);
        assert!(key.contains("file.bin"));
        assert!(key.contains(&"a".repeat(64)));
        assert!(key.contains("owner/repo"));
    }

    #[test]
    fn record_identity_key_scopeless_uses_unscoped() {
        let record = super::FileRecord {
            repository_scope: None,
            ..test_record()
        };
        let key = record_identity_key(&record);
        assert!(key.contains("unscoped"));
    }

    #[test]
    fn repository_record_scope_maps_correctly() {
        let repository =
            RepositoryRef::new(ProviderKind::GitHub, "owner", "repo").unwrap();
        let scope = repository_record_scope(&repository);
        assert_eq!(
            scope.provider(),
            RepositoryProvider::GitHub
        );
    }

    #[test]
    fn collect_deleted_no_duplicate_identity_keys() {
        let record = test_record();
        let mut seen = HashSet::new();
        let mut file_versions = 0u64;
        let mut chunk_hashes = HashSet::new();
        let mut held_object_keys = HashSet::new();

        // First call should succeed
        let result = super::collect_deleted_repository_record_references(
            &shardline_server_core::ServerObjectStore::local(
                tempfile::tempdir().unwrap().path().join("chunks"),
            )
            .unwrap(),
            &record,
            &mut seen,
            &mut file_versions,
            &mut chunk_hashes,
            &mut held_object_keys,
        );
        assert!(result.is_ok());
        assert_eq!(file_versions, 1);
        assert!(chunk_hashes.contains(&"b".repeat(64)));

        // Second call with same identity should be no-op
        let result = super::collect_deleted_repository_record_references(
            &shardline_server_core::ServerObjectStore::local(
                tempfile::tempdir().unwrap().path().join("chunks"),
            )
            .unwrap(),
            &record,
            &mut seen,
            &mut file_versions,
            &mut chunk_hashes,
            &mut held_object_keys,
        );
        assert!(result.is_ok());
        assert_eq!(file_versions, 1);
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

    #[test]
    fn record_identity_key_without_revision_uses_latest() {
        let record = super::FileRecord {
            repository_scope: Some(
                RepositoryScope::new(
                    RepositoryProvider::GitHub,
                    "owner",
                    "repo",
                    None,
                )
                .unwrap(),
            ),
            ..test_record()
        };
        let key = record_identity_key(&record);
        assert!(key.contains("latest"), "expected 'latest' in identity key, got {key:?}");
        assert!(!key.contains("main"));
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

    // ── collect_deleted_repository_record_references: provider+all providers ─

    #[test]
    fn repository_record_scope_all_providers() {
        for provider in &[
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ] {
            let repo = RepositoryRef::new(
                match provider {
                    RepositoryProvider::GitHub => ProviderKind::GitHub,
                    RepositoryProvider::GitLab => ProviderKind::GitLab,
                    RepositoryProvider::Gitea => ProviderKind::Gitea,
                    RepositoryProvider::Codeberg => ProviderKind::Codeberg,
                    RepositoryProvider::Generic => ProviderKind::Generic,
                },
                "owner",
                "repo",
            )
            .unwrap();
            let scope = repository_record_scope(&repo);
            assert_eq!(scope.provider(), *provider);
            assert_eq!(scope.owner(), "owner");
            assert_eq!(scope.name(), "repo");
        }
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
        let new_repo =
            RepositoryRef::new(ProviderKind::GitHub, "new-owner", "new-repo").unwrap();
        let renamed = renamed_file_record(&record, &new_repo).unwrap();
        let scope = renamed.repository_scope.unwrap();
        assert_eq!(scope.revision(), Some("feature-x"));
    }

    // ── record_identity_key special characters ────────────────────────────

    #[test]
    fn record_identity_key_special_characters() {
        let record = super::FileRecord {
            file_id: "file with spaces (1).bin".to_owned(),
            content_hash: "a".repeat(64),
            ..test_record()
        };
        let key = record_identity_key(&record);
        assert!(key.contains("file with spaces (1).bin"));
    }

    // ── record_belongs_to_repository all provider variants ───────────────

    #[test]
    fn record_belongs_to_repository_all_providers_match() {
        let cases = [
            (ProviderKind::GitHub, RepositoryProvider::GitHub),
            (ProviderKind::GitLab, RepositoryProvider::GitLab),
            (ProviderKind::Gitea, RepositoryProvider::Gitea),
            (ProviderKind::Codeberg, RepositoryProvider::Codeberg),
            (ProviderKind::Generic, RepositoryProvider::Generic),
        ];
        for (kind, prov) in &cases {
            let record = super::FileRecord {
                repository_scope: Some(
                    RepositoryScope::new(*prov, "owner", "repo", Some("main")).unwrap(),
                ),
                ..test_record()
            };
            let repository = RepositoryRef::new(*kind, "owner", "repo").unwrap();
            assert!(
                record_belongs_to_repository(&record, &repository),
                "expected match for {kind:?}"
            );
        }
    }

    // ── parse_record_entry with empty bytes ──────────────────────────────

    #[test]
    fn parse_record_entry_empty_bytes_returns_error() {
        let result = parse_record_entry(b"");
        assert!(result.is_err());
    }
}
