use std::io::ErrorKind;

use shardline_index::{
    FileRecord, LocalIndexStoreError, LocalRecordStore, RecordTraversal, RepositoryRecordScope,
};
use shardline_protocol::RepositoryScope;

use crate::{
    ServerError,
    error::IndexError,
    record_store::parse_stored_file_record_bytes,
    validation::{validate_content_hash, validate_identifier},
};

pub(super) async fn read_record(
    record_store: &LocalRecordStore,
    file_id: &str,
    content_hash: Option<&str>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<FileRecord, ServerError> {
    validate_identifier(file_id)?;
    let bytes = if let Some(hash) = content_hash {
        validate_content_hash(hash)?;
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = RecordTraversal::version_record_locator(record_store, &record);
        read_record_bytes(record_store, &locator).await?
    } else {
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: String::new(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = RecordTraversal::latest_record_locator(record_store, &record);
        read_record_bytes(record_store, &locator).await?
    };
    parse_stored_file_record_bytes(&bytes)
}

async fn read_record_bytes(
    record_store: &LocalRecordStore,
    locator: &<LocalRecordStore as RecordTraversal>::Locator,
) -> Result<Vec<u8>, ServerError> {
    match RecordTraversal::read_record_bytes(record_store, locator).await {
        Ok(bytes) => Ok(bytes),
        Err(LocalIndexStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {
            Err(ServerError::NotFound)
        }
        Err(error) => Err(ServerError::Index(IndexError::Local(error))),
    }
}

pub(super) async fn repository_references_xorb(
    record_store: &LocalRecordStore,
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError> {
    let repository = RepositoryRecordScope::from_repository_scope(repository_scope);
    let mut found = false;
    record_store
        .visit_repository_latest_records(&repository, |entry| {
            inspect_repository_record_for_xorb(&mut found, &entry.bytes, hash_hex, repository_scope)
        })
        .await?;
    if found {
        return Ok(true);
    }
    record_store
        .visit_repository_version_records(&repository, |entry| {
            inspect_repository_record_for_xorb(&mut found, &entry.bytes, hash_hex, repository_scope)
        })
        .await?;

    Ok(found)
}

fn inspect_repository_record_for_xorb(
    found: &mut bool,
    bytes: &[u8],
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<(), ServerError> {
    if *found {
        return Ok(());
    }
    let record = parse_stored_file_record_bytes(bytes)?;
    if record.repository_scope.as_ref() == Some(repository_scope)
        && record.chunks.iter().any(|chunk| chunk.hash == hash_hex)
    {
        *found = true;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::to_vec;
    use shardline_index::{FileChunkRecord, FileRecord};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::inspect_repository_record_for_xorb;

    fn make_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap()
    }

    fn make_other_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "other-owner", "repo", None).unwrap()
    }

    fn make_record(scope: &RepositoryScope, chunk_hash: &str) -> FileRecord {
        FileRecord {
            file_id: "test.txt".to_owned(),
            content_hash: "deadbeef".to_owned(),
            total_bytes: 100,
            chunk_size: 64,
            repository_scope: Some(scope.clone()),
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.to_owned(),
                offset: 0,
                length: 100,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        }
    }

    #[test]
    fn matching_hash_and_scope() {
        let scope = make_scope();
        let record = make_record(&scope, "abc123");
        let bytes = to_vec(&record).unwrap();

        let mut found = false;
        inspect_repository_record_for_xorb(&mut found, &bytes, "abc123", &scope).unwrap();
        assert!(found);
    }

    #[test]
    fn matching_hash_wrong_scope() {
        let scope = make_scope();
        let record = make_record(&scope, "abc123");
        let bytes = to_vec(&record).unwrap();

        let mut found = false;
        let other = make_other_scope();
        inspect_repository_record_for_xorb(&mut found, &bytes, "abc123", &other).unwrap();
        assert!(!found);
    }

    #[test]
    fn different_hash() {
        let scope = make_scope();
        let record = make_record(&scope, "abc123");
        let bytes = to_vec(&record).unwrap();

        let mut found = false;
        inspect_repository_record_for_xorb(&mut found, &bytes, "xyz789", &scope).unwrap();
        assert!(!found);
    }

    #[test]
    fn already_found() {
        let scope = make_scope();
        let mut found = true;

        // When found is already true, the function returns early without parsing,
        // so even garbage bytes are fine.
        inspect_repository_record_for_xorb(&mut found, b"not valid json", "anything", &scope)
            .unwrap();
        assert!(found);
    }

    #[test]
    fn invalid_bytes() {
        let scope = make_scope();
        let mut found = false;

        let result =
            inspect_repository_record_for_xorb(&mut found, b"not valid json", "anything", &scope);
        assert!(result.is_err());
    }

    // ── read_record tests ───────────────────────────────────────────────────

    use super::read_record;
    use shardline_index::LocalRecordStore;

    fn make_temp_record_store() -> (tempfile::TempDir, LocalRecordStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalRecordStore::open(tmp.path().to_path_buf());
        (tmp, store)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_record_returns_not_found_for_missing_file() {
        let (_tmp, store) = make_temp_record_store();
        let result = read_record(&store, "nonexistent.txt", None, None).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_record_rejects_invalid_file_id() {
        let (_tmp, store) = make_temp_record_store();
        let result = read_record(&store, "../bad", None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_record_rejects_invalid_content_hash() {
        let (_tmp, store) = make_temp_record_store();
        let result = read_record(&store, "test.txt", Some("badhash"), None).await;
        assert!(matches!(result, Err(crate::ServerError::InvalidContentHash)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_record_with_hash_not_found_for_missing_file() {
        let (_tmp, store) = make_temp_record_store();
        let result = read_record(
            &store,
            "test.txt",
            Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"),
            None,
        )
        .await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    // ── repository_references_xorb tests ────────────────────────────────────

    use super::repository_references_xorb;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repository_references_xorb_returns_false_for_empty_store() {
        let (_tmp, store) = make_temp_record_store();
        let scope = make_scope();
        let result = repository_references_xorb(
            &store,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            &scope,
        )
        .await
        .unwrap();
        assert!(!result);
    }
}
