use std::io::ErrorKind;

use shardline_index::{FileRecord, LocalIndexStoreError, LocalRecordStore, RecordTraversal};
use shardline_protocol::RepositoryScope;

use crate::{
    ServerError,
    error::IndexError,
    record_store::{file_record_locator, parse_stored_file_record_bytes},
};

pub(super) async fn read_record(
    record_store: &LocalRecordStore,
    file_id: &str,
    content_hash: Option<&str>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<FileRecord, ServerError> {
    let locator = file_record_locator(record_store, file_id, content_hash, repository_scope)?;
    let bytes = read_record_bytes(record_store, &locator).await?;
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
    crate::record_store::repository_references_hash_in_scope(
        record_store,
        hash_hex,
        repository_scope,
    )
    .await
}

#[cfg(test)]
mod tests {
    use shardline_index::{FileChunkRecord, FileRecord};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    fn make_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap()
    }

    fn make_other_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "other-owner", "repo", None).unwrap()
    }

    fn make_record(scope: &RepositoryScope, chunk_hash: &str) -> FileRecord {
        FileRecord {
            deduplication_domain: None,
            file_id: "test.txt".to_owned(),
            content_hash: "deadbeef".to_owned(),
            total_bytes: 100,
            chunk_size: 64,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
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
        assert!(matches!(
            result,
            Err(crate::ServerError::InvalidContentHash)
        ));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repository_references_xorb_returns_true_for_matching_record_in_latest() {
        let (_tmp, store) = make_temp_record_store();
        let scope = make_scope();
        let chunk_hash = "fedcba0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let record = make_record(&scope, chunk_hash);
        shardline_index::RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();

        let result = repository_references_xorb(&store, chunk_hash, &scope)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repository_references_xorb_returns_true_for_matching_record_in_version() {
        let (_tmp, store) = make_temp_record_store();
        let scope = make_scope();
        let chunk_hash = "deadbeef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let record = make_record(&scope, chunk_hash);
        shardline_index::RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();

        let result = repository_references_xorb(&store, chunk_hash, &scope)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repository_references_xorb_false_for_wrong_hash() {
        let (_tmp, store) = make_temp_record_store();
        let scope = make_scope();
        let record = make_record(
            &scope,
            "1111aaaabbbbccccddddeeeeffff00001111aaaabbbbccccddddeeeeffff0000",
        );
        shardline_index::RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();

        let different_hash = "2222aaaabbbbccccddddeeeeffff00002222aaaabbbbccccddddeeeeffff0000";
        let result = repository_references_xorb(&store, different_hash, &scope)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repository_references_xorb_false_for_wrong_scope() {
        let (_tmp, store) = make_temp_record_store();
        let scope = make_scope();
        let other_scope = make_other_scope();
        let chunk_hash = "3333aaaabbbbccccddddeeeeffff00003333aaaabbbbccccddddeeeeffff0000";
        let record = make_record(&scope, chunk_hash);
        shardline_index::RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();

        let result = repository_references_xorb(&store, chunk_hash, &other_scope)
            .await
            .unwrap();
        assert!(!result);
    }
}
