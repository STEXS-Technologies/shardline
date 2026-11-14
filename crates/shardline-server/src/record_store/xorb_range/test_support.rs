use shardline_index::{FileChunkRecord, FileRecord, StorageRepresentation};
use shardline_protocol::RepositoryScope;

pub(super) fn record(file_id: &str, hash: &str, scope: &RepositoryScope) -> FileRecord {
    FileRecord {
        deduplication_domain: None,
        file_id: file_id.into(),
        content_hash: "b".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.into(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    }
}
