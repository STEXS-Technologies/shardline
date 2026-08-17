use shardline_index::{RecordTraversal, RepositoryRecordScope};
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectPrefix;

use crate::{
    ServerError,
    model::ServerStatsResponse,
    object_store::{visit_object_prefix, whole_store_chunk_stats},
    overflow::checked_increment,
    protocol_support::scope_namespace,
};

pub(crate) use shardline_index::LocalRecordStore;

pub(crate) fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, ServerError> {
    Ok(shardline_server_core::parse_stored_file_record_bytes(
        bytes,
    )?)
}

/// Computes repository-scoped storage stats for one repository.
///
/// This is the tenant-scoped view used by `/v1/stats` for authenticated
/// callers. Files are attributed per repository from two sources:
/// - the repository's latest file records (xet uploads write records with the
///   repository scope);
/// - the namespace-prefixed protocol objects written by the LFS/OCI/S3/bazel
///   frontends. Those frontends write their records with a `None` repository
///   scope (so the record query above cannot see them), but their object keys
///   embed the per-repository namespace (`protocols/{frontend}/{namespace}/`),
///   so the raw objects are counted directly.
///
/// The chunk pool is dedup-shared CAS infrastructure: nothing per-repository
/// survives an LFS delete, so per-repository chunk attribution is impossible
/// without write-side scope threading. Both stats views therefore report the
/// whole-store pool unchanged.
///
/// # Errors
///
/// Returns [`ServerError`] when the repository's metadata cannot be traversed.
pub(crate) async fn scoped_stats(
    record_store: &(impl RecordTraversal<Error: Into<ServerError>> + Sync),
    object_store: &crate::object_store::ServerObjectStore,
    scope: &RepositoryScope,
) -> Result<ServerStatsResponse, ServerError> {
    let record_scope = RepositoryRecordScope::from_repository_scope(scope);
    let mut files = u64::try_from(
        RecordTraversal::list_repository_latest_record_locators(record_store, &record_scope)
            .await
            .map_err(Into::into)?
            .len(),
    )?;

    let namespace = scope_namespace(Some(scope));
    for prefix in [
        format!("protocols/lfs/{namespace}/"),
        format!("protocols/oci/{namespace}/"),
        format!("protocols/s3/{namespace}/"),
        format!("protocols/bazel/{namespace}/"),
    ] {
        let prefix =
            ObjectPrefix::parse(&prefix).map_err(|_error| ServerError::InvalidContentHash)?;
        visit_object_prefix(object_store, &prefix, |_metadata| {
            files = checked_increment(files)?;
            Ok::<(), ServerError>(())
        })?;
    }

    let (chunks, chunk_bytes) = whole_store_chunk_stats(object_store)?;

    Ok(ServerStatsResponse {
        chunks,
        chunk_bytes,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_stored_file_record_bytes;
    use crate::ServerError;

    #[test]
    fn parse_stored_file_record_bytes_rejects_oversized_metadata_before_json_parsing() {
        use shardline_server_core::MAX_LOCAL_RECORD_METADATA_BYTES;
        let oversized_len = usize::try_from(MAX_LOCAL_RECORD_METADATA_BYTES)
            .ok()
            .and_then(|length| length.checked_add(1));
        assert!(oversized_len.is_some());
        let Some(oversized_len) = oversized_len else {
            return;
        };
        let oversized = vec![b'{'; oversized_len];

        assert!(matches!(
            parse_stored_file_record_bytes(&oversized),
            Err(ServerError::StoredFileMetadataTooLarge {
                maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn parse_stored_file_record_bytes_rejects_invalid_json() {
        let result = parse_stored_file_record_bytes(b"not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn parse_stored_file_record_bytes_rejects_empty_bytes() {
        let result = parse_stored_file_record_bytes(b"");
        assert!(result.is_err());
    }
}
