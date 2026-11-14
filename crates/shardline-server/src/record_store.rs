use crate::ServerError;

mod membership;
mod xorb_range;
pub(crate) use membership::repository_references_hash_in_scope;
pub(crate) use xorb_range::{
    Coverage as XorbRangeCoverage, first_xorb_coverage, repository_xorb_coverage,
};

pub(crate) use shardline_index::LocalRecordStore;

pub(crate) fn file_record_locator<Store: shardline_index::RecordTraversal>(
    store: &Store,
    file_id: &str,
    content_hash: Option<&str>,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<Store::Locator, ServerError> {
    crate::validation::validate_identifier(file_id)?;
    if let Some(hash) = content_hash {
        crate::validation::validate_content_hash(hash)?;
    }
    let probe = shardline_index::FileRecord {
        file_id: file_id.to_owned(),
        content_hash: content_hash.unwrap_or_default().to_owned(),
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
        repository_scope: repository_scope.cloned(),
        chunks: Vec::new(),
    };
    Ok(if content_hash.is_some() {
        store.version_record_locator(&probe)
    } else {
        store.latest_record_locator(&probe)
    })
}

pub(crate) fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, ServerError> {
    Ok(shardline_server_core::parse_stored_file_record_bytes(
        bytes,
    )?)
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
