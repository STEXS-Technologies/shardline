pub(crate) use shardline_index::LocalRecordStore;

pub(crate) fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, crate::ServerError> {
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
