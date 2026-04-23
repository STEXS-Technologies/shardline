use serde_json::from_slice;
use shardline_index::FileRecord;
pub(crate) use shardline_index::LocalRecordStore;

use crate::ServerError;

pub(crate) const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

pub(crate) fn parse_stored_file_record_bytes(bytes: &[u8]) -> Result<FileRecord, ServerError> {
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes > MAX_LOCAL_RECORD_METADATA_BYTES {
        return Err(ServerError::StoredFileMetadataTooLarge {
            observed_bytes,
            maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
        });
    }

    Ok(from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::{MAX_LOCAL_RECORD_METADATA_BYTES, parse_stored_file_record_bytes};
    use crate::ServerError;

    #[test]
    fn parse_stored_file_record_bytes_rejects_oversized_metadata_before_json_parsing() {
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
}
