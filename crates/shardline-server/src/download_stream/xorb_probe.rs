//! Resolve packed containers without a separate S3 HEAD on successful reads.
use tempfile::NamedTempFile;

use super::{ServerError, ServerObjectStore, xorb_object_length};

pub(super) enum PreparedXorb {
    StoredLength(u64),
    Materialized(NamedTempFile),
}

pub(super) fn probe(
    store: &ServerObjectStore,
    hash: &str,
) -> Result<Option<PreparedXorb>, ServerError> {
    if let ServerObjectStore::S3(s3) = store {
        let key = crate::xet_adapter::xorb_object_key(hash)?;
        let mut file = NamedTempFile::new()?;
        match s3.copy_object_to_file(&key, file.as_file_mut()) {
            Ok(None | Some(0)) => return Ok(None),
            Ok(Some(_)) => return Ok(Some(PreparedXorb::Materialized(file))),
            Err(error) => {
                // Preserve legacy routing on probe misses/errors. If metadata
                // identifies an existing container, a failed body read must
                // remain an error rather than falling back to individual data.
                if xorb_object_length(store, hash).is_some() {
                    return Err(error.into());
                }
                return Ok(None);
            }
        }
    }
    Ok(xorb_object_length(store, hash).map(PreparedXorb::StoredLength))
}
