//! A signed reconstruction names a file, so probe that authoritative record
//! before discovering every repository record sharing its container.

use super::{RepositoryScope, ServerBackend, ServerError};
use crate::record_store::XorbRangeCoverage;
use shardline_protocol::ByteRange;

#[cfg(test)]
mod tests;

impl ServerBackend {
    pub(crate) async fn file_xorb_coverage(
        &self,
        file_id: &str,
        hash: &str,
        scope: &RepositoryScope,
        requested: ByteRange,
    ) -> Result<Option<XorbRangeCoverage>, ServerError> {
        let record = match self {
            Self::Local(backend) => backend.read_record(file_id, None, Some(scope)).await,
            Self::Postgres(backend) => backend.read_record(file_id, None, Some(scope)).await,
        };
        let record = match record {
            Ok(record) => record,
            Err(ServerError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        if record.file_id != file_id {
            return Ok(None);
        }
        Ok(XorbRangeCoverage::from_file_record(
            &record, scope, hash, requested,
        ))
    }
}
