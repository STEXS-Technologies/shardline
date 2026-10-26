//! A signed reconstruction names a file, so probe that authoritative record
//! before discovering every repository record sharing its container.

use super::{RepositoryScope, ServerBackend, ServerError};
use crate::record_store::XorbRangeCoverage;
#[cfg(test)]
use shardline_protocol::ByteRange;

#[cfg(test)]
mod tests;

impl ServerBackend {
    #[cfg(test)]
    pub(crate) async fn file_xorb_coverage(
        &self,
        file_id: &str,
        hash: &str,
        scope: &RepositoryScope,
        requested: ByteRange,
    ) -> Result<Option<XorbRangeCoverage>, ServerError> {
        Ok(self
            .file_xorb_candidate(file_id, hash, scope)
            .await?
            .filter(|coverage| coverage.covers(requested)))
    }

    /// A current file proves membership only, pending verified source bounds.
    pub(crate) async fn file_xorb_candidate(
        &self,
        file_id: &str,
        hash: &str,
        scope: &RepositoryScope,
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
        Ok(XorbRangeCoverage::from_file_record_members(
            &record, scope, hash,
        ))
    }
}
