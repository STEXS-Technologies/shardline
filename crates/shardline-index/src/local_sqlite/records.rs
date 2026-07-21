use serde::{Deserialize, Serialize};
use shardline_protocol::ChunkRange;
use shardline_storage::ObjectKey;

use crate::{
    DedupeShardMapping, FileReconstruction, ReconstructionTerm, StoredObjectId, parse_xet_hash_hex,
    xet_hash_hex_string,
};

use super::LocalIndexStoreError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FileReconstructionRecord {
    terms: Vec<ReconstructionTermRecord>,
}

impl FileReconstructionRecord {
    pub(crate) fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(ReconstructionTermRecord::from_domain)
                .collect(),
        }
    }

    pub(crate) fn into_domain(self) -> Result<FileReconstruction, LocalIndexStoreError> {
        let terms = self
            .terms
            .into_iter()
            .map(ReconstructionTermRecord::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileReconstruction::new(terms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReconstructionTermRecord {
    object_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl ReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            object_hash: xet_hash_hex_string(term.object_id().hash()),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, LocalIndexStoreError> {
        let hash = parse_xet_hash_hex(&self.object_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            StoredObjectId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyQuarantineCandidateRecord {
    pub(crate) hash: String,
    pub(crate) bytes: u64,
    pub(crate) first_seen_unreachable_at_unix_seconds: u64,
    pub(crate) delete_after_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredObjectPresenceRecord {
    pub(crate) hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DedupeShardRecord {
    pub(crate) chunk_hash: String,
    pub(crate) shard_object_key: String,
}

impl DedupeShardRecord {
    pub(crate) fn into_domain(self) -> Result<DedupeShardMapping, LocalIndexStoreError> {
        let chunk_hash = parse_xet_hash_hex(&self.chunk_hash)?;
        let shard_object_key = ObjectKey::parse(&self.shard_object_key)?;
        Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
    }
}
