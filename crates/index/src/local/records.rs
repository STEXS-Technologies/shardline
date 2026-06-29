use serde::{Deserialize, Serialize};
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;

use super::error::LocalIndexStoreError;
use crate::{
    DedupeShardMapping, FileReconstruction, ProviderRepositoryState, QuarantineCandidate,
    ReconstructionTerm, RetentionHold, StoredObjectId, WebhookDelivery, WebhookDeliveryError,
    parse_xet_hash_hex, provider::parse_repository_provider, xet_hash_hex_string,
};

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
                .collect::<Vec<_>>(),
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
pub(crate) struct ReconstructionTermRecord {
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
pub(crate) struct QuarantineCandidateRecord {
    object_key: String,
    observed_length: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

impl QuarantineCandidateRecord {
    pub(crate) fn from_domain(candidate: &QuarantineCandidate) -> Self {
        Self {
            object_key: candidate.object_key().as_str().to_owned(),
            observed_length: candidate.observed_length(),
            first_seen_unreachable_at_unix_seconds: candidate
                .first_seen_unreachable_at_unix_seconds(),
            delete_after_unix_seconds: candidate.delete_after_unix_seconds(),
        }
    }

    pub(crate) fn into_domain(self) -> Result<QuarantineCandidate, LocalIndexStoreError> {
        let object_key = ObjectKey::parse(&self.object_key)?;
        QuarantineCandidate::new(
            object_key,
            self.observed_length,
            self.first_seen_unreachable_at_unix_seconds,
            self.delete_after_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RetentionHoldRecord {
    object_key: String,
    reason: String,
    held_at_unix_seconds: u64,
    release_after_unix_seconds: Option<u64>,
}

impl RetentionHoldRecord {
    pub(crate) fn from_domain(hold: &RetentionHold) -> Self {
        Self {
            object_key: hold.object_key().as_str().to_owned(),
            reason: hold.reason().to_owned(),
            held_at_unix_seconds: hold.held_at_unix_seconds(),
            release_after_unix_seconds: hold.release_after_unix_seconds(),
        }
    }

    pub(crate) fn into_domain(self) -> Result<RetentionHold, LocalIndexStoreError> {
        let object_key = ObjectKey::parse(&self.object_key)?;
        RetentionHold::new(
            object_key,
            self.reason,
            self.held_at_unix_seconds,
            self.release_after_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WebhookDeliveryRecord {
    provider: String,
    owner: String,
    repo: String,
    delivery_id: String,
    processed_at_unix_seconds: u64,
}

impl WebhookDeliveryRecord {
    pub(crate) fn from_domain(delivery: &WebhookDelivery) -> Self {
        Self {
            provider: delivery.provider().as_str().to_owned(),
            owner: delivery.owner().to_owned(),
            repo: delivery.repo().to_owned(),
            delivery_id: delivery.delivery_id().to_owned(),
            processed_at_unix_seconds: delivery.processed_at_unix_seconds(),
        }
    }

    pub(crate) fn into_domain(self) -> Result<WebhookDelivery, LocalIndexStoreError> {
        let provider = parse_repository_provider(&self.provider, || {
            LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
        })?;
        WebhookDelivery::new(
            provider,
            self.owner,
            self.repo,
            self.delivery_id,
            self.processed_at_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProviderRepositoryStateRecord {
    provider: String,
    owner: String,
    repo: String,
    last_access_changed_at_unix_seconds: Option<u64>,
    last_revision_pushed_at_unix_seconds: Option<u64>,
    last_pushed_revision: Option<String>,
    #[serde(default)]
    last_cache_invalidated_at_unix_seconds: Option<u64>,
    #[serde(default)]
    last_authorization_rechecked_at_unix_seconds: Option<u64>,
    #[serde(default)]
    last_drift_checked_at_unix_seconds: Option<u64>,
}

impl ProviderRepositoryStateRecord {
    pub(crate) fn from_domain(state: &ProviderRepositoryState) -> Self {
        Self {
            provider: state.provider().as_str().to_owned(),
            owner: state.owner().to_owned(),
            repo: state.repo().to_owned(),
            last_access_changed_at_unix_seconds: state.last_access_changed_at_unix_seconds(),
            last_revision_pushed_at_unix_seconds: state.last_revision_pushed_at_unix_seconds(),
            last_pushed_revision: state.last_pushed_revision().map(ToOwned::to_owned),
            last_cache_invalidated_at_unix_seconds: state.last_cache_invalidated_at_unix_seconds(),
            last_authorization_rechecked_at_unix_seconds: state
                .last_authorization_rechecked_at_unix_seconds(),
            last_drift_checked_at_unix_seconds: state.last_drift_checked_at_unix_seconds(),
        }
    }

    pub(crate) fn into_domain(
        self,
    ) -> Result<ProviderRepositoryState, LocalIndexStoreError> {
        let provider = parse_repository_provider(&self.provider, || {
            LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
        })?;
        Ok(ProviderRepositoryState::new(
            provider,
            self.owner,
            self.repo,
            self.last_access_changed_at_unix_seconds,
            self.last_revision_pushed_at_unix_seconds,
            self.last_pushed_revision,
        )
        .with_reconciliation(
            self.last_cache_invalidated_at_unix_seconds,
            self.last_authorization_rechecked_at_unix_seconds,
            self.last_drift_checked_at_unix_seconds,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyQuarantineCandidateRecord {
    hash: String,
    bytes: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredObjectPresenceRecord {
    hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DedupeShardRecord {
    chunk_hash: String,
    shard_object_key: String,
}

impl DedupeShardRecord {
    pub(crate) fn into_domain(self) -> Result<DedupeShardMapping, LocalIndexStoreError> {
        let chunk_hash = parse_xet_hash_hex(&self.chunk_hash)?;
        let shard_object_key = ObjectKey::parse(&self.shard_object_key)?;
        Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
    }
}
