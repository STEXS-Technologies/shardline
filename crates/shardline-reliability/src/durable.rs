use crate::{
    DigestSnapshot, HubRefSnapshot, OciObjectLifecycleState, OciObjectSnapshot, OciTagSnapshot,
    OperationIdentity, ProviderLifecycleSnapshot, QuarantineLifecycleState, QuarantineSnapshot,
    RetentionHoldLifecycleState, RetentionHoldSnapshot, S3ObjectSnapshot,
    WebhookDeliveryLifecycleState, WebhookDeliverySnapshot,
};
use serde::Serialize;

/// Frozen operation identity used by the canonical evidence encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DurableOperationIdentityV1 {
    pub tenant: String,
    pub repository: String,
    pub operation_id: String,
    pub kind: String,
    pub object_key: Option<String>,
    pub content_sha256: Option<String>,
}

impl From<&OperationIdentity> for DurableOperationIdentityV1 {
    fn from(operation: &OperationIdentity) -> Self {
        Self {
            tenant: operation.tenant.clone(),
            repository: operation.repository.clone(),
            operation_id: operation.operation_id.clone(),
            kind: operation.kind.as_str().to_owned(),
            object_key: operation.object_key.clone(),
            content_sha256: operation.content_sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DurableS3ObjectStateV1 {
    file_id: String,
    size_bytes: u64,
    content_hash: String,
    etag: String,
    user_metadata: Vec<(String, String)>,
    updated_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum DurableSnapshotV1 {
    Digest {
        operation: DurableOperationIdentityV1,
        state_digest: String,
    },
    HubRef {
        repository: String,
        ref_name: String,
        head_sha: Option<String>,
    },
    OciTag {
        scope_namespace: String,
        repository: String,
        tag: String,
        digest_hex: Option<String>,
    },
    OciObject {
        scope_namespace: String,
        repository: String,
        object_kind: String,
        digest_hex: String,
        state: String,
        deleted_at_unix_seconds: Option<u64>,
    },
    S3Object {
        scope_namespace: String,
        object_key: String,
        entry: Option<DurableS3ObjectStateV1>,
    },
    Provider {
        provider: String,
        owner: String,
        repo: String,
        last_access_changed_at_unix_seconds: Option<u64>,
        last_revision_pushed_at_unix_seconds: Option<u64>,
        last_pushed_revision: Option<String>,
        last_cache_invalidated_at_unix_seconds: Option<u64>,
        last_authorization_rechecked_at_unix_seconds: Option<u64>,
        last_drift_checked_at_unix_seconds: Option<u64>,
    },
    Quarantine {
        object_key: String,
        observed_length: u64,
        first_seen_unreachable_at_unix_seconds: u64,
        delete_after_unix_seconds: u64,
        state: String,
    },
    RetentionHold {
        object_key: String,
        reason: String,
        held_at_unix_seconds: u64,
        release_after_unix_seconds: Option<u64>,
        state: String,
    },
    WebhookDelivery {
        provider: String,
        owner: String,
        repo: String,
        delivery_id: String,
        processed_at_unix_seconds: u64,
        state: String,
    },
}

pub trait DurableSnapshotV1Encoding {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1;
}

impl DurableSnapshotV1Encoding for DigestSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::Digest {
            operation: (&self.operation).into(),
            state_digest: self.state_digest.as_str().to_owned(),
        }
    }
}

impl DurableSnapshotV1Encoding for HubRefSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::HubRef {
            repository: self.repository.clone(),
            ref_name: self.ref_name.clone(),
            head_sha: self.head_sha.clone(),
        }
    }
}

impl DurableSnapshotV1Encoding for OciTagSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::OciTag {
            scope_namespace: self.scope_namespace.clone(),
            repository: self.repository.clone(),
            tag: self.tag.clone(),
            digest_hex: self.digest_hex.clone(),
        }
    }
}

impl DurableSnapshotV1Encoding for OciObjectSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::OciObject {
            scope_namespace: self.identity.scope_namespace.clone(),
            repository: self.identity.repository.clone(),
            object_kind: self.identity.object_kind.clone(),
            digest_hex: self.identity.digest_hex.clone(),
            state: match self.state {
                OciObjectLifecycleState::Published => "Published",
                OciObjectLifecycleState::Deleted => "Deleted",
                OciObjectLifecycleState::Reclaimed => "Reclaimed",
            }
            .to_owned(),
            deleted_at_unix_seconds: self.deleted_at_unix_seconds,
        }
    }
}

impl DurableSnapshotV1Encoding for S3ObjectSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::S3Object {
            scope_namespace: self.scope_namespace.clone(),
            object_key: self.object_key.clone(),
            entry: self.entry.as_ref().map(|entry| DurableS3ObjectStateV1 {
                file_id: entry.file_id.clone(),
                size_bytes: entry.size_bytes,
                content_hash: entry.content_hash.clone(),
                etag: entry.etag.clone(),
                user_metadata: entry.user_metadata.clone(),
                updated_at_unix_seconds: entry.updated_at_unix_seconds,
            }),
        }
    }
}

impl DurableSnapshotV1Encoding for ProviderLifecycleSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::Provider {
            provider: self.provider.clone(),
            owner: self.owner.clone(),
            repo: self.repo.clone(),
            last_access_changed_at_unix_seconds: self.last_access_changed_at_unix_seconds,
            last_revision_pushed_at_unix_seconds: self.last_revision_pushed_at_unix_seconds,
            last_pushed_revision: self.last_pushed_revision.clone(),
            last_cache_invalidated_at_unix_seconds: self.last_cache_invalidated_at_unix_seconds,
            last_authorization_rechecked_at_unix_seconds: self
                .last_authorization_rechecked_at_unix_seconds,
            last_drift_checked_at_unix_seconds: self.last_drift_checked_at_unix_seconds,
        }
    }
}

impl DurableSnapshotV1Encoding for QuarantineSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::Quarantine {
            object_key: self.object_key.clone(),
            observed_length: self.observed_length,
            first_seen_unreachable_at_unix_seconds: self.first_seen_unreachable_at_unix_seconds,
            delete_after_unix_seconds: self.delete_after_unix_seconds,
            state: match self.state {
                QuarantineLifecycleState::Active => "Active",
                QuarantineLifecycleState::Released => "Released",
            }
            .to_owned(),
        }
    }
}

impl DurableSnapshotV1Encoding for RetentionHoldSnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::RetentionHold {
            object_key: self.object_key.clone(),
            reason: self.reason.clone(),
            held_at_unix_seconds: self.held_at_unix_seconds,
            release_after_unix_seconds: self.release_after_unix_seconds,
            state: match self.state {
                RetentionHoldLifecycleState::Active => "Active",
                RetentionHoldLifecycleState::Released => "Released",
            }
            .to_owned(),
        }
    }
}

impl DurableSnapshotV1Encoding for WebhookDeliverySnapshot {
    fn durable_snapshot_v1(&self) -> DurableSnapshotV1 {
        DurableSnapshotV1::WebhookDelivery {
            provider: self.provider.clone(),
            owner: self.owner.clone(),
            repo: self.repo.clone(),
            delivery_id: self.delivery_id.clone(),
            processed_at_unix_seconds: self.processed_at_unix_seconds,
            state: match self.state {
                WebhookDeliveryLifecycleState::Processed => "Processed",
                WebhookDeliveryLifecycleState::Released => "Released",
            }
            .to_owned(),
        }
    }
}

pub(crate) fn durable_operation_v1(operation: &OperationIdentity) -> DurableOperationIdentityV1 {
    operation.into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::OperationKind;

    #[test]
    fn operation_kind_encoding_is_name_based_and_stable() {
        let operation =
            OperationIdentity::new("tenant", "repo", "operation", OperationKind::S3Object).unwrap();
        let encoded = DurableOperationIdentityV1::from(&operation);
        assert_eq!(encoded.kind, "S3Object");
    }
}
