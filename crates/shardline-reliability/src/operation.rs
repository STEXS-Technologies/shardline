use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};
use statechronicle_core::digest::ContentDigest;

use crate::ReliabilityError;

/// Durable Shardline operation categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    Upload,
    ResumableSession,
    MetadataCommit,
    OciTag,
    S3Object,
    Visibility,
    ProviderEvent,
    Repair,
    GarbageCollection,
    RetentionHold,
    WebhookDelivery,
}

/// Protocol-specific namespace for materialized resumable-session snapshots.
///
/// These namespaces are persisted in existing snapshot evidence. Keeping the
/// mapping typed centralizes construction without changing stored keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumableSessionSnapshotDomain {
    OciUpload,
    S3Multipart,
    LfsPatch,
}

impl ResumableSessionSnapshotDomain {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OciUpload => "oci-upload-session",
            Self::S3Multipart => "s3-multipart-session",
            Self::LfsPatch => "lfs-patch-session",
        }
    }
}

impl OperationKind {
    /// Parses the stable persisted operation discriminator.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Upload" => Some(Self::Upload),
            "ResumableSession" => Some(Self::ResumableSession),
            "MetadataCommit" => Some(Self::MetadataCommit),
            "OciTag" => Some(Self::OciTag),
            "S3Object" => Some(Self::S3Object),
            "Visibility" => Some(Self::Visibility),
            "ProviderEvent" => Some(Self::ProviderEvent),
            "Repair" => Some(Self::Repair),
            "GarbageCollection" => Some(Self::GarbageCollection),
            "RetentionHold" => Some(Self::RetentionHold),
            "WebhookDelivery" => Some(Self::WebhookDelivery),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "Upload",
            Self::ResumableSession => "ResumableSession",
            Self::MetadataCommit => "MetadataCommit",
            Self::OciTag => "OciTag",
            Self::S3Object => "S3Object",
            Self::Visibility => "Visibility",
            Self::ProviderEvent => "ProviderEvent",
            Self::Repair => "Repair",
            Self::GarbageCollection => "GarbageCollection",
            Self::RetentionHold => "RetentionHold",
            Self::WebhookDelivery => "WebhookDelivery",
        }
    }
}

/// Stable identity for one Shardline operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIdentity {
    pub tenant: String,
    pub repository: String,
    pub operation_id: String,
    pub kind: OperationKind,
    pub object_key: Option<String>,
    pub content_sha256: Option<String>,
}

impl OperationIdentity {
    pub fn new(
        tenant: impl Into<String>,
        repository: impl Into<String>,
        operation_id: impl Into<String>,
        kind: OperationKind,
    ) -> Result<Self, ReliabilityError> {
        let identity = Self {
            tenant: tenant.into(),
            repository: repository.into(),
            operation_id: operation_id.into(),
            kind,
            object_key: None,
            content_sha256: None,
        };
        identity.validate()?;
        Ok(identity)
    }

    #[must_use]
    pub fn with_object_key(mut self, object_key: impl Into<String>) -> Self {
        self.object_key = Some(object_key.into());
        self
    }

    #[must_use]
    pub fn with_content_sha256(mut self, content_sha256: impl Into<String>) -> Self {
        self.content_sha256 = Some(content_sha256.into());
        self
    }

    pub fn content_digest(&self) -> Result<ContentDigest, ReliabilityError> {
        self.validate()?;
        statechronicle_core::canonicalize::canonicalize_and_digest(self)
            .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))
    }

    pub fn penelope_digest(&self) -> Result<PenelopeDigest, ReliabilityError> {
        self.validate()?;
        let bytes = statechronicle_core::canonicalize::canonicalize(self)
            .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))?;
        Ok(PenelopeDigest::sha256(&bytes))
    }

    fn validate(&self) -> Result<(), ReliabilityError> {
        for (field, value) in [
            ("tenant", self.tenant.as_str()),
            ("repository", self.repository.as_str()),
            ("operation_id", self.operation_id.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        if let Some(digest) = &self.content_sha256
            && (digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        {
            return Err(ReliabilityError::InvalidSha256);
        }
        Ok(())
    }
}

/// Builds a protocol snapshot identity while preserving its established
/// persisted namespace and operation shape.
pub fn resumable_session_snapshot_identity(
    domain: ResumableSessionSnapshotDomain,
    scope_namespace: impl Into<String>,
    session_id: impl Into<String>,
    target_key: impl Into<String>,
) -> Result<OperationIdentity, ReliabilityError> {
    Ok(OperationIdentity::new(
        domain.as_str(),
        scope_namespace,
        session_id,
        OperationKind::ResumableSession,
    )?
    .with_object_key(target_key))
}
