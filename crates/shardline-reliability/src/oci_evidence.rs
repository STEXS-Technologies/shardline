use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OciObjectLifecycleState {
    Published,
    Deleted,
    Reclaimed,
}

impl OciObjectLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "Published",
            Self::Deleted => "Deleted",
            Self::Reclaimed => "Reclaimed",
        }
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Published, Self::Published | Self::Deleted)
                | (
                    Self::Deleted,
                    Self::Deleted | Self::Published | Self::Reclaimed
                )
                | (
                    Self::Reclaimed,
                    Self::Reclaimed | Self::Published | Self::Deleted
                )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OciObjectIdentity {
    pub scope_namespace: String,
    pub repository: String,
    pub object_kind: String,
    pub digest_hex: String,
}

/// Typed persisted operation identifier for one OCI object lifecycle.
///
/// The textual representation is stable because it is part of the existing
/// reliability journal key. Keeping construction here prevents storage
/// backends and repair paths from drifting apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciObjectOperationId(String);

impl OciObjectOperationId {
    /// Creates the stable OCI visibility operation identifier.
    #[must_use]
    pub fn new(identity: &OciObjectIdentity) -> Self {
        Self(format!(
            "{}:{}:{}:{}",
            identity.scope_namespace,
            identity.repository,
            identity.object_kind,
            identity.digest_hex
        ))
    }

    /// Returns the persisted operation identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the typed identifier into its persisted representation.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl OciObjectIdentity {
    pub fn new(
        scope_namespace: impl Into<String>,
        repository: impl Into<String>,
        object_kind: impl Into<String>,
        digest_hex: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        let identity = Self {
            scope_namespace: scope_namespace.into(),
            repository: repository.into(),
            object_kind: object_kind.into(),
            digest_hex: digest_hex.into(),
        };
        for (field, value) in [
            ("scope_namespace", identity.scope_namespace.as_str()),
            ("repository", identity.repository.as_str()),
            ("object_kind", identity.object_kind.as_str()),
            ("digest_hex", identity.digest_hex.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        Ok(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OciObjectSnapshot {
    pub identity: OciObjectIdentity,
    pub state: OciObjectLifecycleState,
    pub deleted_at_unix_seconds: Option<u64>,
}

impl OciObjectSnapshot {
    pub fn new(
        identity: OciObjectIdentity,
        state: OciObjectLifecycleState,
        deleted_at_unix_seconds: Option<u64>,
    ) -> Result<Self, ReliabilityError> {
        if matches!(
            state,
            OciObjectLifecycleState::Deleted | OciObjectLifecycleState::Reclaimed
        ) && deleted_at_unix_seconds.is_none()
        {
            return Err(ReliabilityError::EmptyField("deleted_at_unix_seconds"));
        }
        if matches!(state, OciObjectLifecycleState::Published) && deleted_at_unix_seconds.is_some()
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(Self {
            identity,
            state,
            deleted_at_unix_seconds,
        })
    }

    pub fn operation_id(&self) -> OciObjectOperationId {
        OciObjectOperationId::new(&self.identity)
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            format!("oci:{}", self.identity.scope_namespace),
            self.identity.repository.clone(),
            self.operation_id().into_string(),
            OperationKind::Visibility,
        )?
        .with_object_key(self.identity.digest_hex.clone()))
    }
}

impl SnapshotEvidence for OciObjectSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.identity != after.identity {
            return Err(ReliabilityError::OperationMismatch);
        }
        if !self.state.can_transition_to(after.state) {
            return Err(ReliabilityError::InvalidTransition {
                before: self.state.as_str(),
                after: after.state.as_str(),
            });
        }
        Ok(())
    }
}

pub type OciObjectLifecycleEvent = SnapshotEvidenceEvent<OciObjectSnapshot>;

pub fn verify_oci_object_lifecycle_chain(
    events: &[OciObjectLifecycleEvent],
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)
}

pub fn verify_oci_object_lifecycle_events(
    events: &[OciObjectLifecycleEvent],
    expected: &OciObjectSnapshot,
) -> Result<(), ReliabilityError> {
    verify_oci_object_lifecycle_chain(events)?;
    if events.last().is_some_and(|event| event.after == *expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

pub type OciObjectEvidenceLog = SnapshotEvidenceLog<OciObjectSnapshot>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn snapshot(state: OciObjectLifecycleState, deleted_at: Option<u64>) -> OciObjectSnapshot {
        OciObjectSnapshot::new(
            OciObjectIdentity::new("global", "team/assets", "blob", "a".repeat(64)).unwrap(),
            state,
            deleted_at,
        )
        .unwrap()
    }

    #[test]
    fn evidence_models_delete_reclaim_and_republish() {
        let mut log =
            OciObjectEvidenceLog::baseline(snapshot(OciObjectLifecycleState::Published, None))
                .unwrap();
        log.record(snapshot(OciObjectLifecycleState::Deleted, Some(10)))
            .unwrap();
        log.record(snapshot(OciObjectLifecycleState::Reclaimed, Some(10)))
            .unwrap();
        log.record(snapshot(OciObjectLifecycleState::Published, None))
            .unwrap();
        log.verify_for(&snapshot(OciObjectLifecycleState::Published, None))
            .unwrap();
    }

    #[test]
    fn tampered_generation_is_rejected() {
        let mut log =
            OciObjectEvidenceLog::baseline(snapshot(OciObjectLifecycleState::Published, None))
                .unwrap();
        log.record(snapshot(OciObjectLifecycleState::Deleted, Some(10)))
            .unwrap();
        let mut events = log.events().to_vec();
        events.get_mut(1).unwrap().after.deleted_at_unix_seconds = Some(11);
        assert!(matches!(
            verify_oci_object_lifecycle_chain(&events),
            Err(ReliabilityError::StateDigestMismatch)
        ));
    }

    #[test]
    fn operation_id_preserves_the_persisted_identity_format() {
        let object = snapshot(OciObjectLifecycleState::Published, None);
        assert_eq!(
            object.operation_id().as_str(),
            "global:team/assets:blob:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            object.evidence_operation().unwrap().operation_id,
            object.operation_id().as_str()
        );
    }
}
