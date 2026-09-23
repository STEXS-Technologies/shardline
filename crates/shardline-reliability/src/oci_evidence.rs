use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::digest::{
    DigestEncoding, canonical_snapshot_digest, canonical_transition_process_digest, process_digest,
    state_digest,
};
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

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        let operation_id = format!(
            "{}:{}:{}:{}",
            self.identity.scope_namespace,
            self.identity.repository,
            self.identity.object_kind,
            self.identity.digest_hex
        );
        Ok(OperationIdentity::new(
            format!("oci:{}", self.identity.scope_namespace),
            self.identity.repository.clone(),
            operation_id,
            OperationKind::Visibility,
        )?
        .with_object_key(self.identity.digest_hex.clone()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OciObjectLifecycleEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: OciObjectSnapshot,
    pub after: OciObjectSnapshot,
    /// Encoding used for the authenticated digests. Missing on legacy JSON
    /// rows, which deserialize as [`DigestEncoding::LegacyJson`].
    #[serde(default)]
    pub digest_encoding: DigestEncoding,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl OciObjectLifecycleEvent {
    pub fn new(
        sequence: u64,
        before: OciObjectSnapshot,
        after: OciObjectSnapshot,
    ) -> Result<Self, ReliabilityError> {
        if before.identity != after.identity {
            return Err(ReliabilityError::OperationMismatch);
        }
        if !before.state.can_transition_to(after.state) {
            return Err(ReliabilityError::InvalidTransition {
                before: before.state.as_str(),
                after: after.state.as_str(),
            });
        }
        let operation = after.operation()?;
        let digest_encoding = DigestEncoding::CanonicalBcsV1;
        let state_digest = canonical_snapshot_digest(&after)?;
        let process_digest =
            canonical_transition_process_digest(&operation, sequence, &before, &after)?;
        Ok(Self {
            operation,
            sequence,
            before,
            after,
            digest_encoding,
            state_digest,
            process_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        if self.operation != self.after.operation()? {
            return Err(ReliabilityError::OperationMismatch);
        }
        if self.state_digest != state_digest(&self.after, self.digest_encoding)? {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest
            != process_digest(
                &self.operation,
                self.sequence,
                &self.before,
                &self.after,
                self.digest_encoding,
            )?
        {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

pub fn verify_oci_object_lifecycle_chain(
    events: &[OciObjectLifecycleEvent],
) -> Result<(), ReliabilityError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    let mut previous_after = None;
    let mut previous_sequence = None;
    for event in events {
        event.verify_integrity()?;
        if event.operation != first.operation {
            return Err(ReliabilityError::OperationMismatch);
        }
        if previous_sequence.is_some_and(|sequence| event.sequence <= sequence) {
            return Err(ReliabilityError::SequenceRegression);
        }
        if previous_after.is_some_and(|after| event.before != after) {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        previous_sequence = Some(event.sequence);
        previous_after = Some(event.after.clone());
    }
    Ok(())
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OciObjectEvidenceLog(Vec<OciObjectLifecycleEvent>);

impl OciObjectEvidenceLog {
    pub fn from_events(events: Vec<OciObjectLifecycleEvent>) -> Result<Self, ReliabilityError> {
        verify_oci_object_lifecycle_chain(&events)?;
        Ok(Self(events))
    }

    pub fn baseline(snapshot: OciObjectSnapshot) -> Result<Self, ReliabilityError> {
        Ok(Self(vec![OciObjectLifecycleEvent::new(
            0,
            snapshot.clone(),
            snapshot,
        )?]))
    }

    pub fn record(&mut self, snapshot: OciObjectSnapshot) -> Result<(), ReliabilityError> {
        let before = self
            .0
            .last()
            .map(|event| event.after.clone())
            .unwrap_or_else(|| snapshot.clone());
        let sequence = self
            .0
            .last()
            .map_or(0, |event| event.sequence.saturating_add(1));
        self.0
            .push(OciObjectLifecycleEvent::new(sequence, before, snapshot)?);
        verify_oci_object_lifecycle_chain(&self.0)
    }

    pub fn verify_for(&self, expected: &OciObjectSnapshot) -> Result<(), ReliabilityError> {
        verify_oci_object_lifecycle_events(&self.0, expected)
    }

    #[must_use]
    pub fn events(&self) -> &[OciObjectLifecycleEvent] {
        &self.0
    }
}

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
}
