use penelope_domain::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::digest::{
    DigestEncoding, canonical_process_digest, canonical_state_digest, legacy_state_label_digest,
    process_digest,
};
use crate::states::EvidenceState;
use crate::{
    OperationIdentity, OperationKind, ReliabilityError, ResumableLifecycleState,
    UploadLifecycleState,
};

/// One canonical lifecycle evidence event, parameterized only by the owning
/// domain's typed state. Upload intents and resumable sessions are public type
/// aliases of this one implementation; they cannot acquire different digest
/// or chain semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvidenceEvent<S: EvidenceState> {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: S,
    pub after: S,
    /// Encoding used for the integrity digests. Missing on legacy JSON
    /// rows, which deserialize as [`DigestEncoding::LegacyJson`].
    #[serde(default)]
    pub digest_encoding: DigestEncoding,
    pub state_digest: statechronicle_core::digest::ContentDigest,
    pub process_digest: PenelopeDigest,
}

/// Evidence for resumable sessions.
pub type StateTransitionEvent = LifecycleEvidenceEvent<ResumableLifecycleState>;

/// Evidence for upload intents.
pub type LifecycleEvent = LifecycleEvidenceEvent<UploadLifecycleState>;

/// Builds the canonical identity for an upload lifecycle operation.
pub fn upload_operation_identity(
    tenant: impl Into<String>,
    repository: impl Into<String>,
    operation_id: impl Into<String>,
    object_key: impl Into<String>,
    content_sha256: impl Into<String>,
) -> Result<OperationIdentity, ReliabilityError> {
    Ok(
        OperationIdentity::new(tenant, repository, operation_id, OperationKind::Upload)?
            .with_object_key(object_key)
            .with_content_sha256(content_sha256),
    )
}

impl<S: EvidenceState> LifecycleEvidenceEvent<S> {
    pub fn new(
        operation: OperationIdentity,
        sequence: u64,
        before: S,
        after: S,
    ) -> Result<Self, ReliabilityError> {
        if !before.can_transition_to(after) {
            return Err(ReliabilityError::InvalidTransition {
                before: before.as_str(),
                after: after.as_str(),
            });
        }
        let digest_encoding = DigestEncoding::CanonicalBcsV1;
        let state_digest = canonical_state_digest(&after)?;
        let process_digest = canonical_process_digest(&operation, sequence, &before, &after)?;
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
        if !self.before.can_transition_to(self.after) {
            return Err(ReliabilityError::InvalidTransition {
                before: self.before.as_str(),
                after: self.after.as_str(),
            });
        }
        let expected_state = match self.digest_encoding {
            DigestEncoding::LegacyJson => legacy_state_label_digest(self.after.as_str()),
            DigestEncoding::CanonicalBcsV1 | DigestEncoding::CanonicalBcsV2 => {
                canonical_state_digest(&self.after)?
            }
        };
        if self.state_digest != expected_state {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        let expected_process = match self.digest_encoding {
            DigestEncoding::LegacyJson => process_digest(
                &self.operation,
                self.sequence,
                &self.before.as_str(),
                &self.after.as_str(),
                DigestEncoding::LegacyJson,
            )?,
            DigestEncoding::CanonicalBcsV1 | DigestEncoding::CanonicalBcsV2 => {
                canonical_process_digest(&self.operation, self.sequence, &self.before, &self.after)?
            }
        };
        if self.process_digest != expected_process {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

pub(crate) fn verify_evidence_chain<S: EvidenceState>(
    events: &[LifecycleEvidenceEvent<S>],
) -> Result<(), ReliabilityError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    let operation = &first.operation;
    let mut previous_after = None;
    let mut previous_sequence = None;
    for event in events {
        event.verify_integrity()?;
        if event.operation != *operation {
            return Err(ReliabilityError::OperationMismatch);
        }
        if let Some(sequence) = previous_sequence {
            if event.sequence <= sequence {
                return Err(ReliabilityError::SequenceRegression);
            }
            if sequence.checked_add(1) != Some(event.sequence) {
                return Err(ReliabilityError::ChainDiscontinuity);
            }
        }
        if let Some(previous_after) = previous_after
            && event.before != previous_after
        {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        previous_sequence = Some(event.sequence);
        previous_after = Some(event.after);
    }
    Ok(())
}

pub fn verify_state_transition_chain(
    events: &[StateTransitionEvent],
) -> Result<(), ReliabilityError> {
    verify_evidence_chain(events)
}

pub fn verify_state_transition_chain_ends_at(
    events: &[StateTransitionEvent],
    expected: ResumableLifecycleState,
) -> Result<(), ReliabilityError> {
    verify_evidence_chain(events)?;
    if events.last().is_some_and(|event| event.after == expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

pub fn upload_lifecycle_event(
    tenant: impl Into<String>,
    repository: impl Into<String>,
    operation_id: impl Into<String>,
    object_key: impl Into<String>,
    content_sha256: impl Into<String>,
    before: UploadLifecycleState,
    after: UploadLifecycleState,
) -> Result<LifecycleEvent, ReliabilityError> {
    let operation =
        upload_operation_identity(tenant, repository, operation_id, object_key, content_sha256)?;
    LifecycleEvent::new(operation, lifecycle_sequence(before, after), before, after)
}

pub fn resumable_session_event(
    scope_namespace: impl Into<String>,
    session_id: impl Into<String>,
    target_key: impl Into<String>,
    sequence: u64,
    before: ResumableLifecycleState,
    after: ResumableLifecycleState,
) -> Result<StateTransitionEvent, ReliabilityError> {
    let operation = OperationIdentity::new(
        "resumable-session",
        scope_namespace,
        session_id,
        OperationKind::ResumableSession,
    )?
    .with_object_key(target_key);
    StateTransitionEvent::new(operation, sequence, before, after)
}

impl LifecycleEvidenceEvent<UploadLifecycleState> {
    pub fn validate_for_transition(
        &self,
        operation_id: &str,
        before: UploadLifecycleState,
        after: UploadLifecycleState,
    ) -> Result<(), ReliabilityError> {
        if self.operation.kind != OperationKind::Upload
            || self.operation.operation_id != operation_id
            || self.before != before
            || self.after != after
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        self.verify_integrity()
    }
}

pub fn verify_lifecycle_chain(events: &[LifecycleEvent]) -> Result<(), ReliabilityError> {
    verify_evidence_chain(events)
}

pub fn verify_lifecycle_chain_ends_at(
    events: &[LifecycleEvent],
    expected: UploadLifecycleState,
) -> Result<(), ReliabilityError> {
    verify_lifecycle_chain(events)?;
    if events.last().is_some_and(|event| event.after == expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

/// Verifies an upload-intent journal against the immutable operation identity
/// and the state currently stored for that intent.
pub fn verify_upload_lifecycle_events(
    events: &[LifecycleEvent],
    tenant: &str,
    repository: &str,
    operation_id: &str,
    object_key: &str,
    content_sha256: &str,
    expected_state: UploadLifecycleState,
) -> Result<(), ReliabilityError> {
    verify_lifecycle_chain(events)?;
    let Some(first) = events.first() else {
        return Err(ReliabilityError::OperationMismatch);
    };
    if first.sequence != 0
        || first.before != UploadLifecycleState::Created
        || first.after != UploadLifecycleState::Created
    {
        return Err(ReliabilityError::ChainDiscontinuity);
    }
    let operation = &first.operation;
    if operation.kind != OperationKind::Upload
        || operation.tenant != tenant
        || operation.repository != repository
        || operation.operation_id != operation_id
        || operation.object_key.as_deref() != Some(object_key)
        || operation.content_sha256.as_deref() != Some(content_sha256)
    {
        return Err(ReliabilityError::OperationMismatch);
    }
    if events
        .last()
        .is_some_and(|event| event.after == expected_state)
    {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

/// Returns the canonical identity used by an upload evidence chain.
#[must_use]
pub fn upload_lifecycle_identity(events: &[LifecycleEvent]) -> (&str, &str) {
    events
        .first()
        .map(|event| {
            (
                event.operation.tenant.as_str(),
                event.operation.repository.as_str(),
            )
        })
        .unwrap_or(("shardline", "default"))
}

const fn lifecycle_sequence(before: UploadLifecycleState, after: UploadLifecycleState) -> u64 {
    match after {
        UploadLifecycleState::Created => 0,
        UploadLifecycleState::Storing => 1,
        UploadLifecycleState::Stored => 2,
        UploadLifecycleState::MetadataCommitted => 3,
        UploadLifecycleState::Visible => 4,
        UploadLifecycleState::Failed => match before {
            UploadLifecycleState::Created => 1,
            UploadLifecycleState::Storing => 2,
            UploadLifecycleState::Stored => 3,
            UploadLifecycleState::MetadataCommitted => 4,
            UploadLifecycleState::Visible | UploadLifecycleState::Failed => 0,
        },
    }
}

pub fn baseline_upload_lifecycle_events(
    tenant: impl Into<String>,
    repository: impl Into<String>,
    operation_id: impl Into<String>,
    object_key: impl Into<String>,
    content_sha256: impl Into<String>,
    final_state: UploadLifecycleState,
) -> Result<Vec<LifecycleEvent>, ReliabilityError> {
    let tenant = tenant.into();
    let repository = repository.into();
    let operation_id = operation_id.into();
    let object_key = object_key.into();
    let content_sha256 = content_sha256.into();
    let transitions: &[(UploadLifecycleState, UploadLifecycleState)] = match final_state {
        UploadLifecycleState::Created => {
            &[(UploadLifecycleState::Created, UploadLifecycleState::Created)]
        }
        UploadLifecycleState::Storing => &[
            (UploadLifecycleState::Created, UploadLifecycleState::Created),
            (UploadLifecycleState::Created, UploadLifecycleState::Storing),
        ],
        UploadLifecycleState::Stored => &[
            (UploadLifecycleState::Created, UploadLifecycleState::Created),
            (UploadLifecycleState::Created, UploadLifecycleState::Storing),
            (UploadLifecycleState::Storing, UploadLifecycleState::Stored),
        ],
        UploadLifecycleState::MetadataCommitted => &[
            (UploadLifecycleState::Created, UploadLifecycleState::Created),
            (UploadLifecycleState::Created, UploadLifecycleState::Storing),
            (UploadLifecycleState::Storing, UploadLifecycleState::Stored),
            (
                UploadLifecycleState::Stored,
                UploadLifecycleState::MetadataCommitted,
            ),
        ],
        UploadLifecycleState::Visible => &[
            (UploadLifecycleState::Created, UploadLifecycleState::Created),
            (UploadLifecycleState::Created, UploadLifecycleState::Storing),
            (UploadLifecycleState::Storing, UploadLifecycleState::Stored),
            (
                UploadLifecycleState::Stored,
                UploadLifecycleState::MetadataCommitted,
            ),
            (
                UploadLifecycleState::MetadataCommitted,
                UploadLifecycleState::Visible,
            ),
        ],
        UploadLifecycleState::Failed => &[
            (UploadLifecycleState::Created, UploadLifecycleState::Created),
            (UploadLifecycleState::Created, UploadLifecycleState::Failed),
        ],
    };
    transitions
        .iter()
        .map(|(before, after)| {
            upload_lifecycle_event(
                tenant.clone(),
                repository.clone(),
                operation_id.clone(),
                object_key.clone(),
                content_sha256.clone(),
                *before,
                *after,
            )
        })
        .collect()
}

pub fn baseline_resumable_session_events(
    scope_namespace: impl Into<String>,
    session_id: impl Into<String>,
    target_key: impl Into<String>,
    final_state: ResumableLifecycleState,
) -> Result<Vec<StateTransitionEvent>, ReliabilityError> {
    let scope_namespace = scope_namespace.into();
    let session_id = session_id.into();
    let target_key = target_key.into();
    let transitions: &[(ResumableLifecycleState, ResumableLifecycleState)] = match final_state {
        ResumableLifecycleState::Active => &[(
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )],
        ResumableLifecycleState::Completing => &[
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Active,
            ),
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Completing,
            ),
        ],
        ResumableLifecycleState::Completed => &[
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Active,
            ),
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Completing,
            ),
            (
                ResumableLifecycleState::Completing,
                ResumableLifecycleState::Completed,
            ),
        ],
        ResumableLifecycleState::Aborted => &[
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Active,
            ),
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Aborted,
            ),
        ],
        ResumableLifecycleState::Expired => &[
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Active,
            ),
            (
                ResumableLifecycleState::Active,
                ResumableLifecycleState::Expired,
            ),
        ],
    };
    transitions
        .iter()
        .enumerate()
        .map(|(sequence, (before, after))| {
            resumable_session_event(
                scope_namespace.clone(),
                session_id.clone(),
                target_key.clone(),
                u64::try_from(sequence).unwrap_or(u64::MAX),
                *before,
                *after,
            )
        })
        .collect()
}
