use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::digest::{canonical_process_digest, canonical_state_digest};
use crate::{
    OperationIdentity, OperationKind, ReliabilityError, ResumableLifecycleState,
    UploadLifecycleState,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateTransitionEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: ResumableLifecycleState,
    pub after: ResumableLifecycleState,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl StateTransitionEvent {
    pub fn new(
        operation: OperationIdentity,
        sequence: u64,
        before: ResumableLifecycleState,
        after: ResumableLifecycleState,
    ) -> Result<Self, ReliabilityError> {
        if !before.can_transition_to(after) {
            return Err(ReliabilityError::InvalidTransition {
                before: before.as_str(),
                after: after.as_str(),
            });
        }
        let state_digest = canonical_state_digest(after.as_str());
        let process_digest =
            canonical_process_digest(&operation, sequence, before.as_str(), after.as_str())?;
        Ok(Self {
            operation,
            sequence,
            before,
            after,
            state_digest,
            process_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        let expected_state = canonical_state_digest(self.after.as_str());
        if self.state_digest != expected_state {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest
            != canonical_process_digest(
                &self.operation,
                self.sequence,
                self.before.as_str(),
                self.after.as_str(),
            )?
        {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

pub fn verify_state_transition_chain(
    events: &[StateTransitionEvent],
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
        if previous_sequence.is_some_and(|sequence| event.sequence <= sequence) {
            return Err(ReliabilityError::SequenceRegression);
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: UploadLifecycleState,
    pub after: UploadLifecycleState,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
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
        OperationIdentity::new(tenant, repository, operation_id, OperationKind::Upload)?
            .with_object_key(object_key)
            .with_content_sha256(content_sha256);
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

impl LifecycleEvent {
    pub fn new(
        operation: OperationIdentity,
        sequence: u64,
        before: UploadLifecycleState,
        after: UploadLifecycleState,
    ) -> Result<Self, ReliabilityError> {
        if !before.can_transition_to(after) {
            return Err(ReliabilityError::InvalidTransition {
                before: before.as_str(),
                after: after.as_str(),
            });
        }
        let state_digest = canonical_state_digest(after.as_str());
        let process_digest =
            canonical_process_digest(&operation, sequence, before.as_str(), after.as_str())?;
        Ok(Self {
            operation,
            sequence,
            before,
            after,
            state_digest,
            process_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        let expected = Self::new(
            self.operation.clone(),
            self.sequence,
            self.before,
            self.after,
        )?;
        if self.state_digest != expected.state_digest {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest != expected.process_digest {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }

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
    let Some(first) = events.first() else {
        return Ok(());
    };
    let operation = &first.operation;
    let mut previous = first.before;
    let mut previous_sequence = None;
    for event in events {
        if &event.operation != operation {
            return Err(ReliabilityError::OperationMismatch);
        }
        if previous_sequence.is_some_and(|sequence| event.sequence <= sequence) {
            return Err(ReliabilityError::SequenceRegression);
        }
        if event.before != previous {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        event.verify_integrity()?;
        previous = event.after;
        previous_sequence = Some(event.sequence);
    }
    Ok(())
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
