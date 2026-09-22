use serde::{Deserialize, Serialize};

use crate::{
    ReliabilityError, ResumableLifecycleState, StateTransitionEvent, resumable_session_event,
    verify_state_transition_chain,
};

/// Verifies a persisted resumable-session journal against its canonical
/// identity and current durable state.
pub fn verify_resumable_session_events(
    events: &[StateTransitionEvent],
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
    expected_state: ResumableLifecycleState,
) -> Result<(), ReliabilityError> {
    verify_state_transition_chain(events)?;
    let Some(first) = events.first() else {
        return Err(ReliabilityError::OperationMismatch);
    };
    let operation = &first.operation;
    if operation.kind != crate::OperationKind::ResumableSession
        || operation.tenant != "resumable-session"
        || operation.repository != scope_namespace
        || operation.operation_id != session_id
        || operation.object_key.as_deref() != Some(target_key)
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

/// Canonical evidence log for a file-backed resumable session.
///
/// The log is deliberately a newtype so adapters cannot construct or interpret
/// a parallel digest format. Legacy session files may start empty; callers must
/// use [`Self::for_legacy_session`] before accepting or mutating such state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEvidenceLog(Vec<StateTransitionEvent>);

impl SessionEvidenceLog {
    /// Creates the initial active evidence for a newly created session.
    pub fn new(
        scope_namespace: impl Into<String>,
        session_id: impl Into<String>,
        target_key: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        let scope_namespace = scope_namespace.into();
        let session_id = session_id.into();
        let target_key = target_key.into();
        let event = resumable_session_event(
            scope_namespace,
            session_id,
            target_key,
            1,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )?;
        Ok(Self(vec![event]))
    }

    /// Reconstructs the initial active evidence for a pre-evidence session.
    pub fn for_legacy_session(
        scope_namespace: impl Into<String>,
        session_id: impl Into<String>,
        target_key: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        Self::new(scope_namespace, session_id, target_key)
    }

    /// Appends one canonical evidence boundary and verifies the complete chain.
    pub fn record(
        &mut self,
        scope_namespace: impl Into<String>,
        session_id: impl Into<String>,
        target_key: impl Into<String>,
        before: ResumableLifecycleState,
        after: ResumableLifecycleState,
    ) -> Result<(), ReliabilityError> {
        let sequence = self
            .0
            .last()
            .map_or(1, |event| event.sequence.saturating_add(1));
        let event = resumable_session_event(
            scope_namespace,
            session_id,
            target_key,
            sequence,
            before,
            after,
        )?;
        self.0.push(event);
        verify_state_transition_chain(&self.0)
    }

    /// Verifies all stored digests, identity, ordering, and chain continuity.
    pub fn verify(&self) -> Result<(), ReliabilityError> {
        verify_state_transition_chain(&self.0)
    }

    /// Verifies integrity and binds the chain to one canonical session
    /// identity. Adapters use this instead of interpreting operation fields
    /// independently, so a valid chain cannot be replayed for another
    /// session, scope, or target.
    pub fn verify_for(
        &self,
        scope_namespace: &str,
        session_id: &str,
        target_key: &str,
    ) -> Result<(), ReliabilityError> {
        verify_resumable_session_events(
            &self.0,
            scope_namespace,
            session_id,
            target_key,
            self.0
                .last()
                .map_or(ResumableLifecycleState::Active, |event| event.after),
        )
    }

    /// Returns the evidence events in sequence order.
    #[must_use]
    pub fn events(&self) -> &[StateTransitionEvent] {
        &self.0
    }

    /// Returns whether no evidence has been recorded.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn events_mut(&mut self) -> &mut [StateTransitionEvent] {
        &mut self.0
    }
}
