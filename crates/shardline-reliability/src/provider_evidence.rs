use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::digest::{
    DigestEncoding, canonical_snapshot_digest, canonical_transition_process_digest, process_digest,
    state_digest,
};
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Canonical materialized snapshot for one provider repository lifecycle.
///
/// This type deliberately contains the complete durable row, rather than an
/// event-specific subset, so readers can verify that the journal and the
/// materialized provider state agree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRepositoryIdentity {
    pub provider: String,
    pub owner: String,
    pub repo: String,
}

impl ProviderRepositoryIdentity {
    /// Creates a provider repository identity.
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        owner: impl Into<String>,
        repo: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            owner: owner.into(),
            repo: repo.into(),
        }
    }
}

/// Monotonic observations stored for one provider repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderLifecycleObservations {
    pub last_access_changed_at_unix_seconds: Option<u64>,
    pub last_revision_pushed_at_unix_seconds: Option<u64>,
    pub last_pushed_revision: Option<String>,
    pub last_cache_invalidated_at_unix_seconds: Option<u64>,
    pub last_authorization_rechecked_at_unix_seconds: Option<u64>,
    pub last_drift_checked_at_unix_seconds: Option<u64>,
}

impl ProviderLifecycleObservations {
    /// Creates a provider lifecycle observation set.
    #[must_use]
    pub const fn new(
        last_access_changed_at_unix_seconds: Option<u64>,
        last_revision_pushed_at_unix_seconds: Option<u64>,
        last_pushed_revision: Option<String>,
        last_cache_invalidated_at_unix_seconds: Option<u64>,
        last_authorization_rechecked_at_unix_seconds: Option<u64>,
        last_drift_checked_at_unix_seconds: Option<u64>,
    ) -> Self {
        Self {
            last_access_changed_at_unix_seconds,
            last_revision_pushed_at_unix_seconds,
            last_pushed_revision,
            last_cache_invalidated_at_unix_seconds,
            last_authorization_rechecked_at_unix_seconds,
            last_drift_checked_at_unix_seconds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderLifecycleSnapshot {
    pub provider: String,
    pub owner: String,
    pub repo: String,
    pub last_access_changed_at_unix_seconds: Option<u64>,
    pub last_revision_pushed_at_unix_seconds: Option<u64>,
    pub last_pushed_revision: Option<String>,
    pub last_cache_invalidated_at_unix_seconds: Option<u64>,
    pub last_authorization_rechecked_at_unix_seconds: Option<u64>,
    pub last_drift_checked_at_unix_seconds: Option<u64>,
}

impl ProviderLifecycleSnapshot {
    /// Creates a canonical provider lifecycle snapshot from typed components.
    pub fn from_parts(
        identity: ProviderRepositoryIdentity,
        observations: ProviderLifecycleObservations,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            provider: identity.provider,
            owner: identity.owner,
            repo: identity.repo,
            last_access_changed_at_unix_seconds: observations.last_access_changed_at_unix_seconds,
            last_revision_pushed_at_unix_seconds: observations.last_revision_pushed_at_unix_seconds,
            last_pushed_revision: observations.last_pushed_revision,
            last_cache_invalidated_at_unix_seconds: observations
                .last_cache_invalidated_at_unix_seconds,
            last_authorization_rechecked_at_unix_seconds: observations
                .last_authorization_rechecked_at_unix_seconds,
            last_drift_checked_at_unix_seconds: observations.last_drift_checked_at_unix_seconds,
        };
        for (field, value) in [
            ("provider", snapshot.provider.as_str()),
            ("owner", snapshot.owner.as_str()),
            ("repo", snapshot.repo.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        Ok(snapshot)
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        OperationIdentity::new(
            format!("provider:{}", self.provider),
            format!("{}/{}", self.owner, self.repo),
            format!("{}:{}:{}", self.provider, self.owner, self.repo),
            OperationKind::ProviderEvent,
        )
    }
}

/// Tamper-evident provider lifecycle snapshot transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderLifecycleEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: ProviderLifecycleSnapshot,
    pub after: ProviderLifecycleSnapshot,
    /// Encoding used for the authenticated digests. Missing on legacy JSON
    /// rows, which deserialize as [`DigestEncoding::LegacyJson`].
    #[serde(default)]
    pub digest_encoding: DigestEncoding,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl ProviderLifecycleEvent {
    /// Creates one canonical provider lifecycle transition.
    pub fn new(
        sequence: u64,
        before: ProviderLifecycleSnapshot,
        after: ProviderLifecycleSnapshot,
    ) -> Result<Self, ReliabilityError> {
        if before.provider != after.provider
            || before.owner != after.owner
            || before.repo != after.repo
        {
            return Err(ReliabilityError::OperationMismatch);
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

    /// Verifies both canonical digests and immutable operation identity.
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

/// Verifies one complete provider lifecycle evidence chain.
pub fn verify_provider_lifecycle_chain(
    events: &[ProviderLifecycleEvent],
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

/// Verifies provider evidence against the materialized repository identity and state.
pub fn verify_provider_lifecycle_events(
    events: &[ProviderLifecycleEvent],
    expected: &ProviderLifecycleSnapshot,
) -> Result<(), ReliabilityError> {
    verify_provider_lifecycle_chain(events)?;
    let Some(last) = events.last() else {
        return Err(ReliabilityError::OperationMismatch);
    };
    if last.after != *expected {
        return Err(ReliabilityError::StateMismatch);
    }
    Ok(())
}

/// Evidence log for one provider repository lifecycle row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderEvidenceLog(Vec<ProviderLifecycleEvent>);

impl ProviderEvidenceLog {
    /// Wraps persisted events after validating their complete chain.
    pub fn from_events(events: Vec<ProviderLifecycleEvent>) -> Result<Self, ReliabilityError> {
        verify_provider_lifecycle_chain(&events)?;
        Ok(Self(events))
    }

    /// Creates a legacy-compatible baseline for an existing materialized row.
    pub fn baseline(snapshot: ProviderLifecycleSnapshot) -> Result<Self, ReliabilityError> {
        Ok(Self(vec![ProviderLifecycleEvent::new(
            0,
            snapshot.clone(),
            snapshot,
        )?]))
    }

    /// Appends a snapshot after validating the complete chain.
    pub fn record(&mut self, snapshot: ProviderLifecycleSnapshot) -> Result<(), ReliabilityError> {
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
            .push(ProviderLifecycleEvent::new(sequence, before, snapshot)?);
        verify_provider_lifecycle_chain(&self.0)
    }

    /// Verifies all evidence against one materialized row.
    pub fn verify_for(&self, expected: &ProviderLifecycleSnapshot) -> Result<(), ReliabilityError> {
        verify_provider_lifecycle_events(&self.0, expected)
    }

    /// Returns the ordered provider lifecycle events.
    #[must_use]
    pub fn events(&self) -> &[ProviderLifecycleEvent] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) const fn events_mut(&mut self) -> &mut Vec<ProviderLifecycleEvent> {
        &mut self.0
    }
}
