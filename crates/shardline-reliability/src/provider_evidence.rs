use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Typed operation identifier for one provider repository lifecycle row.
///
/// The textual representation is intentionally kept identical to the
/// pre-existing persisted key so this hardening change does not alter lookup
/// or retry behavior. Centralizing construction prevents adapters and repair
/// code from drifting into different identity formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRepositoryOperationId(String);

impl ProviderRepositoryOperationId {
    /// Creates the stable provider lifecycle operation identifier.
    #[must_use]
    pub fn new(provider: &str, owner: &str, repo: &str) -> Self {
        Self(format!("{provider}:{owner}:{repo}"))
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
            ProviderRepositoryOperationId::new(&self.provider, &self.owner, &self.repo)
                .into_string(),
            OperationKind::ProviderEvent,
        )
    }
}

impl SnapshotEvidence for ProviderLifecycleSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.provider != after.provider || self.owner != after.owner || self.repo != after.repo {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

/// Tamper-evident provider lifecycle snapshot transition.
pub type ProviderLifecycleEvent = SnapshotEvidenceEvent<ProviderLifecycleSnapshot>;

/// Verifies one complete provider lifecycle evidence chain.
pub fn verify_provider_lifecycle_chain(
    events: &[ProviderLifecycleEvent],
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)
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
pub type ProviderEvidenceLog = SnapshotEvidenceLog<ProviderLifecycleSnapshot>;
