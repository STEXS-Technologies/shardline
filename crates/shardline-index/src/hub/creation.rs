//! Optional shared policy for repository publication, separate from token scope.
use serde::{Deserialize, Serialize};

use super::HubRepo;

/// Verified identity of a repository creator. Never contains a bearer token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubCreationActor {
    /// Verified token issuer.
    pub issuer: String,
    /// Verified token subject.
    pub subject: String,
}

/// Optional namespace limits applied atomically with repository publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HubCreationPolicy {
    /// Whether otherwise authorized callers may create repositories.
    pub enabled: bool,
    /// Maximum currently retained repositories, across repository types.
    pub max_repositories: Option<u64>,
    /// Maximum successful creations during the trailing window, including deletes.
    pub max_creations: Option<u64>,
    /// Trailing rate window, from one second to one day.
    pub window_seconds: u32,
    /// Identities denied creation, including previously issued valid tokens.
    pub denied_creators: Vec<HubCreationActor>,
}

impl Default for HubCreationPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_repositories: None,
            max_creations: None,
            window_seconds: 3600,
            denied_creators: Vec::new(),
        }
    }
}

/// Invalid owner-supplied repository creation policy.
#[derive(Debug, thiserror::Error)]
#[error("invalid repository creation policy: {0}")]
pub struct HubCreationPolicyError(&'static str);

impl From<HubCreationPolicyError> for crate::LocalIndexStoreError {
    fn from(error: HubCreationPolicyError) -> Self {
        Self::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }
}

impl From<HubCreationPolicyError> for crate::PostgresMetadataStoreError {
    fn from(error: HubCreationPolicyError) -> Self {
        Self::Unsupported(error.to_string())
    }
}

impl HubCreationPolicy {
    /// Validates bounded policy input before any metadata mutation.
    /// # Errors
    /// Rejects invalid namespaces, unsupported limits and excessive actor lists.
    pub fn validate(&self, namespace: &str) -> Result<(), HubCreationPolicyError> {
        if namespace.is_empty()
            || namespace.len() > 255
            || namespace.chars().any(|c| c.is_control() || c == '/')
        {
            return Err(HubCreationPolicyError("invalid namespace"));
        }
        if !(1..=86400).contains(&self.window_seconds)
            || self
                .max_repositories
                .is_some_and(|n| i64::try_from(n).is_err())
            || self
                .max_creations
                .is_some_and(|n| i64::try_from(n).is_err())
        {
            return Err(HubCreationPolicyError("limits exceed supported bounds"));
        }
        if self.denied_creators.len() > 1024
            || self.denied_creators.iter().any(|actor| {
                actor.issuer.is_empty()
                    || actor.subject.is_empty()
                    || actor.issuer.len() > 1024
                    || actor.subject.len() > 1024
                    || actor.issuer.contains('\0')
                    || actor.subject.contains('\0')
            })
        {
            return Err(HubCreationPolicyError("invalid denied creator list"));
        }
        Ok(())
    }
}

/// Publication result, keeping policy rejection distinct from storage failure.
#[derive(Debug)]
pub enum HubCreationOutcome {
    /// Repository, its initial ref and evidence committed atomically.
    Created(HubRepo),
    /// Shared policy revoked creation or disabled the namespace.
    Denied,
    /// Namespace repository capacity has been reached.
    QuotaExceeded,
    /// The trailing successful-creation rate has been reached.
    RateLimited,
}
