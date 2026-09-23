use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookDeliveryLifecycleState {
    Processed,
    Released,
}

impl WebhookDeliveryLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "Processed",
            Self::Released => "Released",
        }
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Processed, Self::Processed)
                | (Self::Processed, Self::Released)
                | (Self::Released, Self::Processed)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliveryIdentity {
    pub provider: String,
    pub owner: String,
    pub repo: String,
    pub delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDeliveryOperationId(String);

impl WebhookDeliveryOperationId {
    fn new(identity: &WebhookDeliveryIdentity) -> Self {
        let fields = [
            identity.provider.as_str(),
            identity.owner.as_str(),
            identity.repo.as_str(),
            identity.delivery_id.as_str(),
        ];
        Self(
            fields
                .into_iter()
                .map(|field| format!("{}:{field}", field.len()))
                .collect::<Vec<_>>()
                .join(""),
        )
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl WebhookDeliveryIdentity {
    pub fn new(
        provider: impl Into<String>,
        owner: impl Into<String>,
        repo: impl Into<String>,
        delivery_id: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        let identity = Self {
            provider: provider.into(),
            owner: owner.into(),
            repo: repo.into(),
            delivery_id: delivery_id.into(),
        };
        for (field, value) in [
            ("provider", identity.provider.as_str()),
            ("owner", identity.owner.as_str()),
            ("repo", identity.repo.as_str()),
            ("delivery_id", identity.delivery_id.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        Ok(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliverySnapshot {
    pub provider: String,
    pub owner: String,
    pub repo: String,
    pub delivery_id: String,
    pub processed_at_unix_seconds: u64,
    pub state: WebhookDeliveryLifecycleState,
}

impl WebhookDeliverySnapshot {
    pub fn new(
        identity: WebhookDeliveryIdentity,
        processed_at_unix_seconds: u64,
        state: WebhookDeliveryLifecycleState,
    ) -> Self {
        Self {
            provider: identity.provider,
            owner: identity.owner,
            repo: identity.repo,
            delivery_id: identity.delivery_id,
            processed_at_unix_seconds,
            state,
        }
    }

    fn identity(&self) -> Result<WebhookDeliveryIdentity, ReliabilityError> {
        WebhookDeliveryIdentity::new(
            self.provider.clone(),
            self.owner.clone(),
            self.repo.clone(),
            self.delivery_id.clone(),
        )
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        let identity = self.identity()?;
        Ok(OperationIdentity::new(
            format!("provider:{}", identity.provider),
            format!("{}/{}", identity.owner, identity.repo),
            WebhookDeliveryOperationId::new(&identity).0,
            OperationKind::WebhookDelivery,
        )?
        .with_object_key(format!(
            "{}/{}/{}",
            identity.provider, identity.owner, identity.repo
        )))
    }

    fn legacy_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        let identity = self.identity()?;
        Ok(OperationIdentity::new(
            format!("provider:{}", identity.provider),
            format!("{}/{}", identity.owner, identity.repo),
            identity.delivery_id,
            OperationKind::WebhookDelivery,
        )?
        .with_object_key(format!(
            "{}/{}/{}",
            identity.provider, identity.owner, identity.repo
        )))
    }
}

impl SnapshotEvidence for WebhookDeliverySnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_operation(
        &self,
        operation: &OperationIdentity,
    ) -> Result<(), ReliabilityError> {
        if &self.operation()? == operation || &self.legacy_operation()? == operation {
            Ok(())
        } else {
            Err(ReliabilityError::OperationMismatch)
        }
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.provider != after.provider
            || self.owner != after.owner
            || self.repo != after.repo
            || self.delivery_id != after.delivery_id
        {
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

pub type WebhookDeliveryLifecycleEvent = SnapshotEvidenceEvent<WebhookDeliverySnapshot>;
pub type WebhookDeliveryEvidenceLog = SnapshotEvidenceLog<WebhookDeliverySnapshot>;

pub fn verify_webhook_delivery_chain(
    events: &[WebhookDeliveryLifecycleEvent],
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)
}

pub fn verify_webhook_delivery_events(
    events: &[WebhookDeliveryLifecycleEvent],
    expected: &WebhookDeliverySnapshot,
) -> Result<(), ReliabilityError> {
    verify_webhook_delivery_chain(events)?;
    if events.last().is_some_and(|event| event.after == *expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::snapshot_event::SnapshotEvidence;

    fn snapshot(state: WebhookDeliveryLifecycleState) -> WebhookDeliverySnapshot {
        WebhookDeliverySnapshot::new(
            WebhookDeliveryIdentity::new("github", "team", "assets", "delivery-1").unwrap(),
            100,
            state,
        )
    }

    #[test]
    fn processed_release_processed_chain_verifies() {
        let mut log = WebhookDeliveryEvidenceLog::baseline(snapshot(
            WebhookDeliveryLifecycleState::Processed,
        ))
        .unwrap();
        log.record(snapshot(WebhookDeliveryLifecycleState::Released))
            .unwrap();
        log.record(snapshot(WebhookDeliveryLifecycleState::Processed))
            .unwrap();
        verify_webhook_delivery_events(
            log.events(),
            &snapshot(WebhookDeliveryLifecycleState::Processed),
        )
        .unwrap();
    }

    #[test]
    fn tampered_delivery_identity_is_rejected() {
        let log = WebhookDeliveryEvidenceLog::baseline(snapshot(
            WebhookDeliveryLifecycleState::Processed,
        ))
        .unwrap();
        let mut events = log.events().to_vec();
        if let Some(event) = events.first_mut() {
            event.after.delivery_id = "tampered".to_owned();
        }
        assert!(verify_webhook_delivery_chain(&events).is_err());
    }

    #[test]
    fn operation_id_is_scoped_to_repository_identity() {
        let first = WebhookDeliverySnapshot::new(
            WebhookDeliveryIdentity::new("github", "team", "assets", "delivery-shared").unwrap(),
            100,
            WebhookDeliveryLifecycleState::Processed,
        );
        let second = WebhookDeliverySnapshot::new(
            WebhookDeliveryIdentity::new("github", "team", "other-assets", "delivery-shared")
                .unwrap(),
            100,
            WebhookDeliveryLifecycleState::Processed,
        );
        assert_ne!(
            first.evidence_operation().unwrap().operation_id,
            second.evidence_operation().unwrap().operation_id
        );
    }
}
