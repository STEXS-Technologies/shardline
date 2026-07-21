use shardline_protocol::RepositoryProvider;
use shardline_storage::ObjectKey;
use thiserror::Error;

/// Durable quarantine state for one unreachable object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineCandidate {
    object_key: ObjectKey,
    observed_length: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

impl QuarantineCandidate {
    /// Creates a quarantine candidate record.
    ///
    /// # Errors
    ///
    /// Returns [`QuarantineCandidateError::InvertedTimeline`] when the deletion
    /// eligibility time precedes the first unreachable observation.
    pub fn new(
        object_key: ObjectKey,
        observed_length: u64,
        first_seen_unreachable_at_unix_seconds: u64,
        delete_after_unix_seconds: u64,
    ) -> Result<Self, QuarantineCandidateError> {
        if delete_after_unix_seconds < first_seen_unreachable_at_unix_seconds {
            return Err(QuarantineCandidateError::InvertedTimeline);
        }

        Ok(Self {
            object_key,
            observed_length,
            first_seen_unreachable_at_unix_seconds,
            delete_after_unix_seconds,
        })
    }

    /// Returns the object key tracked by this candidate.
    #[must_use]
    pub const fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the observed object length when the candidate was recorded.
    #[must_use]
    pub const fn observed_length(&self) -> u64 {
        self.observed_length
    }

    /// Returns when the object first became unreachable.
    #[must_use]
    pub const fn first_seen_unreachable_at_unix_seconds(&self) -> u64 {
        self.first_seen_unreachable_at_unix_seconds
    }

    /// Returns when the object becomes eligible for deletion.
    #[must_use]
    pub const fn delete_after_unix_seconds(&self) -> u64 {
        self.delete_after_unix_seconds
    }
}

/// Quarantine candidate construction failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum QuarantineCandidateError {
    /// The deletion eligibility timestamp precedes the first unreachable timestamp.
    #[error("quarantine delete-after time must not precede first-unreachable time")]
    InvertedTimeline,
}

/// Durable retention hold for one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionHold {
    object_key: ObjectKey,
    reason: String,
    held_at_unix_seconds: u64,
    release_after_unix_seconds: Option<u64>,
}

impl RetentionHold {
    /// Creates a retention hold record.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionHoldError::EmptyReason`] when the reason is blank.
    pub fn new(
        object_key: ObjectKey,
        reason: String,
        held_at_unix_seconds: u64,
        release_after_unix_seconds: Option<u64>,
    ) -> Result<Self, RetentionHoldError> {
        if reason.trim().is_empty() {
            return Err(RetentionHoldError::EmptyReason);
        }
        if let Some(release_after_unix_seconds) = release_after_unix_seconds
            && release_after_unix_seconds < held_at_unix_seconds
        {
            return Err(RetentionHoldError::InvertedTimeline);
        }

        Ok(Self {
            object_key,
            reason,
            held_at_unix_seconds,
            release_after_unix_seconds,
        })
    }

    /// Returns the held object key.
    #[must_use]
    pub const fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    /// Returns the operator-supplied hold reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns when the hold was recorded.
    #[must_use]
    pub const fn held_at_unix_seconds(&self) -> u64 {
        self.held_at_unix_seconds
    }

    /// Returns when the hold stops applying, if it is time-bounded.
    #[must_use]
    pub const fn release_after_unix_seconds(&self) -> Option<u64> {
        self.release_after_unix_seconds
    }

    /// Returns whether the hold is active at the supplied Unix timestamp.
    #[must_use]
    pub const fn is_active_at(&self, now_unix_seconds: u64) -> bool {
        match self.release_after_unix_seconds {
            Some(release_after_unix_seconds) => release_after_unix_seconds > now_unix_seconds,
            None => true,
        }
    }
}

/// Durable record of one processed provider webhook delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDelivery {
    provider: RepositoryProvider,
    owner: String,
    repo: String,
    delivery_id: String,
    processed_at_unix_seconds: u64,
}

/// Durable provider-derived repository lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRepositoryState {
    provider: RepositoryProvider,
    owner: String,
    repo: String,
    last_access_changed_at_unix_seconds: Option<u64>,
    last_revision_pushed_at_unix_seconds: Option<u64>,
    last_pushed_revision: Option<String>,
    last_cache_invalidated_at_unix_seconds: Option<u64>,
    last_authorization_rechecked_at_unix_seconds: Option<u64>,
    last_drift_checked_at_unix_seconds: Option<u64>,
}

impl ProviderRepositoryState {
    /// Creates repository lifecycle state.
    #[must_use]
    pub const fn new(
        provider: RepositoryProvider,
        owner: String,
        repo: String,
        last_access_changed_at_unix_seconds: Option<u64>,
        last_revision_pushed_at_unix_seconds: Option<u64>,
        last_pushed_revision: Option<String>,
    ) -> Self {
        Self {
            provider,
            owner,
            repo,
            last_access_changed_at_unix_seconds,
            last_revision_pushed_at_unix_seconds,
            last_pushed_revision,
            last_cache_invalidated_at_unix_seconds: None,
            last_authorization_rechecked_at_unix_seconds: None,
            last_drift_checked_at_unix_seconds: None,
        }
    }

    /// Returns a copy with reconciliation timestamps set.
    #[must_use]
    pub const fn with_reconciliation(
        mut self,
        last_cache_invalidated_at_unix_seconds: Option<u64>,
        last_authorization_rechecked_at_unix_seconds: Option<u64>,
        last_drift_checked_at_unix_seconds: Option<u64>,
    ) -> Self {
        self.last_cache_invalidated_at_unix_seconds = last_cache_invalidated_at_unix_seconds;
        self.last_authorization_rechecked_at_unix_seconds =
            last_authorization_rechecked_at_unix_seconds;
        self.last_drift_checked_at_unix_seconds = last_drift_checked_at_unix_seconds;
        self
    }

    /// Returns the repository hosting provider.
    #[must_use]
    pub const fn provider(&self) -> RepositoryProvider {
        self.provider
    }

    /// Returns the repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the repository name.
    #[must_use]
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// Returns the latest access-change timestamp, when observed.
    #[must_use]
    pub const fn last_access_changed_at_unix_seconds(&self) -> Option<u64> {
        self.last_access_changed_at_unix_seconds
    }

    /// Returns the latest revision-push timestamp, when observed.
    #[must_use]
    pub const fn last_revision_pushed_at_unix_seconds(&self) -> Option<u64> {
        self.last_revision_pushed_at_unix_seconds
    }

    /// Returns the latest pushed revision, when observed.
    #[must_use]
    pub fn last_pushed_revision(&self) -> Option<&str> {
        self.last_pushed_revision.as_deref()
    }

    /// Returns when provider-driven cache invalidation was last reconciled.
    #[must_use]
    pub const fn last_cache_invalidated_at_unix_seconds(&self) -> Option<u64> {
        self.last_cache_invalidated_at_unix_seconds
    }

    /// Returns when provider authorization was last rechecked after an access-change signal.
    #[must_use]
    pub const fn last_authorization_rechecked_at_unix_seconds(&self) -> Option<u64> {
        self.last_authorization_rechecked_at_unix_seconds
    }

    /// Returns when repository drift was last checked after a provider lifecycle signal.
    #[must_use]
    pub const fn last_drift_checked_at_unix_seconds(&self) -> Option<u64> {
        self.last_drift_checked_at_unix_seconds
    }
}

impl WebhookDelivery {
    /// Creates a processed webhook delivery record.
    ///
    /// # Errors
    ///
    /// Returns [`WebhookDeliveryError`] when repository or delivery components are
    /// blank, too long, or contain control characters.
    pub fn new(
        provider: RepositoryProvider,
        owner: String,
        repo: String,
        delivery_id: String,
        processed_at_unix_seconds: u64,
    ) -> Result<Self, WebhookDeliveryError> {
        validate_webhook_component(&owner, WebhookDeliveryError::EmptyRepositoryOwner)?;
        validate_webhook_component(&repo, WebhookDeliveryError::EmptyRepositoryName)?;
        validate_webhook_component(&delivery_id, WebhookDeliveryError::EmptyDeliveryId)?;

        Ok(Self {
            provider,
            owner,
            repo,
            delivery_id,
            processed_at_unix_seconds,
        })
    }

    /// Returns the provider family that emitted the webhook.
    #[must_use]
    pub const fn provider(&self) -> RepositoryProvider {
        self.provider
    }

    /// Returns the repository owner or namespace for the delivery claim.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the repository name for the delivery claim.
    #[must_use]
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// Returns the stable provider delivery identifier.
    #[must_use]
    pub fn delivery_id(&self) -> &str {
        &self.delivery_id
    }

    /// Returns when the webhook delivery was first processed.
    #[must_use]
    pub const fn processed_at_unix_seconds(&self) -> u64 {
        self.processed_at_unix_seconds
    }
}

const MAX_WEBHOOK_DELIVERY_COMPONENT_BYTES: usize = 512;

fn validate_webhook_component(
    value: &str,
    empty_error: WebhookDeliveryError,
) -> Result<(), WebhookDeliveryError> {
    if value.trim().is_empty() {
        return Err(empty_error);
    }
    if value.len() > MAX_WEBHOOK_DELIVERY_COMPONENT_BYTES {
        return Err(WebhookDeliveryError::TooLong);
    }
    if value.chars().any(char::is_control) {
        return Err(WebhookDeliveryError::InvalidComponent);
    }

    Ok(())
}

/// Invalid retention-hold input.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum RetentionHoldError {
    /// The hold reason was blank.
    #[error("retention hold reason must not be empty")]
    EmptyReason,
    /// The hold release time preceded the hold creation time.
    #[error("retention hold release time must not precede held time")]
    InvertedTimeline,
}

/// Invalid processed-webhook-delivery input.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum WebhookDeliveryError {
    /// The webhook repository owner was blank.
    #[error("webhook delivery repository owner must not be empty")]
    EmptyRepositoryOwner,
    /// The webhook repository name was blank.
    #[error("webhook delivery repository name must not be empty")]
    EmptyRepositoryName,
    /// The webhook delivery identifier was blank.
    #[error("webhook delivery id must not be empty")]
    EmptyDeliveryId,
    /// A webhook delivery component contained control characters.
    #[error("webhook delivery component contained invalid control characters")]
    InvalidComponent,
    /// A webhook delivery component exceeded the supported length.
    #[error("webhook delivery component exceeded supported length")]
    TooLong,
    /// The stored or provided webhook provider name was invalid.
    #[error("webhook delivery provider was invalid")]
    InvalidProvider,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use shardline_protocol::RepositoryProvider;
    use shardline_storage::ObjectKey;

    use super::{
        ProviderRepositoryState, QuarantineCandidate, QuarantineCandidateError, RetentionHold,
        RetentionHoldError, WebhookDelivery, WebhookDeliveryError,
    };

    #[test]
    fn quarantine_candidate_keeps_fields() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 128, 10, 20).unwrap();

        assert_eq!(candidate.object_key(), &key);
        assert_eq!(candidate.observed_length(), 128);
        assert_eq!(candidate.first_seen_unreachable_at_unix_seconds(), 10);
        assert_eq!(candidate.delete_after_unix_seconds(), 20);
    }

    #[test]
    fn quarantine_candidate_rejects_inverted_timeline() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();

        assert_eq!(
            QuarantineCandidate::new(key, 128, 20, 10),
            Err(QuarantineCandidateError::InvertedTimeline)
        );
    }

    #[test]
    fn retention_hold_keeps_fields() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let hold = RetentionHold::new(
            key.clone(),
            "provider deletion grace".to_owned(),
            10,
            Some(20),
        )
        .unwrap();

        assert_eq!(hold.object_key(), &key);
        assert_eq!(hold.reason(), "provider deletion grace");
        assert_eq!(hold.held_at_unix_seconds(), 10);
        assert_eq!(hold.release_after_unix_seconds(), Some(20));
        assert!(hold.is_active_at(19));
        assert!(!hold.is_active_at(20));
    }

    #[test]
    fn retention_hold_rejects_blank_reason() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();

        assert_eq!(
            RetentionHold::new(key, "   ".to_owned(), 10, None),
            Err(RetentionHoldError::EmptyReason)
        );
    }

    #[test]
    fn retention_hold_rejects_inverted_timeline() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();

        assert_eq!(
            RetentionHold::new(key, "provider deletion grace".to_owned(), 20, Some(10)),
            Err(RetentionHoldError::InvertedTimeline)
        );
    }

    #[test]
    fn provider_repository_state_keeps_fields() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitLab,
            "team".to_owned(),
            "assets".to_owned(),
            Some(11),
            Some(12),
            Some("refs/heads/main".to_owned()),
        );

        assert_eq!(state.provider(), RepositoryProvider::GitLab);
        assert_eq!(state.owner(), "team");
        assert_eq!(state.repo(), "assets");
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(11));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(12));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/main"));

        let reconciled = state.with_reconciliation(Some(13), Some(14), Some(15));
        assert_eq!(
            reconciled.last_cache_invalidated_at_unix_seconds(),
            Some(13)
        );
        assert_eq!(
            reconciled.last_authorization_rechecked_at_unix_seconds(),
            Some(14)
        );
        assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(15));
    }

    #[test]
    fn webhook_delivery_rejects_blank_identifier() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "   ".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::EmptyDeliveryId));
    }

    #[test]
    fn webhook_delivery_rejects_control_characters() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-\n1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::InvalidComponent));
    }

    #[test]
    fn webhook_delivery_rejects_blank_repository_scope() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            " ".to_owned(),
            "delivery-1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::EmptyRepositoryName));
    }

    // ── Error display ──────────────────────────────────────────────────────

    #[test]
    fn quarantine_candidate_error_display() {
        assert_eq!(
            QuarantineCandidateError::InvertedTimeline.to_string(),
            "quarantine delete-after time must not precede first-unreachable time"
        );
    }

    #[test]
    fn retention_hold_error_empty_reason_display() {
        assert_eq!(
            RetentionHoldError::EmptyReason.to_string(),
            "retention hold reason must not be empty"
        );
    }

    #[test]
    fn retention_hold_error_inverted_timeline_display() {
        assert_eq!(
            RetentionHoldError::InvertedTimeline.to_string(),
            "retention hold release time must not precede held time"
        );
    }

    #[test]
    fn webhook_delivery_error_empty_repository_owner_display() {
        assert_eq!(
            WebhookDeliveryError::EmptyRepositoryOwner.to_string(),
            "webhook delivery repository owner must not be empty"
        );
    }

    #[test]
    fn webhook_delivery_error_empty_repository_name_display() {
        assert_eq!(
            WebhookDeliveryError::EmptyRepositoryName.to_string(),
            "webhook delivery repository name must not be empty"
        );
    }

    #[test]
    fn webhook_delivery_error_empty_delivery_id_display() {
        assert_eq!(
            WebhookDeliveryError::EmptyDeliveryId.to_string(),
            "webhook delivery id must not be empty"
        );
    }

    #[test]
    fn webhook_delivery_error_invalid_component_display() {
        assert_eq!(
            WebhookDeliveryError::InvalidComponent.to_string(),
            "webhook delivery component contained invalid control characters"
        );
    }

    #[test]
    fn webhook_delivery_error_too_long_display() {
        assert_eq!(
            WebhookDeliveryError::TooLong.to_string(),
            "webhook delivery component exceeded supported length"
        );
    }

    #[test]
    fn webhook_delivery_error_invalid_provider_display() {
        assert_eq!(
            WebhookDeliveryError::InvalidProvider.to_string(),
            "webhook delivery provider was invalid"
        );
    }

    #[test]
    fn webhook_delivery_rejects_blank_owner() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            " ".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::EmptyRepositoryOwner));
    }

    #[test]
    fn webhook_delivery_rejects_too_long_component() {
        let long = "a".repeat(600);
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            long,
            "assets".to_owned(),
            "delivery-1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::TooLong));
    }

    #[test]
    fn webhook_delivery_rejects_control_characters_in_owner() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team\x00name".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::InvalidComponent));
    }

    #[test]
    fn webhook_delivery_rejects_control_characters_in_repo() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets\nrepo".to_owned(),
            "delivery-1".to_owned(),
            10,
        );
        assert_eq!(delivery, Err(WebhookDeliveryError::InvalidComponent));
    }

    #[test]
    fn webhook_delivery_keeps_fields() {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            42,
        )
        .unwrap();
        assert_eq!(delivery.provider(), RepositoryProvider::GitHub);
        assert_eq!(delivery.owner(), "team");
        assert_eq!(delivery.repo(), "assets");
        assert_eq!(delivery.delivery_id(), "delivery-1");
        assert_eq!(delivery.processed_at_unix_seconds(), 42);
    }

    // ── ProviderRepositoryState edge cases ─────────────────────────────────

    #[test]
    fn provider_repository_state_with_reconciliation_all_none() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            Some(10),
            Some(20),
            Some("refs/heads/main".to_owned()),
        );
        let reconciled = state.with_reconciliation(None, None, None);
        assert!(
            reconciled
                .last_cache_invalidated_at_unix_seconds()
                .is_none()
        );
        assert!(
            reconciled
                .last_authorization_rechecked_at_unix_seconds()
                .is_none()
        );
        assert!(reconciled.last_drift_checked_at_unix_seconds().is_none());
    }

    #[test]
    fn provider_repository_state_new_all_none() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            None,
            None,
            None,
        );
        assert!(state.last_access_changed_at_unix_seconds().is_none());
        assert!(state.last_revision_pushed_at_unix_seconds().is_none());
        assert!(state.last_pushed_revision().is_none());
    }

    #[test]
    fn retention_hold_without_release_is_active_indefinitely() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let hold = RetentionHold::new(key, "infinite hold".to_owned(), 10, None).unwrap();
        assert!(hold.is_active_at(0));
        assert!(hold.is_active_at(u64::MAX));
    }

    #[test]
    fn retention_hold_release_at_exact_boundary_is_inactive() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let hold = RetentionHold::new(key, "exact release".to_owned(), 10, Some(20)).unwrap();
        assert!(
            !hold.is_active_at(20),
            "hold with release_at=20 should be inactive at t=20"
        );
        assert!(
            !hold.is_active_at(21),
            "hold with release_at=20 should be inactive at t=21"
        );
    }

    #[test]
    fn retention_hold_reason_preserves_non_whitespace_strings() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb").unwrap();
        let reason = "  spaced out  ";
        let hold = RetentionHold::new(key, reason.to_owned(), 10, None).unwrap();
        assert_eq!(hold.reason(), "  spaced out  ");
    }

    #[test]
    fn provider_repository_state_field_accessors_return_correct_values() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::Gitea,
            "org".to_owned(),
            "repo".to_owned(),
            Some(100),
            Some(200),
            Some("refs/heads/dev".to_owned()),
        );
        assert_eq!(state.provider(), RepositoryProvider::Gitea);
        assert_eq!(state.owner(), "org");
        assert_eq!(state.repo(), "repo");
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(100));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/dev"));
    }

    #[test]
    fn provider_repository_state_with_reconciliation_sets_timestamps() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            Some(10),
            Some(20),
            Some("refs/heads/main".to_owned()),
        );
        let reconciled = state.with_reconciliation(Some(30), Some(40), Some(50));
        assert_eq!(
            reconciled.last_cache_invalidated_at_unix_seconds(),
            Some(30)
        );
        assert_eq!(
            reconciled.last_authorization_rechecked_at_unix_seconds(),
            Some(40)
        );
        assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(50));
    }

    #[test]
    fn provider_repository_state_with_reconciliation_partial_none() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::Codeberg,
            "user".to_owned(),
            "project".to_owned(),
            None,
            None,
            None,
        );
        let reconciled = state.with_reconciliation(Some(1), None, Some(3));
        assert_eq!(reconciled.last_cache_invalidated_at_unix_seconds(), Some(1));
        assert!(
            reconciled
                .last_authorization_rechecked_at_unix_seconds()
                .is_none()
        );
        assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(3));
    }
}
