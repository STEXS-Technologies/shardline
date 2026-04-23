use thiserror::Error;

use crate::{AuthorizationRequest, ProviderKind, RepositoryRef, RevisionRef};

const MAX_PROVIDER_IDENTITY_BYTES: usize = 512;
const MAX_PROVIDER_URL_BYTES: usize = 4096;

/// Version-control provider adapter contract.
pub trait ProviderAdapter {
    /// Adapter-specific error type.
    type Error;

    /// Returns the provider family implemented by this adapter.
    fn kind(&self) -> ProviderKind;

    /// Evaluates whether the provider grants the requested repository access.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when provider state cannot be reached or interpreted.
    fn check_access(
        &self,
        request: &AuthorizationRequest,
    ) -> Result<AuthorizationDecision, Self::Error>;

    /// Resolves repository metadata used for token scopes, webhook handling, and
    /// repository bootstrap behavior.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when repository metadata cannot be loaded.
    fn repository_metadata(
        &self,
        repository: &RepositoryRef,
    ) -> Result<RepositoryMetadata, Self::Error>;

    /// Parses a provider-specific webhook request into a normalized repository event.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the webhook cannot be authenticated or decoded.
    fn parse_webhook(
        &self,
        request: WebhookRequest<'_>,
    ) -> Result<Option<RepositoryWebhookEvent>, Self::Error>;
}

/// Access decision returned by a provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationDecision {
    /// The provider granted access to a concrete subject.
    Allow(ProviderSubject),
    /// The provider denied access.
    Deny,
}

/// Provider identity for the subject that requested access.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderSubject(String);

impl ProviderSubject {
    /// Creates a provider subject identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderBoundaryError`] when the identifier is empty, too large,
    /// or contains control characters.
    pub fn new(value: &str) -> Result<Self, ProviderBoundaryError> {
        validate_component(value, MAX_PROVIDER_IDENTITY_BYTES)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the subject identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Repository visibility as reported by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepositoryVisibility {
    /// Publicly readable repository.
    Public,
    /// Private repository.
    Private,
    /// Provider-specific internal visibility.
    Internal,
}

/// Normalized repository metadata resolved from a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryMetadata {
    repository: RepositoryRef,
    visibility: RepositoryVisibility,
    default_revision: RevisionRef,
    clone_url: CanonicalCloneUrl,
}

impl RepositoryMetadata {
    /// Creates normalized repository metadata.
    #[must_use]
    pub const fn new(
        repository: RepositoryRef,
        visibility: RepositoryVisibility,
        default_revision: RevisionRef,
        clone_url: CanonicalCloneUrl,
    ) -> Self {
        Self {
            repository,
            visibility,
            default_revision,
            clone_url,
        }
    }

    /// Returns the repository identity.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryRef {
        &self.repository
    }

    /// Returns the provider-reported visibility.
    #[must_use]
    pub const fn visibility(&self) -> RepositoryVisibility {
        self.visibility
    }

    /// Returns the default revision used when a caller omitted a revision.
    #[must_use]
    pub const fn default_revision(&self) -> &RevisionRef {
        &self.default_revision
    }

    /// Returns the canonical clone URL.
    #[must_use]
    pub const fn clone_url(&self) -> &CanonicalCloneUrl {
        &self.clone_url
    }
}

/// Canonical clone URL for a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCloneUrl(String);

impl CanonicalCloneUrl {
    /// Creates a canonical clone URL.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderBoundaryError`] when the URL is empty, too large, or
    /// contains control characters.
    pub fn new(value: &str) -> Result<Self, ProviderBoundaryError> {
        validate_component(value, MAX_PROVIDER_URL_BYTES)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the clone URL.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Normalized provider webhook request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookRequest<'request> {
    event_name: &'request str,
    delivery_id: &'request str,
    signature: Option<&'request str>,
    body: &'request [u8],
}

impl<'request> WebhookRequest<'request> {
    /// Creates a normalized webhook request view.
    #[must_use]
    pub const fn new(
        event_name: &'request str,
        delivery_id: &'request str,
        signature: Option<&'request str>,
        body: &'request [u8],
    ) -> Self {
        Self {
            event_name,
            delivery_id,
            signature,
            body,
        }
    }

    /// Returns the normalized event name.
    #[must_use]
    pub const fn event_name(&self) -> &str {
        self.event_name
    }

    /// Returns the delivery identifier.
    #[must_use]
    pub const fn delivery_id(&self) -> &str {
        self.delivery_id
    }

    /// Returns the raw signature header, if one was supplied.
    #[must_use]
    pub const fn signature(&self) -> Option<&str> {
        self.signature
    }

    /// Returns the raw webhook body.
    #[must_use]
    pub const fn body(&self) -> &[u8] {
        self.body
    }
}

/// Stable webhook delivery identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDeliveryId(String);

impl WebhookDeliveryId {
    /// Creates a delivery identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderBoundaryError`] when the identifier is empty, too large,
    /// or contains control characters.
    pub fn new(value: &str) -> Result<Self, ProviderBoundaryError> {
        validate_component(value, MAX_PROVIDER_IDENTITY_BYTES)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the delivery identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Normalized repository webhook event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWebhookEvent {
    repository: RepositoryRef,
    delivery_id: WebhookDeliveryId,
    kind: RepositoryWebhookEventKind,
}

impl RepositoryWebhookEvent {
    /// Creates a normalized repository webhook event.
    #[must_use]
    pub const fn new(
        repository: RepositoryRef,
        delivery_id: WebhookDeliveryId,
        kind: RepositoryWebhookEventKind,
    ) -> Self {
        Self {
            repository,
            delivery_id,
            kind,
        }
    }

    /// Returns the affected repository.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryRef {
        &self.repository
    }

    /// Returns the provider delivery identifier.
    #[must_use]
    pub const fn delivery_id(&self) -> &WebhookDeliveryId {
        &self.delivery_id
    }

    /// Returns the normalized webhook event kind.
    #[must_use]
    pub const fn kind(&self) -> &RepositoryWebhookEventKind {
        &self.kind
    }
}

/// Normalized repository webhook event kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryWebhookEventKind {
    /// Repository permissions or visibility changed.
    AccessChanged,
    /// Repository was deleted and its roots may be collectible after retention windows.
    RepositoryDeleted,
    /// Repository was renamed.
    RepositoryRenamed {
        /// New repository identity.
        new_repository: RepositoryRef,
    },
    /// A revision was updated and provider integration may trigger reconciliation.
    RevisionPushed {
        /// Updated revision reference.
        revision: RevisionRef,
    },
}

/// Provider boundary validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ProviderBoundaryError {
    /// A required value was empty.
    #[error("provider boundary value must not be empty")]
    Empty,
    /// A value contained control characters.
    #[error("provider boundary value must not contain control characters")]
    ControlCharacter,
    /// A value exceeded the supported metadata bound.
    #[error("provider boundary value exceeded supported length")]
    TooLong,
}

fn validate_component(value: &str, max_bytes: usize) -> Result<(), ProviderBoundaryError> {
    if value.trim().is_empty() {
        return Err(ProviderBoundaryError::Empty);
    }

    if value.len() > max_bytes {
        return Err(ProviderBoundaryError::TooLong);
    }

    if value.chars().any(char::is_control) {
        return Err(ProviderBoundaryError::ControlCharacter);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationDecision, CanonicalCloneUrl, MAX_PROVIDER_IDENTITY_BYTES,
        MAX_PROVIDER_URL_BYTES, ProviderAdapter, ProviderBoundaryError, ProviderSubject,
        RepositoryMetadata, RepositoryVisibility, RepositoryWebhookEvent,
        RepositoryWebhookEventKind, WebhookDeliveryId, WebhookRequest,
    };
    use crate::{AuthorizationRequest, ProviderKind, RepositoryAccess, RepositoryRef, RevisionRef};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MockProvider;

    impl ProviderAdapter for MockProvider {
        type Error = ProviderBoundaryError;

        fn kind(&self) -> ProviderKind {
            ProviderKind::GitHub
        }

        fn check_access(
            &self,
            request: &AuthorizationRequest,
        ) -> Result<AuthorizationDecision, Self::Error> {
            if request.access() == RepositoryAccess::Write {
                return Ok(AuthorizationDecision::Allow(request.subject().clone()));
            }

            Ok(AuthorizationDecision::Deny)
        }

        fn repository_metadata(
            &self,
            repository: &RepositoryRef,
        ) -> Result<RepositoryMetadata, Self::Error> {
            Ok(RepositoryMetadata::new(
                repository.clone(),
                RepositoryVisibility::Private,
                RevisionRef::new("refs/heads/main")
                    .map_err(|_error| ProviderBoundaryError::Empty)?,
                CanonicalCloneUrl::new("https://github.example/team/assets.git")?,
            ))
        }

        fn parse_webhook(
            &self,
            request: WebhookRequest<'_>,
        ) -> Result<Option<RepositoryWebhookEvent>, Self::Error> {
            if request.event_name() != "push" {
                return Ok(None);
            }

            let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets")
                .map_err(|_error| ProviderBoundaryError::Empty)?;
            let delivery_id = WebhookDeliveryId::new(request.delivery_id())?;
            let revision = RevisionRef::new("refs/heads/main")
                .map_err(|_error| ProviderBoundaryError::Empty)?;

            Ok(Some(RepositoryWebhookEvent::new(
                repository,
                delivery_id,
                RepositoryWebhookEventKind::RevisionPushed { revision },
            )))
        }
    }

    #[test]
    fn provider_subject_rejects_empty_values() {
        assert_eq!(ProviderSubject::new(" "), Err(ProviderBoundaryError::Empty));
    }

    #[test]
    fn provider_subject_rejects_oversized_values() {
        let oversized = "s".repeat(MAX_PROVIDER_IDENTITY_BYTES + 1);

        assert_eq!(
            ProviderSubject::new(&oversized),
            Err(ProviderBoundaryError::TooLong)
        );
    }

    #[test]
    fn clone_url_rejects_control_characters() {
        assert_eq!(
            CanonicalCloneUrl::new("https://example.invalid/repo.git\n"),
            Err(ProviderBoundaryError::ControlCharacter)
        );
    }

    #[test]
    fn clone_url_rejects_oversized_values() {
        let oversized = "u".repeat(MAX_PROVIDER_URL_BYTES + 1);

        assert_eq!(
            CanonicalCloneUrl::new(&oversized),
            Err(ProviderBoundaryError::TooLong)
        );
    }

    #[test]
    fn webhook_delivery_id_rejects_empty_values() {
        assert_eq!(
            WebhookDeliveryId::new(""),
            Err(ProviderBoundaryError::Empty)
        );
    }

    #[test]
    fn webhook_delivery_id_rejects_oversized_values() {
        let oversized = "d".repeat(MAX_PROVIDER_IDENTITY_BYTES + 1);

        assert_eq!(
            WebhookDeliveryId::new(&oversized),
            Err(ProviderBoundaryError::TooLong)
        );
    }

    #[test]
    fn provider_adapter_reports_kind_and_allows_write_access() {
        let provider = MockProvider;
        let subject = ProviderSubject::new("caller-123");
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");

        assert!(subject.is_ok());
        assert!(repository.is_ok());
        assert!(revision.is_ok());

        let (Ok(subject), Ok(repository), Ok(revision)) = (subject, repository, revision) else {
            return;
        };
        let request = AuthorizationRequest::new(
            subject.clone(),
            repository,
            revision,
            RepositoryAccess::Write,
        );
        let decision = provider.check_access(&request);
        assert_eq!(provider.kind(), ProviderKind::GitHub);
        assert_eq!(decision, Ok(AuthorizationDecision::Allow(subject)));
    }

    #[test]
    fn provider_adapter_denies_read_access_when_not_granted() {
        let provider = MockProvider;
        let subject = ProviderSubject::new("caller-123");
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");

        assert!(subject.is_ok());
        assert!(repository.is_ok());
        assert!(revision.is_ok());

        let (Ok(subject), Ok(repository), Ok(revision)) = (subject, repository, revision) else {
            return;
        };
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);

        assert_eq!(
            provider.check_access(&request),
            Ok(AuthorizationDecision::Deny)
        );
    }

    #[test]
    fn provider_adapter_returns_repository_metadata() {
        let provider = MockProvider;
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");

        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let metadata = provider.repository_metadata(&repository);

        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };

        assert_eq!(metadata.repository(), &repository);
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
        assert_eq!(
            metadata.clone_url().as_str(),
            "https://github.example/team/assets.git"
        );
    }

    #[test]
    fn provider_adapter_normalizes_push_webhooks() {
        let provider = MockProvider;
        let request = WebhookRequest::new("push", "delivery-1", Some("sig"), b"{}");
        let event = provider.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let revision = RevisionRef::new("refs/heads/main");

        assert!(revision.is_ok());
        let Ok(revision) = revision else {
            return;
        };

        assert_eq!(event.delivery_id().as_str(), "delivery-1");
        assert_eq!(event.repository().provider(), ProviderKind::GitHub);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn provider_adapter_ignores_unknown_webhooks() {
        let provider = MockProvider;
        let request = WebhookRequest::new("issues", "delivery-1", None, b"{}");

        assert_eq!(provider.parse_webhook(request), Ok(None));
    }
}
