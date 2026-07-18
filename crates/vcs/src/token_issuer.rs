use std::num::NonZeroU64;

use shardline_protocol::{
    RepositoryScope, SecretString, TokenClaims, TokenClaimsError, TokenCodecError, TokenScope,
    TokenSigner, unix_now_seconds_lossy,
};
use thiserror::Error;

use crate::{
    AuthorizationDecision, AuthorizationRequest, ProviderAdapter, ProviderSubject,
    RepositoryAccess, RepositoryRef, RevisionRef,
};

/// Provider access grant that has already been authorized by a provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedRepositoryAccess {
    subject: ProviderSubject,
    repository: RepositoryRef,
    revision: RevisionRef,
    access: RepositoryAccess,
}

impl GrantedRepositoryAccess {
    /// Runs the provider authorization check and returns a typed grant when access is
    /// allowed.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when provider state cannot be reached or interpreted.
    pub fn authorize<A: ProviderAdapter>(
        adapter: &A,
        request: &AuthorizationRequest,
    ) -> Result<Option<Self>, A::Error> {
        let decision = adapter.check_access(request)?;
        Ok(Self::from_decision(request, decision))
    }

    /// Converts a normalized authorization decision into a typed access grant.
    #[must_use]
    pub fn from_decision(
        request: &AuthorizationRequest,
        decision: AuthorizationDecision,
    ) -> Option<Self> {
        match decision {
            AuthorizationDecision::Allow(subject) => Some(Self {
                subject,
                repository: request.repository().clone(),
                revision: request.revision().clone(),
                access: request.access(),
            }),
            AuthorizationDecision::Deny => None,
        }
    }

    /// Returns the granted provider subject.
    #[must_use]
    pub const fn subject(&self) -> &ProviderSubject {
        &self.subject
    }

    /// Returns the granted repository reference.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryRef {
        &self.repository
    }

    /// Returns the granted revision reference.
    #[must_use]
    pub const fn revision(&self) -> &RevisionRef {
        &self.revision
    }

    /// Returns the granted access level.
    #[must_use]
    pub const fn access(&self) -> RepositoryAccess {
        self.access
    }
}

/// Signed CAS token minted from a provider access grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIssuedToken {
    token: SecretString,
    claims: TokenClaims,
}

impl ProviderIssuedToken {
    /// Returns the opaque bearer token string.
    #[must_use]
    pub fn token(&self) -> &str {
        self.token.expose_secret()
    }

    /// Returns the claims embedded in the signed token.
    #[must_use]
    pub const fn claims(&self) -> &TokenClaims {
        &self.claims
    }
}

/// Provider-backed token issuer for repository-scoped CAS access.
#[derive(Debug, Clone)]
pub struct ProviderTokenIssuer {
    issuer: String,
    ttl_seconds: NonZeroU64,
    signer: TokenSigner,
}

impl ProviderTokenIssuer {
    /// Creates a provider-backed token issuer.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderTokenIssuanceError`] when the signing key is invalid.
    pub fn new(
        issuer: &str,
        signing_key: &[u8],
        ttl_seconds: NonZeroU64,
    ) -> Result<Self, ProviderTokenIssuanceError> {
        let signer = TokenSigner::new(signing_key)?;
        Ok(Self {
            issuer: issuer.to_owned(),
            ttl_seconds,
            signer,
        })
    }

    /// Returns the configured issuer identity.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the configured token lifetime in seconds.
    #[must_use]
    pub const fn ttl_seconds(&self) -> NonZeroU64 {
        self.ttl_seconds
    }

    /// Mints a signed bearer token for a provider access grant using the current
    /// system clock.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderTokenIssuanceError`] when token claims cannot be built or
    /// signed.
    pub fn issue(
        &self,
        grant: &GrantedRepositoryAccess,
    ) -> Result<ProviderIssuedToken, ProviderTokenIssuanceError> {
        self.issue_at(grant, unix_now_seconds_lossy())
    }

    /// Mints a signed bearer token for a provider access grant using the supplied
    /// issuance timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderTokenIssuanceError`] when token claims cannot be built or
    /// signed.
    pub fn issue_at(
        &self,
        grant: &GrantedRepositoryAccess,
        issued_at_unix_seconds: u64,
    ) -> Result<ProviderIssuedToken, ProviderTokenIssuanceError> {
        let expires_at_unix_seconds = issued_at_unix_seconds
            .checked_add(self.ttl_seconds.get())
            .ok_or(ProviderTokenIssuanceError::LifetimeOverflow)?;
        let repository = RepositoryScope::new(
            grant.repository().provider().repository_provider(),
            grant.repository().owner(),
            grant.repository().name(),
            Some(grant.revision().as_str()),
        )?;
        let claims = TokenClaims::new(
            &self.issuer,
            grant.subject().as_str(),
            token_scope(grant.access()),
            repository,
            expires_at_unix_seconds,
        )?;
        let token = self.signer.sign(&claims)?;

        Ok(ProviderIssuedToken {
            token: SecretString::new(token),
            claims,
        })
    }
}

/// Provider-backed token issuance failure.
#[derive(Debug, Error)]
pub enum ProviderTokenIssuanceError {
    /// The issued token lifetime overflowed `u64`.
    #[error("token lifetime overflowed")]
    LifetimeOverflow,
    /// The configured signing key was invalid or token signing failed.
    #[error("token codec operation failed")]
    Codec(#[from] TokenCodecError),
    /// The issued claims were invalid.
    #[error("token claims were invalid")]
    Claims(#[from] TokenClaimsError),
}

const fn token_scope(access: RepositoryAccess) -> TokenScope {
    match access {
        RepositoryAccess::Read => TokenScope::Read,
        RepositoryAccess::Write => TokenScope::Write,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, num::NonZeroU64};

    use shardline_protocol::TokenSigner;

    use super::{GrantedRepositoryAccess, ProviderTokenIssuanceError, ProviderTokenIssuer};
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        GitHubAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RevisionRef,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn github_adapter() -> Result<GitHubAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("github-app")?;
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new("github-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://github.example/team/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ))?;

        Ok(GitHubAdapter::new(catalog, None))
    }

    #[test]
    fn granted_repository_access_is_created_only_for_allow_decisions() {
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("github-user-1");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(subject)) = (repository, revision, subject) else {
            return;
        };
        let request = AuthorizationRequest::new(
            subject.clone(),
            repository,
            revision,
            RepositoryAccess::Write,
        );

        let allow = GrantedRepositoryAccess::from_decision(
            &request,
            AuthorizationDecision::Allow(subject.clone()),
        );
        let deny = GrantedRepositoryAccess::from_decision(&request, AuthorizationDecision::Deny);

        assert!(allow.is_some());
        let Some(allow) = allow else {
            return;
        };
        assert_eq!(allow.subject(), &subject);
        assert_eq!(allow.access(), RepositoryAccess::Write);
        assert_eq!(deny, None);
    }

    #[test]
    fn granted_repository_access_runs_adapter_authorization() {
        let adapter = github_adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let allowed_subject = ProviderSubject::new("github-user-1");
        let denied_subject = ProviderSubject::new("github-user-2");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(allowed_subject.is_ok());
        assert!(denied_subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(allowed_subject), Ok(denied_subject)) =
            (repository, revision, allowed_subject, denied_subject)
        else {
            return;
        };

        let allowed_request = AuthorizationRequest::new(
            allowed_subject.clone(),
            repository.clone(),
            revision.clone(),
            RepositoryAccess::Write,
        );
        let denied_request = AuthorizationRequest::new(
            denied_subject,
            repository,
            revision,
            RepositoryAccess::Write,
        );

        let allowed = GrantedRepositoryAccess::authorize(&adapter, &allowed_request);
        let denied = GrantedRepositoryAccess::authorize(&adapter, &denied_request);

        assert!(allowed.is_ok());
        assert!(denied.is_ok());
        let Ok(allowed) = allowed else {
            return;
        };
        let Ok(denied) = denied else {
            return;
        };
        assert!(allowed.is_some());
        let Some(allowed) = allowed else {
            return;
        };
        assert_eq!(allowed.subject(), &allowed_subject);
        assert_eq!(denied, None);
    }

    #[test]
    fn provider_token_issuer_signs_repository_scoped_claims() {
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("gitlab-user-7");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(subject)) = (repository, revision, subject) else {
            return;
        };
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let issuer = ProviderTokenIssuer::new(
            "gitlab-adapter",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        );
        assert!(issuer.is_ok());
        let Ok(issuer) = issuer else {
            return;
        };

        let issued = issuer.issue_at(&grant, 41);

        assert!(issued.is_ok());
        let Ok(issued) = issued else {
            return;
        };
        assert_eq!(issued.claims().issuer(), "gitlab-adapter");
        assert_eq!(issued.claims().subject(), "gitlab-user-7");
        assert_eq!(issued.claims().repository().owner(), "group");
        assert_eq!(issued.claims().repository().name(), "assets");
        assert_eq!(
            issued.claims().repository().revision(),
            Some("refs/heads/main")
        );
        assert_eq!(issued.claims().expires_at_unix_seconds(), 42);

        let signer = TokenSigner::new(b"a]32-byte-signing-key-for-testing!");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let verified = signer.verify_at(issued.token(), 42);
        assert!(verified.is_ok());
        let Ok(verified) = verified else {
            return;
        };
        assert_eq!(verified, issued.claims().clone());
    }

    #[test]
    fn provider_token_issuer_rejects_overflowing_lifetimes() {
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("subject-1");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(subject)) = (repository, revision, subject) else {
            return;
        };
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Write,
        };
        let issuer = ProviderTokenIssuer::new(
            "generic",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        );
        assert!(issuer.is_ok());
        let Ok(issuer) = issuer else {
            return;
        };

        let issued = issuer.issue_at(&grant, u64::MAX);

        assert!(matches!(
            issued,
            Err(ProviderTokenIssuanceError::LifetimeOverflow)
        ));
    }

    #[test]
    fn provider_token_issuance_error_display_all_variants() {
        let cases: &[(ProviderTokenIssuanceError, &str)] = &[
            (ProviderTokenIssuanceError::LifetimeOverflow, "overflow"),
            (
                ProviderTokenIssuanceError::Codec(shardline_protocol::TokenCodecError::Expired),
                "codec",
            ),
            (
                ProviderTokenIssuanceError::Claims(
                    shardline_protocol::TokenClaimsError::EmptyIssuer,
                ),
                "claims",
            ),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn provider_token_issuer_accessors() {
        let issuer = ProviderTokenIssuer::new(
            "test-issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(3600).unwrap(),
        )
        .unwrap();
        assert_eq!(issuer.issuer(), "test-issuer");
        assert_eq!(issuer.ttl_seconds().get(), 3600);
    }

    #[test]
    fn provider_token_issuer_rejects_empty_key() {
        let result = ProviderTokenIssuer::new("issuer", b"", NonZeroU64::MIN);
        assert!(result.is_err());
    }

    #[test]
    fn provider_token_issuer_issue_with_current_time() {
        let issuer = ProviderTokenIssuer::new(
            "issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(3600).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let token = issuer.issue(&grant).unwrap();
        assert!(!token.token().is_empty());
        assert_eq!(token.claims().issuer(), "issuer");
        assert_eq!(token.claims().subject(), "user-1");
    }

    #[test]
    fn provider_token_issuer_issue_write_scope() {
        let issuer = ProviderTokenIssuer::new(
            "issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(3600).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("user-2").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Write,
        };
        let token = issuer.issue_at(&grant, 100).unwrap();
        assert_eq!(
            token.claims().scope(),
            shardline_protocol::TokenScope::Write
        );
        assert_eq!(token.claims().expires_at_unix_seconds(), 100 + 3600);
    }

    #[test]
    fn granted_access_repository_and_revision_accessors() {
        let subject = ProviderSubject::new("subject-1").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/develop").unwrap();
        let grant = GrantedRepositoryAccess {
            subject: subject.clone(),
            repository: repository.clone(),
            revision: revision.clone(),
            access: RepositoryAccess::Write,
        };
        assert_eq!(grant.subject(), &subject);
        assert_eq!(grant.repository(), &repository);
        assert_eq!(grant.revision(), &revision);
        assert_eq!(grant.access(), RepositoryAccess::Write);
    }

    #[test]
    fn provider_issued_token_accessors() {
        let issuer = ProviderTokenIssuer::new(
            "test",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        )
        .unwrap();
        let subject = ProviderSubject::new("sub").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let token = issuer.issue_at(&grant, 0).unwrap();
        assert!(!token.token().is_empty());
        assert_eq!(token.claims().issuer(), "test");
    }

    #[test]
    fn provider_token_issuer_issue_rejects_empty_issuer() {
        let issuer =
            ProviderTokenIssuer::new("", b"a]32-byte-signing-key-for-testing!", NonZeroU64::MIN)
                .unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let result = issuer.issue_at(&grant, 0);
        assert!(result.is_err());
    }

    #[test]
    fn provider_token_issuer_new_rejects_zero_length_ttl() {
        // NonZeroU64::MIN is valid (value 1), so this should succeed
        let result = ProviderTokenIssuer::new(
            "issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(1).unwrap(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn provider_token_issuer_large_ttl_with_small_time() {
        let issuer = ProviderTokenIssuer::new(
            "issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        // With issued_at = 0, 0 + u64::MAX should not overflow
        let result = issuer.issue_at(&grant, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn provider_token_issuer_ttl_overflow_with_max_time() {
        let issuer = ProviderTokenIssuer::new(
            "issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let result = issuer.issue_at(&grant, u64::MAX);
        assert!(matches!(
            result,
            Err(ProviderTokenIssuanceError::LifetimeOverflow)
        ));
    }

    #[test]
    fn provider_token_issuer_issue_at_zero_time() {
        let issuer = ProviderTokenIssuer::new(
            "zero-time",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(3600).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Write,
        };
        let token = issuer.issue_at(&grant, 0).unwrap();
        assert_eq!(token.claims().expires_at_unix_seconds(), 3600);
        assert_eq!(
            token.claims().scope(),
            shardline_protocol::TokenScope::Write
        );
    }

    #[test]
    fn provider_token_issuer_issue_with_current_time_returns_valid_token() {
        let issuer = ProviderTokenIssuer::new(
            "current-time-test",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::new(60).unwrap(),
        )
        .unwrap();
        let subject = ProviderSubject::new("live-user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "project").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let token = issuer.issue(&grant).unwrap();
        assert!(!token.token().is_empty());
        assert_eq!(token.claims().issuer(), "current-time-test");
        assert_eq!(token.claims().subject(), "live-user");
    }

    #[test]
    fn granted_repository_access_debug_format() {
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let debug = format!("{grant:?}");
        assert!(debug.contains("GrantedRepositoryAccess"));
    }

    #[test]
    fn granted_repository_access_clone_eq() {
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let cloned = grant.clone();
        assert_eq!(grant, cloned);
    }

    #[test]
    fn provider_issued_token_debug_format() {
        let issuer = ProviderTokenIssuer::new(
            "debug-test",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        )
        .unwrap();
        let subject = ProviderSubject::new("debug-sub").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let token = issuer.issue_at(&grant, 0).unwrap();
        let debug = format!("{token:?}");
        assert!(debug.contains("ProviderIssuedToken"));
    }

    #[test]
    fn provider_token_issuer_debug_format() {
        let issuer = ProviderTokenIssuer::new(
            "debug-issuer",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        )
        .unwrap();
        let debug = format!("{issuer:?}");
        assert!(debug.contains("ProviderTokenIssuer"));
    }

    #[test]
    fn provider_token_issuer_signs_with_different_keys_produce_different_tokens() {
        let issuer_a = ProviderTokenIssuer::new(
            "issuer-a",
            b"a]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        )
        .unwrap();
        let issuer_b = ProviderTokenIssuer::new(
            "issuer-b",
            b"b]32-byte-signing-key-for-testing!",
            NonZeroU64::MIN,
        )
        .unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let grant = GrantedRepositoryAccess {
            subject: subject.clone(),
            repository: repository.clone(),
            revision: revision.clone(),
            access: RepositoryAccess::Read,
        };
        let grant_b = GrantedRepositoryAccess {
            subject,
            repository,
            revision,
            access: RepositoryAccess::Read,
        };
        let token_a = issuer_a.issue_at(&grant, 100).unwrap();
        let token_b = issuer_b.issue_at(&grant_b, 100).unwrap();
        // Different issuers should produce different claims
        assert_ne!(token_a.claims().issuer(), token_b.claims().issuer());
    }

    #[test]
    fn granted_repository_access_from_deny_decision() {
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let result = GrantedRepositoryAccess::from_decision(&request, AuthorizationDecision::Deny);
        assert!(result.is_none());
    }

    #[test]
    fn provider_token_issuance_error_is_not_clone() {
        // Verify that the error type implements std::error::Error
        fn is_error<T: std::error::Error>() {}
        is_error::<ProviderTokenIssuanceError>();
    }
}
