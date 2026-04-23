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
        let issuer = ProviderTokenIssuer::new("gitlab-adapter", b"signing-key", NonZeroU64::MIN);
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

        let signer = TokenSigner::new(b"signing-key");
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
        let issuer = ProviderTokenIssuer::new("generic", b"signing-key", NonZeroU64::MIN);
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
}
