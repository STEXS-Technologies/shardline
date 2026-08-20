//! Typed, repo-scoped authorization capabilities.
//!
//! An [`AuthorizedRepository`] is the only handle through which repository-scoped
//! storage operations will be permitted. It is deliberately **sealed**: its fields
//! are private and, apart from the `#[doc(hidden)]` seams reserved for the
//! auth-layer extractors, it has no public constructor, so a forged [`AuthContext`]
//! (whose [`AuthContext::new`] is `pub const` and accepts bare [`TokenClaims`])
//! cannot mint one. The sole **public** mint path is
//! [`AuthorizedRepository::verify_and_authorize`], which re-verifies the bearer
//! token against an [`AuthProvider`] and enforces the required scope before a
//! capability can exist.
//!
//! # Seal seam (type-enforced)
//!
//! The seal is a *type-level guarantee*, not a convention:
//! [`Self::from_verified_context`] consumes a [`VerifiedAuthContext`] — a type
//! that **only** the auth layer can mint. Its constructor is `pub(crate)`
//! inside `shardline-auth`, and the sole production path to one is
//! [`AuthProvider::verify_verified`], which wraps claims a provider just
//! verified. A forged [`AuthContext`] (bare `TokenClaims` through
//! [`AuthContext::new`]) cannot be converted to a [`VerifiedAuthContext`], so
//! the seam is unreachable with unverified claims: no crate outside the auth
//! layer can mint a capability from a hand-constructed context.

// `AuthContext` is referenced only by intra-doc links below (it is the
// forgeable value type the seal protects against); rustc does not count
// doc-link usage for the unused-imports lint, so keep it explicitly.
#[allow(unused_imports)]
use shardline_auth::{AuthContext, AuthError, AuthProvider, VerifiedAuthContext};
use shardline_protocol::{RepositoryScope, TokenClaims, TokenScope};

/// A verified, scope-checked capability authorizing access to a single repository.
///
/// A capability may carry the verified [`TokenClaims`] it was minted from, or no
/// claims at all: permissive deployments (no auth provider configured) mint
/// [`Self::anonymous_full_access`] capabilities whose [`Self::namespace`] is
/// `None`, reproducing today's `scope_namespace(None)` global (unscoped) namespace
/// behavior exactly.
///
/// # Sealing
///
/// This type is sealed by construction: its fields are private and the only
/// non-hidden constructor is [`Self::verify_and_authorize`]. The
/// [`Self::from_verified_context`] seam takes a [`VerifiedAuthContext`], which
/// **only the auth layer can mint** (see the module docs), so
/// `AuthContext::new` (which is `pub const` and takes bare claims) cannot be
/// used to mint a capability — there is no `From<AuthContext>` or
/// `From<TokenClaims>` conversion into a `VerifiedAuthContext`, so any attempt
/// is rejected at compile time:
///
/// ```compile_fail
/// use shardline_server_core::{AuthContext, auth::VerifiedAuthContext};
/// use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
///
/// let repo = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None).unwrap();
/// let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, u64::MAX).unwrap();
/// let forged = AuthContext::new(claims);
/// // A forged AuthContext is a valid value type, but there is deliberately no
/// // conversion to VerifiedAuthContext — only a provider's verification (code
/// // inside shardline-auth) can mint one, so the capability seam is
/// // unreachable from a forged context:
/// let _verified: VerifiedAuthContext = forged.into();
/// ```
///
/// This compile-fail guarantee is the **type-enforced** replacement for the
/// old convention-only seal: the `#[doc(hidden)]` seam
/// [`Self::from_verified_context`] now consumes a [`VerifiedAuthContext`], so a
/// hand-constructed [`AuthContext`] — or any other code path that skips a
/// provider verification — cannot produce a capability (see the module docs on
/// the seal seam).
///
/// The remaining constructors ([`Self::from_verified_context`] and
/// [`Self::anonymous_full_access`]) are `#[doc(hidden)]` seams consumed by the
/// auth-layer extractors only; they are not part of the public, handler-facing
/// API. Repository-scoped storage will later require `&AuthorizedRepository`
/// (instead of bare claims) as the proof of authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedRepository {
    /// Verified claims this capability was minted from, or `None` for
    /// anonymous (permissive-mode) capabilities.
    claims: Option<TokenClaims>,
    /// Granted scope; always a real scope even when `claims` is `None`.
    scope: TokenScope,
}

impl AuthorizedRepository {
    /// Verifies a bearer token and, when the granted scope is sufficient, returns
    /// a repository-scoped authorization capability.
    ///
    /// This is the **only** public mint path for [`AuthorizedRepository`]. It
    /// delegates signature verification to [`AuthProvider::verify_token`] and then
    /// enforces `required_scope` via [`scope_allows`]; a token whose granted scope
    /// does not cover `required_scope` yields [`AuthError::InsufficientScope`].
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the token is invalid, expired, or does not grant
    /// the required scope.
    pub fn verify_and_authorize(
        provider: &dyn AuthProvider,
        token: &str,
        required_scope: TokenScope,
    ) -> Result<Self, AuthError> {
        let claims = provider.verify_token(token)?;
        if !scope_allows(claims.scope(), required_scope) {
            return Err(AuthError::InsufficientScope);
        }
        let scope = claims.scope();
        Ok(Self {
            claims: Some(claims),
            scope,
        })
    }

    /// Auth-layer extractor seam: builds a capability from a
    /// [`VerifiedAuthContext`] that ALREADY came out of a real auth-provider
    /// verification (via [`AuthProvider::verify_verified`], reached through
    /// `ServerAuth::authorize` / `authorize_s3` / the Hub authorize helpers).
    ///
    /// Applies the same [`scope_allows`] gate as [`Self::verify_and_authorize`];
    /// a context whose granted scope does not cover `required_scope` yields
    /// [`AuthError::InsufficientScope`].
    ///
    /// # Type-level seal
    ///
    /// This seam takes a [`VerifiedAuthContext`] rather than an [`AuthContext`]:
    /// a `VerifiedAuthContext` can only be constructed by code inside
    /// `shardline-auth` (a provider's [`AuthProvider::verify_verified`]), so a
    /// forged [`AuthContext`] built with [`AuthContext::new`] over bare
    /// [`TokenClaims`] cannot be converted to one and can never reach this
    /// seam. This is a compile-time guarantee, not a convention.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InsufficientScope`] when the context's granted scope
    /// does not cover `required_scope`.
    #[doc(hidden)]
    pub fn from_verified_context(
        ctx: VerifiedAuthContext,
        required_scope: TokenScope,
    ) -> Result<Self, AuthError> {
        let claims = ctx.into_claims();
        if !scope_allows(claims.scope(), required_scope) {
            return Err(AuthError::InsufficientScope);
        }
        let scope = claims.scope();
        Ok(Self {
            claims: Some(claims),
            scope,
        })
    }

    /// Permissive-mode seam for extractors when no auth provider is configured.
    ///
    /// Carries no claims ([`Self::claims`], [`Self::repository`] and
    /// [`Self::namespace`] are all `None`) and grants [`TokenScope::Write`];
    /// [`Self::namespace`] is `None`, reproducing today's
    /// `scope_namespace(None)` global namespace exactly.
    ///
    /// # Contract
    ///
    /// Do NOT call from handlers; reserved for the auth-layer extractors.
    #[doc(hidden)]
    #[must_use]
    pub const fn anonymous_full_access() -> Self {
        Self {
            claims: None,
            scope: TokenScope::Write,
        }
    }

    /// Returns the verified token claims, if this capability was minted from a
    /// token (i.e. it is not an anonymous full-access capability).
    #[must_use]
    pub const fn claims(&self) -> Option<&TokenClaims> {
        self.claims.as_ref()
    }

    /// Returns the authorized repository scope, or `None` for anonymous
    /// (permissive-mode) capabilities.
    #[must_use]
    pub const fn repository(&self) -> Option<&RepositoryScope> {
        match &self.claims {
            Some(claims) => Some(claims.repository()),
            None => None,
        }
    }

    /// Alias for [`Self::repository`] — the namespace this capability resolves to.
    ///
    /// `None` resolves to the global (unscoped) namespace, exactly like
    /// `scope_namespace(None)`.
    #[must_use]
    pub const fn namespace(&self) -> Option<&RepositoryScope> {
        self.repository()
    }

    /// Returns the authorized repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> Option<&str> {
        self.repository().map(RepositoryScope::owner)
    }

    /// Returns the authorized repository name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.repository().map(RepositoryScope::name)
    }

    /// Returns the granted token scope.
    #[must_use]
    pub const fn scope(&self) -> TokenScope {
        self.scope
    }

    /// Returns true when this capability permits read operations.
    #[must_use]
    pub const fn allows_read(&self) -> bool {
        self.scope.allows_read()
    }

    /// Returns true when this capability permits write operations.
    #[must_use]
    pub const fn allows_write(&self) -> bool {
        self.scope.allows_write()
    }
}

/// Returns whether `actual_scope` satisfies the `required_scope`.
///
/// Read operations require a read-capable scope; write operations require a
/// write-capable scope (which implies read capability).
#[must_use]
pub const fn scope_allows(actual_scope: TokenScope, required_scope: TokenScope) -> bool {
    match required_scope {
        TokenScope::Read => actual_scope.allows_read(),
        TokenScope::Write => actual_scope.allows_write(),
    }
}

#[cfg(test)]
mod tests {
    use shardline_auth::{AuthError, AuthProvider, LocalHmacProvider, VerifiedAuthContext};
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

    use super::{AuthorizedRepository, scope_allows};

    const VALID_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";
    const OTHER_KEY: &[u8] = b"another-signing-key-32-bytes-ok!";

    fn claims(scope: TokenScope) -> TokenClaims {
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))
            .unwrap();
        TokenClaims::new("local", "provider-user-1", scope, repo, u64::MAX).unwrap()
    }

    fn mint(provider: &LocalHmacProvider, scope: TokenScope) -> String {
        provider.mint_token(&claims(scope)).unwrap()
    }

    /// Verifies a freshly minted token through the provider, mirroring the
    /// auth-provider verification step performed by the server's authorize
    /// paths before `from_verified_context` is reached.
    fn verified_context(provider: &LocalHmacProvider, scope: TokenScope) -> VerifiedAuthContext {
        let token = mint(provider, scope);
        provider.verify_verified(&token).expect("token verifies")
    }

    // ── verify_and_authorize ─────────────────────────────────────────────

    #[test]
    fn verify_and_authorize_mints_capability_from_valid_token() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let token = mint(&provider, TokenScope::Write);

        let capability =
            AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Write);

        assert!(capability.is_ok());
        let Ok(capability) = capability else {
            return;
        };
        let claims = capability.claims().expect("capability carries claims");
        assert_eq!(claims.subject(), "provider-user-1");
        assert_eq!(claims.scope(), TokenScope::Write);
        let repo = capability.repository().expect("capability carries a repo");
        assert_eq!(repo.owner(), "acme");
        assert_eq!(repo.name(), "assets");
        assert_eq!(repo.revision(), Some("main"));
        assert_eq!(capability.namespace(), capability.repository());
        assert_eq!(capability.owner(), Some("acme"));
        assert_eq!(capability.name(), Some("assets"));
        assert_eq!(capability.scope(), TokenScope::Write);
        assert!(capability.allows_read());
        assert!(capability.allows_write());
    }

    #[test]
    fn verify_and_authorize_rejects_insufficient_scope() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let token = mint(&provider, TokenScope::Read);

        let result =
            AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Write);

        assert!(matches!(result, Err(AuthError::InsufficientScope)));
    }

    #[test]
    fn verify_and_authorize_rejects_invalid_token() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();

        let result = AuthorizedRepository::verify_and_authorize(
            &provider,
            "garbage-token",
            TokenScope::Read,
        );

        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    #[test]
    fn verify_and_authorize_rejects_token_signed_with_different_key() {
        let minting = LocalHmacProvider::new(VALID_KEY).unwrap();
        let verifying = LocalHmacProvider::new(OTHER_KEY).unwrap();
        let token = mint(&minting, TokenScope::Write);

        let result =
            AuthorizedRepository::verify_and_authorize(&verifying, &token, TokenScope::Write);

        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    // ── from_verified_context ────────────────────────────────────────────

    #[test]
    fn from_verified_context_mints_capability_with_matching_repo_and_claims() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let ctx = verified_context(&provider, TokenScope::Write);

        let capability = AuthorizedRepository::from_verified_context(ctx, TokenScope::Write);

        let Ok(capability) = capability else {
            panic!("expected Ok capability");
        };
        let claims = capability.claims().expect("capability carries claims");
        assert_eq!(claims.subject(), "provider-user-1");
        assert_eq!(claims.scope(), TokenScope::Write);
        let repo = capability.repository().expect("capability carries a repo");
        assert_eq!(repo.owner(), "acme");
        assert_eq!(repo.name(), "assets");
        assert_eq!(capability.owner(), Some("acme"));
        assert_eq!(capability.name(), Some("assets"));
        assert_eq!(capability.scope(), TokenScope::Write);
        assert!(capability.allows_write());
    }

    #[test]
    fn from_verified_context_rejects_insufficient_scope() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let ctx = verified_context(&provider, TokenScope::Read);

        let result = AuthorizedRepository::from_verified_context(ctx, TokenScope::Write);

        assert!(matches!(result, Err(AuthError::InsufficientScope)));
    }

    // ── anonymous_full_access ────────────────────────────────────────────

    #[test]
    fn anonymous_full_access_resolves_to_global_namespace() {
        let capability = AuthorizedRepository::anonymous_full_access();

        assert_eq!(capability.claims(), None);
        assert_eq!(capability.repository(), None);
        // Permissive storage keying must reproduce scope_namespace(None): None.
        assert_eq!(capability.namespace(), None);
        assert_eq!(capability.owner(), None);
        assert_eq!(capability.name(), None);
        assert_eq!(capability.scope(), TokenScope::Write);
        assert!(capability.allows_read());
        assert!(capability.allows_write());
    }

    // ── scope_allows ─────────────────────────────────────────────────────

    #[test]
    fn scope_allows_read_when_scope_is_read() {
        assert!(scope_allows(TokenScope::Read, TokenScope::Read));
    }

    #[test]
    fn scope_allows_write_when_scope_is_write() {
        assert!(scope_allows(TokenScope::Write, TokenScope::Write));
    }

    #[test]
    fn scope_allows_read_when_scope_is_write() {
        // Write scope implicitly allows Read
        assert!(scope_allows(TokenScope::Write, TokenScope::Read));
    }

    #[test]
    fn scope_allows_rejects_write_when_scope_is_read() {
        assert!(!scope_allows(TokenScope::Read, TokenScope::Write));
    }
}
