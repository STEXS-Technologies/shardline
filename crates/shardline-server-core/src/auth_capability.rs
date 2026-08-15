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
//! # Seal seam (convention-enforced, not type-enforced)
//!
//! The seal is a *convention*, not a type-level guarantee: [`Self::from_verified_context`]
//! is `#[doc(hidden)] pub` and [`AuthContext::new`] is `pub const` over bare claims,
//! so any crate that depends on `shardline-server-core` *could* call the seam
//! directly and mint a capability. There is deliberately **no** cross-crate marker
//! type that would let `from_verified_context` reject an `AuthContext` it cannot
//! prove came from a provider verification. All current call sites feed only
//! provider-verified contexts (produced by `ServerAuth::authorize` /
//! `authorize_s3` after `AuthProvider::verify_token`), so the seam is not
//! exploitable today, but it is a residual risk to close with a type-level seal
//! (a marker only the auth layer can mint) in a future refactor.

use shardline_auth::{AuthContext, AuthError, AuthProvider};
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
/// non-hidden constructor is [`Self::verify_and_authorize`]. `AuthContext::new`
/// (which is `pub const` and takes bare claims) therefore cannot be used to mint a
/// capability — there is no `From<AuthContext>` or `From<TokenClaims>`
/// implementation, so any attempt is rejected at compile time:
///
/// ```compile_fail
/// use shardline_server_core::{AuthContext, auth_capability::AuthorizedRepository};
/// use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
///
/// let repo = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None).unwrap();
/// let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, u64::MAX).unwrap();
/// let forged = AuthContext::new(claims);
/// // No conversion from a forgeable AuthContext exists, so a capability cannot
/// // be minted through the ordinary type system:
/// let _capability: AuthorizedRepository = forged.into();
/// ```
///
/// Note that this compile-fail guarantee only blocks `From<AuthContext>`
/// conversions. The `#[doc(hidden)]` seam [`Self::from_verified_context`]
/// accepts an `AuthContext` directly and is contract-enforced rather than
/// type-enforced: it MUST only be called with a context produced by an
/// auth-provider verification. Minting a capability from unverified,
/// handler-constructed claims — or any other code path — is a vulnerability,
/// not a supported API (see the module docs on the seal seam).
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

    /// Auth-layer extractor seam: builds a capability from an [`AuthContext`]
    /// that ALREADY came out of a real auth-provider verification (via
    /// `ServerAuth::authorize` / `authorize_s3`).
    ///
    /// Applies the same [`scope_allows`] gate as [`Self::verify_and_authorize`];
    /// a context whose granted scope does not cover `required_scope` yields
    /// [`AuthError::InsufficientScope`].
    ///
    /// # Contract — do not violate
    ///
    /// The caller MUST supply a context produced by an auth-provider
    /// verification (`AuthProvider::verify_token`). Minting a capability from
    /// unverified claims — for example by constructing [`AuthContext`] with
    /// [`AuthContext::new`] over bare [`TokenClaims`] and passing it here — is
    /// a vulnerability: it forges authorization for a repository the caller
    /// was never granted. This contract is convention-enforced only (there is
    /// no cross-crate marker that would reject an unverified context at
    /// compile time); every call site in this repository feeds a
    /// provider-verified context. Do not add new call sites that cannot
    /// guarantee provider verification, and close the seam with a type-level
    /// seal if the contract ever needs machine enforcement.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InsufficientScope`] when the context's granted scope
    /// does not cover `required_scope`.
    #[doc(hidden)]
    pub fn from_verified_context(
        ctx: AuthContext,
        required_scope: TokenScope,
    ) -> Result<Self, AuthError> {
        let claims = ctx.claims;
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
    use shardline_auth::{AuthContext, AuthError, AuthProvider, LocalHmacProvider};
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

    /// Verifies a freshly minted token and wraps it in an `AuthContext`, mirroring
    /// the auth-provider verification step performed by the server's authorize
    /// paths before `from_verified_context` is reached.
    fn verified_context(provider: &LocalHmacProvider, scope: TokenScope) -> AuthContext {
        let token = mint(provider, scope);
        let claims = provider.verify_token(&token).expect("token verifies");
        AuthContext::new(claims)
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
