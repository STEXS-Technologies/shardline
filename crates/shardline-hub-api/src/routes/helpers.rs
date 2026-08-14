use axum::extract::FromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;

use crate::error::HubApiError;
use crate::models::RepoType;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;
use shardline_server_core::{AuthContext, AuthorizedRepository};

use super::HubState;

/// Authorize the request if auth is configured. Returns `Ok(())` when no auth
/// is set (permissive) or when the token satisfies the required scope.
pub(crate) fn authorize(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<(), HubApiError> {
    if let Some(auth) = &state.auth {
        auth.authorize(headers, required_scope)?;
    }
    Ok(())
}

/// Authorize the request and return the verified auth context, when auth is
/// configured.
///
/// Returns `Ok(None)` in permissive mode (no auth configured) and
/// `Ok(Some(AuthContext))` when a valid token was presented. This lets
/// repo-scoped handlers enforce a token→repository binding in addition to the
/// scope check performed by [`authorize`].
///
/// # Errors
///
/// Returns [`HubApiError::Unauthorized`] or [`HubApiError::InvalidToken`] on a
/// missing/invalid token, or [`HubApiError::Forbidden`] on insufficient scope.
pub(crate) fn authorize_with_context(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, HubApiError> {
    if let Some(auth) = &state.auth {
        Ok(Some(auth.authorize(headers, required_scope)?))
    } else {
        Ok(None)
    }
}

/// Requires that the authenticated token's repository scope matches the
/// request's `(ns, repo)` URL-path segment.
///
/// This is the hub-api counterpart to the core layer's
/// `validate_oci_repository_scope` / `scope_namespace` binding: a token issued
/// for repo `owner/name` must only ever act on that same repo. Without this
/// binding, any authenticated user could read/write/delete/git-push another
/// tenant's repository using a token scoped only by `scope` (Read/Write).
///
/// In permissive mode (`auth_ctx` is `None`, i.e. no auth is configured) the
/// check is a no-op so development deployments without auth keep working.
///
/// # Errors
///
/// Returns [`HubApiError::Forbidden`] (HTTP 403) when the token's repository
/// does not match the requested `(ns, repo)`.
pub(crate) fn require_repository_binding(
    auth_ctx: Option<&AuthContext>,
    ns: &str,
    repo: &str,
) -> Result<(), HubApiError> {
    let Some(ctx) = auth_ctx else {
        return Ok(());
    };
    let claims_repo = ctx.claims().repository();
    if claims_repo.owner() == ns && claims_repo.name() == repo {
        Ok(())
    } else {
        Err(HubApiError::Forbidden)
    }
}

/// Converts a `HubRepoType` to the API path string.
///
/// Delegates to [`RepoType::as_path_str`] so the plural path segment mapping
/// lives in a single place.
pub(crate) fn repo_type_path(rt: HubRepoType) -> &'static str {
    RepoType::from(rt).as_path_str()
}

/// Typed, repo-scoped authorization capability extractor.
///
/// This is the **only** way a repo-scoped Hub route handler can obtain an
/// [`AuthorizedRepository`]: it reproduces the exact auth chain the handlers
/// used to perform inline, in the same order:
///
/// 1. parse `(ns, repo)` from the request path segments;
/// 2. [`authorize_with_context`] — 401 on missing/invalid credentials, 403 on
///    insufficient scope (permissive `Ok(None)` when no auth is configured);
/// 3. [`require_repository_binding`] — 403 when the token's repository scope
///    does not match the URL's `(ns, repo)`; no-op when auth is `None`;
/// 4. mint the capability: `Some(ctx)` → [`AuthorizedRepository::from_verified_context`],
///    `None` → [`AuthorizedRepository::anonymous_full_access`] (binding skipped,
///    global namespace — reproducing today's permissive no-op exactly).
///
/// The const generic parameter `WRITE` selects the required scope:
/// `HubRepository` (default, read-scoped) vs `HubRepository<true>` (write-scoped).
/// `BIND` controls whether the token→repository binding is enforced; it is
/// disabled only for the deliberately global repo-create routes.
///
/// Pathless routes (LFS, repo list/search, repo create) carry no `(ns, repo)`
/// in the URL; their isolation comes from the token claims carried by the
/// capability, so the binding is skipped for them.
#[derive(Debug, Clone)]
pub struct HubRepository<const WRITE: bool = false, const BIND: bool = true> {
    /// The verified capability. Handlers must pass this (or its
    /// `repository()`/`claims()`) wherever repo-scoped state is touched.
    pub(crate) capability: AuthorizedRepository,
}

impl<const WRITE: bool, const BIND: bool> HubRepository<WRITE, BIND> {
    const fn required_scope() -> TokenScope {
        if WRITE {
            TokenScope::Write
        } else {
            TokenScope::Read
        }
    }

    /// Runs the extractor chain against already-parsed request parts.
    fn from_request(parts: &mut Parts, state: &HubState) -> Result<Self, HubApiError> {
        let required_scope = Self::required_scope();
        // 1. (ns, repo) from the request path segments, when the route has them.
        let ns_repo = extract_repo_path(parts.uri.path());
        // 2. Authorize (permissive `Ok(None)` when no auth is configured).
        let auth_ctx = authorize_with_context(state, &parts.headers, required_scope)?;
        // 3. Enforce the token→repository binding (no-op when auth is None,
        //    when the route is pathless, or when BIND is disabled).
        if BIND && let Some((ns, repo)) = &ns_repo {
            require_repository_binding(auth_ctx.as_ref(), ns, repo)?;
        }
        // 4. Mint the capability.
        let capability = match auth_ctx {
            Some(ctx) => AuthorizedRepository::from_verified_context(ctx, required_scope)?,
            None => AuthorizedRepository::anonymous_full_access(),
        };
        Ok(Self { capability })
    }

    /// Returns the authorized repository capability.
    #[must_use]
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.capability
    }

    /// Requires that this capability's repository scope matches `ns/repo`.
    ///
    /// This is the capability-based counterpart of [`require_repository_binding`]
    /// for handlers whose repository identity comes from the request body
    /// (e.g. the compat repo-delete endpoint) rather than the URL path. It is a
    /// no-op for anonymous (permissive-mode) capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::Forbidden`] when the capability's repository does
    /// not match `ns/repo`.
    pub(crate) fn require_binding(&self, ns: &str, repo: &str) -> Result<(), HubApiError> {
        let Some(claims) = self.capability.claims() else {
            return Ok(());
        };
        let claims_repo = claims.repository();
        if claims_repo.owner() == ns && claims_repo.name() == repo {
            Ok(())
        } else {
            Err(HubApiError::Forbidden)
        }
    }
}

impl<const WRITE: bool, const BIND: bool> FromRequestParts<HubState>
    for HubRepository<WRITE, BIND>
{
    type Rejection = HubApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HubState,
    ) -> Result<Self, Self::Rejection> {
        Self::from_request(parts, state)
    }
}

/// Known repo-type path segments (singular and plural API forms).
const REPO_TYPE_SEGMENTS: &[&str] = &["model", "models", "dataset", "datasets", "space", "spaces"];

/// Test helper: runs the real authorize path against `state` and mints a
/// capability, mirroring what the [`HubRepository`] extractor does. Permissive
/// states produce an anonymous full-access capability; configured auth
/// providers are exercised through the real `authorize_with_context` chain.
#[cfg(test)]
pub(crate) fn test_repo<const WRITE: bool, const BIND: bool>(
    state: &HubState,
    headers: &HeaderMap,
) -> HubRepository<WRITE, BIND> {
    let required_scope = if WRITE {
        TokenScope::Write
    } else {
        TokenScope::Read
    };
    let ctx = authorize_with_context(state, headers, required_scope).expect("test auth");
    let capability = ctx.map_or_else(AuthorizedRepository::anonymous_full_access, |ctx| {
        AuthorizedRepository::from_verified_context(ctx, required_scope).expect("test scope")
    });
    HubRepository { capability }
}

/// Known "verb" segments that immediately follow a repo-scoped `(ns, repo)`
/// pair in the Hub router's path space.
const REPO_VERB_SEGMENTS: &[&str] = &[
    "revision",
    "modelcard",
    "revisions",
    "preupload",
    "commit",
    "tree",
    "webhooks",
    "resolve",
    "info",
    "HEAD",
    "git-upload-pack",
    "git-receive-pack",
    "parquet",
    "first-rows",
    "viewer",
];

/// Extracts the `(ns, repo)` pair from a repo-scoped request path.
///
/// Recognizes every repo-scoped route family served by the Hub router:
///
/// - `{type}/{ns}/{repo}/<verb>[/...]` and `/api/{type}/{ns}/{repo}/<verb>[/...]`
/// - `/api/datasets/{ns}/{repo}/<verb>[/...]` (no repo-type segment)
/// - `/{ns}/{repo}/resolve/...` (root-level model resolve)
/// - `/api/{type}/{ns}/{repo}` (the path ends at the repository itself)
///
/// Returns `None` for pathless routes (`/objects/batch`, `/lfs/objects/{oid}`,
/// `/api/repos`, `/api/{type}/search`, `/api/repos/create`, `/api/repos/delete`)
/// whose isolation comes from the token claims rather than a URL binding.
fn extract_repo_path(path: &str) -> Option<(String, String)> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // `(ns, repo)` is the pair immediately preceding a known verb segment.
    for window in segments.windows(3) {
        let [ns, repo, verb] = window else { continue };
        if REPO_VERB_SEGMENTS.contains(verb) {
            return Some((ns.to_string(), repo.to_string()));
        }
    }
    // `/api/{type}/{ns}/{repo}` — the path ends at the repository. The segment
    // before the pair must be a repo type to distinguish this from pathless
    // route families (`/lfs/objects/{oid}`, `/api/repos`, ...).
    if let [repo_type, ns, repo] = segments.get(segments.len().checked_sub(3)?..)?
        && REPO_TYPE_SEGMENTS.contains(repo_type)
    {
        return Some((ns.to_string(), repo.to_string()));
    }
    None
}
