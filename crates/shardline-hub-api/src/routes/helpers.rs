use axum::extract::FromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;

use crate::error::HubApiError;
use crate::models::RepoType;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;
use shardline_server_core::{AuthorizedRepository, VerifiedAuthContext};

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
/// `Ok(Some(VerifiedAuthContext))` when a valid token was presented. This lets
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
) -> Result<Option<VerifiedAuthContext>, HubApiError> {
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
    auth_ctx: Option<&VerifiedAuthContext>,
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
    // `/api/{type}/{ns}/{repo}` — the path ends at the repository. Prefer this
    // shape FIRST: it is the single source of truth for a repository that ends
    // the path, and it must win even when the repo's name collides with a route
    // verb (e.g. `/api/models/ns/commit` is the repository `ns/commit`, NOT a
    // commit of `models/ns`). The segment before the pair must be a repo type
    // to distinguish this from pathless route families (`/lfs/objects/{oid}`,
    // `/api/repos`, ...).
    if let [repo_type, ns, repo] = segments.get(segments.len().checked_sub(3)?..)?
        && REPO_TYPE_SEGMENTS.contains(repo_type)
    {
        return Some((ns.to_string(), repo.to_string()));
    }
    // `(ns, repo)` is the pair immediately preceding a known verb segment. This
    // is only reached when the path does NOT end at the repository. Scan
    // BACKWARD and take the LAST verb-followed window: the router's verbs
    // always directly follow the `(ns, repo)` pair, so the last window wins
    // even when an earlier window would also end in a verb — e.g. a repo named
    // `commit` at `/api/models/ns/commit/commit/{rev}` must bind to
    // `ns/commit`, not to `models/ns` from the `[models, ns, commit]` window.
    for window in segments.windows(3).rev() {
        let [ns, repo, verb] = window else { continue };
        if REPO_VERB_SEGMENTS.contains(verb) {
            return Some((ns.to_string(), repo.to_string()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::extract_repo_path;

    #[test]
    fn extract_repo_path_repo_named_like_verb_is_parsed_as_repository() {
        // A repository whose name collides with a route verb must parse as the
        // last-3 [type, ns, repo] shape, not as a verb-window over the type.
        for (path, expected) in [
            ("/api/models/ns/commit", ("ns", "commit")),
            ("/api/models/ns/tree", ("ns", "tree")),
            ("/api/models/ns/webhooks", ("ns", "webhooks")),
            ("/api/models/ns/resolve", ("ns", "resolve")),
            ("/api/models/ns/HEAD", ("ns", "HEAD")),
            ("/api/models/ns/info", ("ns", "info")),
            ("/api/models/ns/revisions", ("ns", "revisions")),
            (
                "/api/models/ns/git-receive-pack",
                ("ns", "git-receive-pack"),
            ),
            ("/api/datasets/ns/parquet", ("ns", "parquet")),
        ] {
            let parsed = extract_repo_path(path);
            assert_eq!(
                parsed,
                Some((expected.0.to_owned(), expected.1.to_owned())),
                "path {path} must bind to {expected:?}"
            );
        }
    }

    #[test]
    fn extract_repo_path_verb_after_non_repo_type_segment() {
        // A path that ends at the repository (no trailing verb) still binds to
        // the last-3 shape, even when the repo-type segment is the pair head.
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/datasets/ns/my-repo"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_verb_window_still_works_for_trailing_verbs() {
        // When the path continues past the repository with a real verb, the
        // verb-window rule still resolves the (ns, repo) pair.
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/commit/abc123"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/tree/main/path/file.txt"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/datasets/ns/my-repo/parquet"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/webhooks"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        // Root-level model resolve.
        assert_eq!(
            extract_repo_path("/ns/my-repo/resolve/some/name"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_verb_collision_nested_route_binds_to_repository() {
        // A repo named `commit` used through a route whose verb is also
        // `commit` (`/api/models/{ns}/{repo}/commit/{rev}`): the LAST
        // verb-followed window is `[ns, commit, commit]`, not the type-prefixed
        // `[models, ns, commit]` window.
        assert_eq!(
            extract_repo_path("/api/models/ns/commit/commit/main"),
            Some(("ns".to_owned(), "commit".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/tree/tree/main/a/b"),
            Some(("ns".to_owned(), "tree".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/webhooks/webhooks/abc"),
            Some(("ns".to_owned(), "webhooks".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/revisions/revisions"),
            Some(("ns".to_owned(), "revisions".to_owned()))
        );
        // Root-level git smart-HTTP for a repo named `commit`.
        assert_eq!(
            extract_repo_path("/models/ns/commit/git-receive-pack"),
            Some(("ns".to_owned(), "commit".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_repo_named_like_repo_type_segment() {
        // A repo literally named "models" must still bind as the last pair.
        assert_eq!(
            extract_repo_path("/api/models/ns/models"),
            Some(("ns".to_owned(), "models".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/models/revisions"),
            Some(("ns".to_owned(), "models".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_pathless_routes_return_none() {
        for path in [
            "/objects/batch",
            "/lfs/objects/0123456789abcdef0123456789abcdef",
            "/api/repos",
            "/api/models/search",
            "/api/repos/create",
            "/api/repos/delete",
        ] {
            assert_eq!(extract_repo_path(path), None, "path {path} is pathless");
        }
    }
}
