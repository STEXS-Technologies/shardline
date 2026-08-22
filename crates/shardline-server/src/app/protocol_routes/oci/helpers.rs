use std::sync::Arc;

use axum::{
    extract::{FromRequestParts, Path},
    http::{HeaderMap, Method, request::Parts},
};
use shardline_index::{OciObjectKey as OciIndexObjectKey, OciObjectKind};
use shardline_protocol::TokenScope;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectKey, ObjectPrefix};

use crate::{ServerError, error::OciError, protocol_support::validate_oci_repository_scope};

use super::super::AppState;
use super::path::{OciPath, parse_oci_path};
use super::token::oci_authorize;

/// A typed, repository-scoped OCI authorization capability extracted by axum.
///
/// `OciRepository` is the **only** way an OCI handler may obtain an
/// [`AuthorizedRepository`]: its [`FromRequestParts`] implementation reproduces
/// the exact OCI authorization chain — OCI path parse ([`parse_oci_path`]) →
/// bearer authorization + scope check ([`oci_authorize`], preserving the 401
/// challenge / 403 scope mapping) → requested-repository ↔ claims binding
/// ([`validate_oci_repository_scope`], mismatch → 4xx as before) — in the same
/// order as the pre-refactor `oci_dispatch_parsed` + `oci_authorize` calls, and
/// then mints the capability from the already-verified context. A handler that
/// does not carry this extractor cannot reach repository-scoped storage at all.
pub(crate) struct OciRepository {
    /// The parsed OCI request path (repository + operation-specific segments).
    path: OciPath,
    /// The verified, scope-checked capability backing this extractor.
    capability: AuthorizedRepository,
}

impl OciRepository {
    /// The parsed OCI request path.
    #[must_use]
    pub(crate) const fn path(&self) -> &OciPath {
        &self.path
    }

    /// The verified, scope-checked capability backing this extractor.
    #[must_use]
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.capability
    }

    /// The requested repository from the OCI path.
    #[must_use]
    pub(crate) fn repository(&self) -> &str {
        self.path.repository()
    }

    /// Reproduces the OCI authorization + binding chain and mints a capability
    /// with the given required scope.
    ///
    /// 1. Parse the OCI path ([`parse_oci_path`], unchanged: unknown paths and
    ///    invalid repositories/digests fail exactly as the dispatcher's parse
    ///    step did).
    /// 2. Determine the required scope for the `(method, path)` pair. Method +
    ///    path combinations that no OCI handler serves return `NotFound`
    ///    WITHOUT touching authorization — exactly like the pre-refactor
    ///    `_ => Err(ServerError::NotFound)` dispatch arm.
    /// 3. Authorize ([`oci_authorize`] unchanged: 401 challenge for
    ///    missing/invalid tokens, 403 for insufficient scope, `Ok(None)` for
    ///    permissive deployments without an auth provider).
    /// 4. Bind the requested repository to the token claims before minting
    ///    ([`validate_oci_repository_scope`], unchanged: `None` claims pass,
    ///    mismatch → `NotFound` 4xx as today).
    /// 5. Mint the capability: `Some(context)` (already verified by the auth
    ///    layer) → [`AuthorizedRepository::from_verified_context`] (no token is
    ///    re-verified; the scope gate is re-applied idempotently); `None`
    ///    (permissive) → [`AuthorizedRepository::anonymous_full_access`], whose
    ///    `None` namespace resolves to the global namespace exactly like
    ///    `scope_namespace(None)`.
    fn authorize(
        state: &Arc<AppState>,
        headers: &HeaderMap,
        _method: &Method,
        parsed: OciPath,
        required_scope: TokenScope,
    ) -> Result<Self, OciError> {
        let repository = parsed.repository();
        let auth = oci_authorize(state, headers, Some(repository), required_scope)?;
        validate_oci_repository_scope(
            repository,
            auth.as_ref().map(|context| context.claims().repository()),
        )?;

        let capability = match auth {
            Some(context) => {
                // The verified context (minted by the auth layer's
                // `verify_verified`) flows straight into the capability seam;
                // `from_verified_context` only re-applies the scope gate
                // idempotently.
                AuthorizedRepository::from_verified_context(context, required_scope)
                    .map_err(ServerError::from)?
            }
            None => AuthorizedRepository::anonymous_full_access(),
        };
        Ok(Self {
            path: parsed,
            capability,
        })
    }
}

impl FromRequestParts<Arc<AppState>> for OciRepository {
    type Rejection = OciError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        // The OCI wildcard route is `/v2/{*path}`; extract the same `{*path}`
        // value the pre-refactor dispatchers received, then parse it.
        let path: String = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|_error| OciError::from(ServerError::NotFound))?
            .0;
        let parsed = parse_oci_path(&path)?;

        // The required scope follows every dispatch arm's per-method mapping
        // (blob/manifest/tags reads → Read; uploads, session mutations,
        // manifest/blob writes → Write). Unserved method + path combos return
        // NotFound without touching authorization, matching the pre-refactor
        // `_ => Err(ServerError::NotFound)` arm.
        let required_scope = oci_required_scope(&parts.method, &parsed)
            .ok_or_else(|| OciError::from(ServerError::NotFound))?;

        Self::authorize(state, &parts.headers, &parts.method, parsed, required_scope)
    }
}

/// Maps an OCI `(method, path)` pair to the token scope required to serve it.
///
/// Mirrors the per-arm scope choices of the pre-refactor dispatcher:
/// `None` means the combination is not served by any OCI handler (→ 404).
const fn oci_required_scope(method: &Method, path: &OciPath) -> Option<TokenScope> {
    match (method, path) {
        (&Method::GET | &Method::HEAD, OciPath::Blob { .. } | OciPath::Manifest { .. }) => {
            Some(TokenScope::Read)
        }
        (&Method::GET, OciPath::TagsList { .. }) => Some(TokenScope::Read),
        (&Method::DELETE, OciPath::Blob { .. } | OciPath::Manifest { .. }) => {
            Some(TokenScope::Write)
        }
        (&Method::PUT, OciPath::Manifest { .. }) => Some(TokenScope::Write),
        (&Method::POST, OciPath::BlobUploads { .. }) => Some(TokenScope::Write),
        (
            &Method::PATCH | &Method::PUT | &Method::GET | &Method::DELETE,
            OciPath::BlobUploadSession { .. },
        ) => Some(TokenScope::Write),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Capability-keyed storage key derivation.
//
// These wrappers are the only way OCI handlers derive repository-scoped
// storage keys: they take an `&AuthorizedRepository` (produced by the auth
// layer extractor) instead of a bare `Option<&RepositoryScope>` and derive the
// storage namespace via `scope_namespace(auth.namespace())` — `None` (the
// anonymous/permissive capability) resolves to the global namespace exactly
// like `scope_namespace(None)` did before.
// ---------------------------------------------------------------------------

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub(crate) fn oci_blob_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, ServerError> {
    crate::oci_adapter::oci_blob_key(repository, digest_hex, auth.namespace())
        .map_err(ServerError::from)
}

/// Builds the durable logical-visibility identity for an OCI blob.
pub(crate) fn oci_blob_index_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
) -> OciIndexObjectKey {
    oci_index_object_key(repository, digest_hex, auth, OciObjectKind::Blob)
}

/// Builds the durable logical-visibility identity for an OCI manifest.
pub(crate) fn oci_manifest_index_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
) -> OciIndexObjectKey {
    oci_index_object_key(repository, digest_hex, auth, OciObjectKind::Manifest)
}

fn oci_index_object_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
    kind: OciObjectKind,
) -> OciIndexObjectKey {
    OciIndexObjectKey {
        scope_namespace: crate::protocol_support::scope_namespace(auth.namespace()),
        repository: repository.to_owned(),
        kind,
        digest_hex: digest_hex.to_owned(),
    }
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub(crate) fn oci_manifest_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, ServerError> {
    crate::oci_adapter::oci_manifest_key(repository, digest_hex, auth.namespace())
        .map_err(ServerError::from)
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub(crate) fn oci_manifest_media_type_key(
    repository: &str,
    digest_hex: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, ServerError> {
    crate::oci_adapter::oci_manifest_media_type_key(repository, digest_hex, auth.namespace())
        .map_err(ServerError::from)
}

/// # Errors
///
/// Returns an error when the repository, tag, or scope is invalid.
pub(crate) fn oci_tag_key(
    repository: &str,
    tag: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, ServerError> {
    crate::oci_adapter::oci_tag_key(repository, tag, auth.namespace()).map_err(ServerError::from)
}

/// # Errors
///
/// Returns [`ServerError`] when the repository name or scope is invalid.
pub(crate) fn oci_manifest_prefix(
    repository: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectPrefix, ServerError> {
    crate::oci_adapter::oci_manifest_prefix(repository, auth.namespace()).map_err(ServerError::from)
}

/// # Errors
///
/// Returns [`ServerError`] when the repository name or scope is invalid.
pub(crate) fn oci_tag_prefix(
    repository: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectPrefix, ServerError> {
    crate::oci_adapter::oci_tag_prefix(repository, auth.namespace()).map_err(ServerError::from)
}

pub(crate) const fn oci_route_served_by_api(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Manifest { .. } | OciPath::TagsList { .. }
        ) | (&Method::HEAD, OciPath::Manifest { .. })
            | (&Method::PUT, OciPath::Manifest { .. })
            | (&Method::DELETE, OciPath::Manifest { .. })
            | (&Method::DELETE, OciPath::Blob { .. })
    )
}

pub(crate) const fn oci_route_served_by_transfer(method: &Method, path: &OciPath) -> bool {
    matches!(
        (method, path),
        (
            &Method::GET,
            OciPath::Blob { .. } | OciPath::BlobUploadSession { .. }
        ) | (&Method::HEAD, OciPath::Blob { .. })
            | (&Method::POST, OciPath::BlobUploads { .. })
            | (&Method::PATCH, OciPath::BlobUploadSession { .. })
            | (&Method::PUT, OciPath::BlobUploadSession { .. })
            | (&Method::DELETE, OciPath::BlobUploadSession { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::protocol_routes::oci::path::OciPath;

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const SESSION: &str = "0000000000000001";
    const REPO: &str = "team/assets";

    fn blob_path() -> OciPath {
        OciPath::Blob {
            repository: REPO.to_owned(),
            digest_hex: DIGEST.to_owned(),
        }
    }

    fn manifest_path() -> OciPath {
        OciPath::Manifest {
            repository: REPO.to_owned(),
            reference: "v1".to_owned(),
        }
    }

    fn blob_uploads_path() -> OciPath {
        OciPath::BlobUploads {
            repository: REPO.to_owned(),
        }
    }

    fn blob_upload_session_path() -> OciPath {
        OciPath::BlobUploadSession {
            repository: REPO.to_owned(),
            session_id: SESSION.to_owned(),
        }
    }

    fn tags_list_path() -> OciPath {
        OciPath::TagsList {
            repository: REPO.to_owned(),
        }
    }

    // ── oci_route_served_by_api ────────────────────────────────────────

    #[test]
    fn api_serves_get_manifest() {
        assert!(oci_route_served_by_api(&Method::GET, &manifest_path()));
    }

    #[test]
    fn api_serves_head_manifest() {
        assert!(oci_route_served_by_api(&Method::HEAD, &manifest_path()));
    }

    #[test]
    fn api_serves_put_manifest() {
        assert!(oci_route_served_by_api(&Method::PUT, &manifest_path()));
    }

    #[test]
    fn api_serves_delete_manifest() {
        assert!(oci_route_served_by_api(&Method::DELETE, &manifest_path()));
    }

    #[test]
    fn api_serves_get_tags_list() {
        assert!(oci_route_served_by_api(&Method::GET, &tags_list_path()));
    }

    #[test]
    fn api_serves_delete_blob() {
        assert!(oci_route_served_by_api(&Method::DELETE, &blob_path()));
    }

    #[test]
    fn api_rejects_get_blob() {
        assert!(!oci_route_served_by_api(&Method::GET, &blob_path()));
    }

    #[test]
    fn api_rejects_post_blob_uploads() {
        assert!(!oci_route_served_by_api(
            &Method::POST,
            &blob_uploads_path()
        ));
    }

    #[test]
    fn api_rejects_patch_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::PATCH,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_put_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::PUT,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_get_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::GET,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn api_rejects_delete_blob_upload_session() {
        assert!(!oci_route_served_by_api(
            &Method::DELETE,
            &blob_upload_session_path()
        ));
    }

    // ── oci_route_served_by_transfer ───────────────────────────────────

    #[test]
    fn transfer_serves_get_blob() {
        assert!(oci_route_served_by_transfer(&Method::GET, &blob_path()));
    }

    #[test]
    fn transfer_serves_head_blob() {
        assert!(oci_route_served_by_transfer(&Method::HEAD, &blob_path()));
    }

    #[test]
    fn transfer_serves_post_blob_uploads() {
        assert!(oci_route_served_by_transfer(
            &Method::POST,
            &blob_uploads_path()
        ));
    }

    #[test]
    fn transfer_serves_patch_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::PATCH,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_put_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::PUT,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_get_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::GET,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_serves_delete_blob_upload_session() {
        assert!(oci_route_served_by_transfer(
            &Method::DELETE,
            &blob_upload_session_path()
        ));
    }

    #[test]
    fn transfer_rejects_get_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::GET,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_head_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::HEAD,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_put_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::PUT,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_delete_manifest() {
        assert!(!oci_route_served_by_transfer(
            &Method::DELETE,
            &manifest_path()
        ));
    }

    #[test]
    fn transfer_rejects_get_tags_list() {
        assert!(!oci_route_served_by_transfer(
            &Method::GET,
            &tags_list_path()
        ));
    }

    #[test]
    fn transfer_rejects_delete_blob() {
        assert!(!oci_route_served_by_transfer(&Method::DELETE, &blob_path()));
    }

    // ── api ∩ transfer = ∅ for all method+path combinations ───────────

    #[test]
    fn api_and_transfer_are_complementary() {
        let methods = [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ];
        let paths = [
            blob_path(),
            manifest_path(),
            blob_uploads_path(),
            blob_upload_session_path(),
            tags_list_path(),
        ];
        for method in &methods {
            for path in &paths {
                let served_by_api = oci_route_served_by_api(method, path);
                let served_by_transfer = oci_route_served_by_transfer(method, path);
                assert!(
                    !(served_by_api && served_by_transfer),
                    "method={method} path={path:?} served by BOTH api and transfer"
                );
            }
        }
    }
}
