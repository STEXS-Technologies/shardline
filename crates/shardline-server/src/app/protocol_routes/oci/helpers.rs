use std::sync::Arc;

use axum::{
    extract::{FromRequestParts, Path},
    http::{Method, request::Parts},
};
use shardline_index::{OciObjectKey as OciIndexObjectKey, OciObjectKind};
use shardline_protocol::TokenScope;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectKey, ObjectPrefix};

use crate::{ServerError, error::OciError, protocol_support::validate_oci_repository_scope};

use super::super::AppState;
use super::path::{OciPath, parse_oci_path};
use super::token::oci_authorize;

/// Parsed OCI route and repository authorization, verified together by axum.
///
/// Unsupported method/path pairs return 404 before authentication. Served routes
/// retain OCI's bearer challenge and scope errors, then bind token claims to the
/// requested repository before exposing a storage capability.
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
}

impl FromRequestParts<Arc<AppState>> for OciRepository {
    type Rejection = OciError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let path: String = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|_error| OciError::from(ServerError::NotFound))?
            .0;
        let parsed = parse_oci_path(&path)?;

        // Reject unsupported routes before touching authentication.
        let required_scope = oci_required_scope(&parts.method, &parsed)
            .ok_or_else(|| OciError::from(ServerError::NotFound))?;

        let auth = oci_authorize(
            state,
            &parts.headers,
            Some(parsed.repository()),
            required_scope,
        )?;
        validate_oci_repository_scope(
            parsed.repository(),
            auth.as_ref().map(|context| context.claims().repository()),
        )?;
        let capability = match auth {
            Some(context) => AuthorizedRepository::from_verified_context(context, required_scope)
                .map_err(ServerError::from)?,
            None => AuthorizedRepository::anonymous_full_access(),
        };
        Ok(Self {
            path: parsed,
            capability,
        })
    }
}

/// Maps an OCI `(method, path)` pair to the token scope required to serve it.
/// Returns `None` for unsupported routes.
const fn oci_required_scope(method: &Method, path: &OciPath) -> Option<TokenScope> {
    if !oci_route_served_by_api(method, path) && !oci_route_served_by_transfer(method, path) {
        return None;
    }
    // Upload-session status is part of the write workflow, including GET.
    if matches!(method, &Method::GET | &Method::HEAD)
        && !matches!(path, OciPath::BlobUploadSession { .. })
    {
        Some(TokenScope::Read)
    } else {
        Some(TokenScope::Write)
    }
}

// Storage keys require a verified capability. An anonymous capability uses
// the global namespace in deployments without an authentication provider.

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
            OciPath::Manifest { .. } | OciPath::TagsList { .. } | OciPath::Referrers { .. }
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

    #[test]
    fn api_route_contract() {
        for (method, path, expected) in [
            (Method::GET, manifest_path(), true),
            (Method::HEAD, manifest_path(), true),
            (Method::PUT, manifest_path(), true),
            (Method::DELETE, manifest_path(), true),
            (Method::GET, tags_list_path(), true),
            (Method::DELETE, blob_path(), true),
            (Method::GET, blob_path(), false),
            (Method::POST, blob_uploads_path(), false),
            (Method::PATCH, blob_upload_session_path(), false),
            (Method::PUT, blob_upload_session_path(), false),
            (Method::GET, blob_upload_session_path(), false),
            (Method::DELETE, blob_upload_session_path(), false),
        ] {
            assert_eq!(
                oci_route_served_by_api(&method, &path),
                expected,
                "method={method} path={path:?}"
            );
        }
    }

    #[test]
    fn transfer_route_contract() {
        for (method, path, expected) in [
            (Method::GET, blob_path(), true),
            (Method::HEAD, blob_path(), true),
            (Method::POST, blob_uploads_path(), true),
            (Method::PATCH, blob_upload_session_path(), true),
            (Method::PUT, blob_upload_session_path(), true),
            (Method::GET, blob_upload_session_path(), true),
            (Method::DELETE, blob_upload_session_path(), true),
            (Method::GET, manifest_path(), false),
            (Method::HEAD, manifest_path(), false),
            (Method::PUT, manifest_path(), false),
            (Method::DELETE, manifest_path(), false),
            (Method::GET, tags_list_path(), false),
            (Method::DELETE, blob_path(), false),
        ] {
            assert_eq!(
                oci_route_served_by_transfer(&method, &path),
                expected,
                "method={method} path={path:?}"
            );
        }
    }

    // ── api ∩ transfer = ∅ for all method+path combinations ───────────

    #[test]
    fn route_admission_and_required_scopes() {
        use TokenScope::{Read, Write};
        let methods = [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
            Method::CONNECT,
            Method::TRACE,
        ];
        let cases = [
            (
                blob_path(),
                [Some(Read), Some(Read), None, None, None, Some(Write)],
            ),
            (
                manifest_path(),
                [Some(Read), Some(Read), None, Some(Write), None, Some(Write)],
            ),
            (
                blob_uploads_path(),
                [None, None, Some(Write), None, None, None],
            ),
            (
                blob_upload_session_path(),
                [
                    Some(Write),
                    None,
                    None,
                    Some(Write),
                    Some(Write),
                    Some(Write),
                ],
            ),
            (tags_list_path(), [Some(Read), None, None, None, None, None]),
            (
                OciPath::Referrers {
                    repository: REPO.to_owned(),
                    digest_hex: DIGEST.to_owned(),
                },
                [Some(Read), None, None, None, None, None],
            ),
        ];
        for (path, scopes) in cases {
            for (index, method) in methods.iter().enumerate() {
                let expected_scope = scopes.get(index).copied().flatten();
                assert_eq!(
                    oci_required_scope(method, &path),
                    expected_scope,
                    "method={method} path={path:?}"
                );
                let served_by_api = oci_route_served_by_api(method, &path);
                let served_by_transfer = oci_route_served_by_transfer(method, &path);
                assert_eq!(
                    served_by_api || served_by_transfer,
                    expected_scope.is_some()
                );
                assert!(
                    !(served_by_api && served_by_transfer),
                    "method={method} path={path:?} served by BOTH api and transfer"
                );
            }
        }
    }
}
