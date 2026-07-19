use shardline_protocol::RepositoryScope;
use shardline_storage::{ObjectKey, ObjectPrefix};

use crate::{
    OciAdapterError,
    protocol_support::{
        object_key, parse_sha256_digest, scope_namespace, stable_hex_id,
        validate_oci_repository_name, validate_oci_repository_scope, validate_oci_tag,
    },
    types::OciReference,
};

/// # Errors
///
/// Returns an error when the repository name is not a valid OCI repository name.
pub fn validate_repository(repository: &str) -> Result<(), OciAdapterError> {
    validate_oci_repository_name(repository)
}

/// # Errors
///
/// Returns an error when the reference is not a valid OCI tag or digest.
pub fn parse_reference(reference: &str) -> Result<OciReference, OciAdapterError> {
    if reference.starts_with("sha256:") {
        return Ok(OciReference::Digest(parse_sha256_digest(reference)?));
    }
    validate_oci_tag(reference)?;
    Ok(OciReference::Tag(reference.to_owned()))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_blob_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/blobs/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_manifest_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifests/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_manifest_media_type_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifest-media-types/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, tag, or scope is invalid.
pub fn oci_tag_key(
    repository: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    validate_oci_tag(tag)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/tags/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        tag
    ))
}

/// Returns the object prefix for manifest digests in an OCI repository.
///
/// # Errors
///
/// Returns [`OciAdapterError`] when the repository name is invalid.
pub fn oci_manifest_prefix(
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/manifests/",
        scope_namespace(repository_scope),
        stable_hex_id(repository)
    ))
    .map_err(OciAdapterError::from)
}

/// # Errors
///
/// Returns [`OciAdapterError`] when the repository name is invalid or contains an
/// unsafe path.
pub fn oci_tag_prefix(
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tags/",
        scope_namespace(repository_scope),
        stable_hex_id(repository)
    ))
    .map_err(OciAdapterError::from)
}

/// # Errors
///
/// Returns an error when the repository, digest, tag, or scope is invalid.
pub fn oci_tag_target_key(
    repository: &str,
    digest_hex: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    validate_oci_tag(tag)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/tag-targets/{}/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex,
        tag
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_tag_target_prefix(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tag-targets/{}/",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
    .map_err(OciAdapterError::from)
}

#[must_use]
pub fn oci_blob_location(repository: &str, digest_hex: &str) -> String {
    format!("/v2/{repository}/blobs/sha256:{digest_hex}")
}

#[must_use]
pub fn oci_manifest_location(repository: &str, reference: &str) -> String {
    format!("/v2/{repository}/manifests/{reference}")
}

#[must_use]
pub fn upload_session_location(repository: &str, session_id: &str) -> String {
    format!("/v2/{repository}/blobs/uploads/{session_id}")
}
