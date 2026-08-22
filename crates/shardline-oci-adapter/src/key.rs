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

/// Validates an OCI repository name against the registry naming rules.
///
/// # Examples
///
/// ```
/// use shardline_oci_adapter::validate_repository;
///
/// assert!(validate_repository("acme/models").is_ok());
/// assert!(validate_repository("Acme/Models").is_err());
/// ```
///
/// # Errors
///
/// Returns an error when the repository name is not a valid OCI repository name.
pub fn validate_repository(repository: &str) -> Result<(), OciAdapterError> {
    validate_oci_repository_name(repository)
}

/// Parses an OCI reference into a [`Digest`](OciReference::Digest) or
/// [`Tag`](OciReference::Tag).
///
/// A reference is a digest when it starts with the `sha256:` prefix; anything
/// else must be a valid tag.
///
/// # Examples
///
/// ```
/// use shardline_oci_adapter::{OciReference, parse_reference};
///
/// let digest = parse_reference(
///     "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
/// )?;
/// assert!(matches!(digest, OciReference::Digest(_)));
///
/// let tag = parse_reference("latest")?;
/// assert!(matches!(tag, OciReference::Tag(_)));
///
/// assert!(parse_reference("bad tag!").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
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

/// Builds the storage object key for a blob in an OCI repository.
///
/// # Examples
///
/// ```
/// use shardline_oci_adapter::oci_blob_key;
///
/// let key = oci_blob_key(
///     "acme/models",
///     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
///     None,
/// )?;
/// assert!(key.as_str().starts_with("protocols/oci/global/repos/"));
/// assert!(key.as_str().ends_with(
///     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
/// ));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
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

fn validate_scope_namespace(scope_namespace: &str) -> Result<(), OciAdapterError> {
    if scope_namespace == "global"
        || (scope_namespace.len() == 64
            && scope_namespace
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    {
        return Ok(());
    }
    Err(OciAdapterError::InvalidContentHash)
}

fn oci_object_key_from_namespace(
    repository: &str,
    digest_hex: &str,
    scope_namespace: &str,
    object_namespace: &str,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_scope_namespace(scope_namespace)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    object_key(&format!(
        "protocols/oci/{scope_namespace}/repos/{}/{object_namespace}/{digest_hex}",
        stable_hex_id(repository),
    ))
}

/// Rebuilds an OCI blob key from its durable tombstone namespace.
///
/// # Errors
///
/// Returns an error when the namespace, repository, or digest is invalid.
pub fn oci_blob_key_from_namespace(
    repository: &str,
    digest_hex: &str,
    scope_namespace: &str,
) -> Result<ObjectKey, OciAdapterError> {
    oci_object_key_from_namespace(repository, digest_hex, scope_namespace, "blobs")
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

/// Rebuilds an OCI manifest key from its durable tombstone namespace.
///
/// # Errors
///
/// Returns an error when the namespace, repository, or digest is invalid.
pub fn oci_manifest_key_from_namespace(
    repository: &str,
    digest_hex: &str,
    scope_namespace: &str,
) -> Result<ObjectKey, OciAdapterError> {
    oci_object_key_from_namespace(repository, digest_hex, scope_namespace, "manifests")
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

/// Rebuilds an OCI manifest media-type key from its durable tombstone namespace.
///
/// # Errors
///
/// Returns an error when the namespace, repository, or digest is invalid.
pub fn oci_manifest_media_type_key_from_namespace(
    repository: &str,
    digest_hex: &str,
    scope_namespace: &str,
) -> Result<ObjectKey, OciAdapterError> {
    oci_object_key_from_namespace(
        repository,
        digest_hex,
        scope_namespace,
        "manifest-media-types",
    )
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

/// Returns the registry URL path for a blob in an OCI repository.
///
/// # Examples
///
/// ```
/// use shardline_oci_adapter::oci_blob_location;
///
/// assert_eq!(
///     oci_blob_location("acme/models", "0123"),
///     "/v2/acme/models/blobs/sha256:0123"
/// );
/// ```
#[must_use]
pub fn oci_blob_location(repository: &str, digest_hex: &str) -> String {
    format!("/v2/{repository}/blobs/sha256:{digest_hex}")
}

/// Returns the registry URL path for a manifest in an OCI repository.
///
/// # Examples
///
/// ```
/// use shardline_oci_adapter::oci_manifest_location;
///
/// assert_eq!(
///     oci_manifest_location("acme/models", "v1.2.3"),
///     "/v2/acme/models/manifests/v1.2.3"
/// );
/// ```
#[must_use]
pub fn oci_manifest_location(repository: &str, reference: &str) -> String {
    format!("/v2/{repository}/manifests/{reference}")
}

#[must_use]
pub fn upload_session_location(repository: &str, session_id: &str) -> String {
    format!("/v2/{repository}/blobs/uploads/{session_id}")
}
