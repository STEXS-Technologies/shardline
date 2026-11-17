use sha2::{Digest, Sha256};
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectKey;

use crate::validate_content_hash;

const MAX_UPLOAD_SESSION_ID_BYTES: usize = 64;

pub trait ProtocolValidation {
    fn invalid_digest() -> Self;
    fn invalid_content_hash() -> Self;
    fn invalid_repository_name() -> Self;
    fn not_found() -> Self;
    fn invalid_manifest_reference() -> Self;
    fn invalid_upload_session() -> Self;
}

/// Parses a `sha256:<hex>` digest string, validating the hex portion.
///
/// # Errors
///
/// Returns `E::invalid_digest()` when the value is not prefixed with `sha256:`
/// or the hex portion fails content-hash validation.
pub fn parse_sha256_digest<E: ProtocolValidation>(value: &str) -> Result<String, E> {
    let Some(hash_hex) = value.strip_prefix("sha256:") else {
        return Err(E::invalid_digest());
    };
    validate_content_hash(hash_hex).map_err(|_error| E::invalid_digest())?;
    Ok(hash_hex.to_owned())
}

#[must_use]
pub fn scope_namespace(repository_scope: Option<&RepositoryScope>) -> String {
    repository_scope.map_or_else(
        || "global".to_owned(),
        |scope| {
            let mut hasher = Sha256::new();
            hasher.update(scope.provider().as_str().as_bytes());
            hasher.update([0]);
            hasher.update(scope.owner().as_bytes());
            hasher.update([0]);
            hasher.update(scope.name().as_bytes());
            hasher.update([0]);
            if let Some(revision) = scope.revision() {
                hasher.update(revision.as_bytes());
            }
            hex::encode(hasher.finalize())
        },
    )
}

/// Returns the deterministic file-record identifier used for a protocol object.
///
/// Keeping this derivation in the shared core lets online request handling and
/// offline garbage collection address the same deduplicated record without
/// copying the encoding contract.
#[must_use]
pub fn protocol_object_file_id(object_key: &ObjectKey) -> String {
    format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(object_key.as_str().as_bytes()))
    )
}

/// Parses an object key from a string path.
///
/// # Errors
///
/// Returns `E::invalid_content_hash()` when the string is not a valid
/// [`ObjectKey`] (empty, unsafe path, or too long).
pub fn object_key<E: ProtocolValidation>(value: &str) -> Result<ObjectKey, E> {
    ObjectKey::parse(value).map_err(|_error| E::invalid_content_hash())
}

/// Builds a shared-namespace object key for a SHA-256 digest.
///
/// # Errors
///
/// Returns `E::invalid_content_hash()` when the digest hex is malformed.
pub fn shared_sha256_object_key<E: ProtocolValidation>(digest_hex: &str) -> Result<ObjectKey, E> {
    validate_content_hash(digest_hex).map_err(|_error| E::invalid_content_hash())?;
    object_key(&format!("protocols/shared/sha256/{digest_hex}"))
}

/// Validates an OCI repository name according to the OCI distribution spec.
///
/// # Errors
///
/// Returns `E::invalid_repository_name()` when the name is empty, contains
/// path separators or traversal sequences, or has non-compliant segments.
pub fn validate_oci_repository_name<E: ProtocolValidation>(value: &str) -> Result<(), E> {
    for segment in value.split('/') {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || !segment.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
        {
            return Err(E::invalid_repository_name());
        }
    }

    Ok(())
}

/// Validates that an OCI repository name falls within a bound repository scope.
///
/// When no scope is configured the check is a no-op.
///
/// # Errors
///
/// Returns `E::not_found()` when the name does not match or extend the
/// repository scope's `owner/name` prefix.
pub fn validate_oci_repository_scope<E: ProtocolValidation>(
    value: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<(), E> {
    let Some(repository_scope) = repository_scope else {
        return Ok(());
    };

    let mut expected_root = format!("{}/{}", repository_scope.owner(), repository_scope.name());
    expected_root.make_ascii_lowercase();
    if value == expected_root
        || value
            .strip_prefix(&expected_root)
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return Ok(());
    }

    Err(E::not_found())
}

/// Validates an OCI tag reference according to the OCI distribution spec.
///
/// # Errors
///
/// Returns `E::invalid_manifest_reference()` when the tag is empty, exceeds
/// 128 bytes, or contains invalid characters.
pub fn validate_oci_tag<E: ProtocolValidation>(value: &str) -> Result<(), E> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(E::invalid_manifest_reference());
    };
    if !(first.is_ascii_alphanumeric() || first == b'_') {
        return Err(E::invalid_manifest_reference());
    }
    if value.len() > 128
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(E::invalid_manifest_reference());
    }

    Ok(())
}

/// Validates an upload session identifier.
///
/// Must be non-empty, at most 64 bytes, and contain only ASCII hex digits
/// or hyphens.
///
/// # Errors
///
/// Returns `E::invalid_upload_session()` when the identifier is malformed.
pub fn validate_upload_session_id<E: ProtocolValidation>(value: &str) -> Result<(), E> {
    if value.is_empty()
        || value.len() > MAX_UPLOAD_SESSION_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return Err(E::invalid_upload_session());
    }

    Ok(())
}

impl ProtocolValidation for crate::ValidateContentHashError {
    fn invalid_digest() -> Self {
        Self
    }
    fn invalid_content_hash() -> Self {
        Self
    }
    fn invalid_repository_name() -> Self {
        Self
    }
    fn not_found() -> Self {
        Self
    }
    fn invalid_manifest_reference() -> Self {
        Self
    }
    fn invalid_upload_session() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValidateContentHashError;

    #[test]
    fn sha256_digest_parser_requires_prefixed_lowercase_hex() {
        assert_eq!(
            parse_sha256_digest::<ValidateContentHashError>(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            )
            .unwrap(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        for value in [
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg",
            "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ] {
            assert!(
                parse_sha256_digest::<ValidateContentHashError>(value).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn oci_repository_validator_checks_segments() {
        for (value, valid) in [
            ("team/assets", true),
            ("repo", true),
            ("team-1/assets_v2.0", true),
            ("../assets", false),
            ("Team/assets", false),
            ("team//assets", false),
            ("", false),
            ("/repo", false),
            ("repo/", false),
            ("team\\repo", false),
            ("team//repo", false),
            ("team/./repo", false),
            ("team/../repo", false),
            ("Team/Repo", false),
            ("team/repo!", false),
        ] {
            assert_eq!(
                validate_oci_repository_name::<ValidateContentHashError>(value).is_ok(),
                valid,
                "{value:?}"
            );
        }
    }

    #[test]
    fn oci_tag_validator_checks_characters_and_length() {
        let maximum = "a".repeat(128);
        let oversized = "a".repeat(129);
        for (value, valid) in [
            ("v1", true),
            ("_debug.2026-04-23", true),
            ("valid-tag", true),
            ("v1.0.0", true),
            (maximum.as_str(), true),
            ("bad/tag", false),
            ("-bad", false),
            ("", false),
            (".tag", false),
            (oversized.as_str(), false),
            ("tag with spaces", false),
            ("tag\nnewline", false),
        ] {
            assert_eq!(
                validate_oci_tag::<ValidateContentHashError>(value).is_ok(),
                valid,
                "{value:?}"
            );
        }
    }

    #[test]
    fn upload_session_validator_checks_characters_and_length() {
        let maximum = "a".repeat(MAX_UPLOAD_SESSION_ID_BYTES);
        let oversized = "a".repeat(MAX_UPLOAD_SESSION_ID_BYTES + 1);
        for (value, valid) in [
            ("0000000000000001", true),
            ("dead-beef", true),
            ("a1b2c3d4e5f6", true),
            (maximum.as_str(), true),
            ("session_1", false),
            (oversized.as_str(), false),
            ("", false),
            ("session@123", false),
        ] {
            assert_eq!(
                validate_upload_session_id::<ValidateContentHashError>(value).is_ok(),
                valid,
                "{value:?}"
            );
        }
    }

    #[test]
    fn oci_repository_scope_validator_checks_boundaries_and_unscoped_access() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
        let mixed_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "Team", "Assets", None).unwrap();
        for (value, scope, valid) in [
            ("team/assets", Some(&mixed_scope), true),
            ("team/assets/cache", Some(&mixed_scope), true),
            ("Team/Assets", Some(&mixed_scope), false),
            ("team/assets-other", Some(&mixed_scope), false),
            ("team/assets", Some(&scope), true),
            ("team/assets/cache", Some(&scope), true),
            ("team/other", Some(&scope), false),
            ("other/assets", Some(&scope), false),
            ("other/repo", Some(&scope), false),
            ("team/assets-other", Some(&scope), false),
            ("any/repo", None, true),
        ] {
            assert_eq!(
                validate_oci_repository_scope::<ValidateContentHashError>(value, scope).is_ok(),
                valid,
                "{value}"
            );
        }
    }

    #[test]
    fn shared_sha256_key_uses_stable_shared_namespace() {
        let key = shared_sha256_object_key::<ValidateContentHashError>(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        assert_eq!(
            key.as_str(),
            "protocols/shared/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn scope_namespace_preserves_global_and_revision_encoding() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        assert_eq!(scope_namespace(None), "global");
        for (revision, expected) in [
            (
                Some("main"),
                "dd80a080687cba3deddd237177f7f99484811e475ba6bac34ec5e4295fd8cb32",
            ),
            (
                None,
                "d6db281729e70151c911326ac411108165abfc2bb78a3313c14d99cf7ec2cfd2",
            ),
        ] {
            let scope =
                RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", revision)
                    .unwrap();
            assert_eq!(scope_namespace(Some(&scope)), expected, "{revision:?}");
        }
    }

    #[test]
    fn object_key_valid_path() {
        let key = object_key::<ValidateContentHashError>("aa/abcdef");
        assert!(key.is_ok());
        assert_eq!(key.unwrap().as_str(), "aa/abcdef");
    }

    #[test]
    fn object_key_invalid_path() {
        let key = object_key::<ValidateContentHashError>("");
        assert!(key.is_err());
    }

    #[test]
    fn shared_sha256_key_rejects_bad_hash() {
        let key = shared_sha256_object_key::<ValidateContentHashError>("bad");
        assert!(key.is_err());
    }
}
