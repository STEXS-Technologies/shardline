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

pub fn parse_sha256_digest<E: ProtocolValidation>(value: &str) -> Result<String, E> {
    let Some(hash_hex) = value.strip_prefix("sha256:") else {
        return Err(E::invalid_digest());
    };
    validate_content_hash(hash_hex).map_err(|_| E::invalid_digest())?;
    Ok(hash_hex.to_owned())
}

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

pub fn object_key<E: ProtocolValidation>(value: &str) -> Result<ObjectKey, E> {
    ObjectKey::parse(value).map_err(|_| E::invalid_content_hash())
}

pub fn shared_sha256_object_key<E: ProtocolValidation>(
    digest_hex: &str,
) -> Result<ObjectKey, E> {
    validate_content_hash(digest_hex).map_err(|_| E::invalid_content_hash())?;
    object_key(&format!("protocols/shared/sha256/{digest_hex}"))
}

pub fn validate_oci_repository_name<E: ProtocolValidation>(value: &str) -> Result<(), E> {
    if value.is_empty() || value.starts_with('/') || value.ends_with('/') || value.contains('\\') {
        return Err(E::invalid_repository_name());
    }

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

pub fn validate_oci_repository_scope<E: ProtocolValidation>(
    value: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<(), E> {
    let Some(repository_scope) = repository_scope else {
        return Ok(());
    };

    let expected_root = format!(
        "{}/{}",
        repository_scope.owner().to_ascii_lowercase(),
        repository_scope.name().to_ascii_lowercase()
    );
    if value == expected_root
        || value
            .strip_prefix(&expected_root)
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return Ok(());
    }

    Err(E::not_found())
}

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
        let digest = parse_sha256_digest::<ValidateContentHashError>(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert!(digest.is_ok());
        assert_eq!(
            digest.unwrap_or_default(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert!(parse_sha256_digest::<ValidateContentHashError>(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )
        .is_err());
        assert!(parse_sha256_digest::<ValidateContentHashError>(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"
        )
        .is_err());
    }

    #[test]
    fn oci_repository_validator_rejects_traversal_and_uppercase() {
        assert!(validate_oci_repository_name::<ValidateContentHashError>("team/assets").is_ok());
        assert!(validate_oci_repository_name::<ValidateContentHashError>("../assets").is_err());
        assert!(
            validate_oci_repository_name::<ValidateContentHashError>("Team/assets").is_err()
        );
        assert!(
            validate_oci_repository_name::<ValidateContentHashError>("team//assets").is_err()
        );
    }

    #[test]
    fn oci_tag_validator_enforces_allowed_characters() {
        assert!(validate_oci_tag::<ValidateContentHashError>("v1").is_ok());
        assert!(validate_oci_tag::<ValidateContentHashError>("_debug.2026-04-23").is_ok());
        assert!(validate_oci_tag::<ValidateContentHashError>("bad/tag").is_err());
        assert!(validate_oci_tag::<ValidateContentHashError>("-bad").is_err());
    }

    #[test]
    fn upload_session_validator_accepts_hex_and_hyphen_only() {
        assert!(
            validate_upload_session_id::<ValidateContentHashError>("0000000000000001").is_ok()
        );
        assert!(validate_upload_session_id::<ValidateContentHashError>("dead-beef").is_ok());
        assert!(
            validate_upload_session_id::<ValidateContentHashError>("session_1").is_err()
        );
        assert!(validate_upload_session_id::<ValidateContentHashError>(
            &"a".repeat(MAX_UPLOAD_SESSION_ID_BYTES + 1)
        )
        .is_err());
    }

    #[test]
    fn oci_repository_scope_validator_accepts_bound_roots_and_nested_namespaces() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        assert!(
            validate_oci_repository_scope::<ValidateContentHashError>("team/assets", Some(&scope))
                .is_ok()
        );
        assert!(validate_oci_repository_scope::<ValidateContentHashError>(
            "team/assets/cache",
            Some(&scope)
        )
        .is_ok());
        assert!(
            validate_oci_repository_scope::<ValidateContentHashError>("team/other", Some(&scope))
                .is_err()
        );
        assert!(validate_oci_repository_scope::<ValidateContentHashError>(
            "other/assets",
            Some(&scope)
        )
        .is_err());
    }

    #[test]
    fn shared_sha256_key_uses_stable_shared_namespace() {
        let key = shared_sha256_object_key::<ValidateContentHashError>(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        assert_eq!(
            key.as_str(),
            "protocols/shared/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }
}
