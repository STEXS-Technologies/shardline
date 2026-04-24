use sha2::{Digest, Sha256};
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectKey;

use crate::{ServerError, validation::validate_content_hash};

const MAX_UPLOAD_SESSION_ID_BYTES: usize = 64;

pub(crate) fn parse_sha256_digest(value: &str) -> Result<String, ServerError> {
    let Some(hash_hex) = value.strip_prefix("sha256:") else {
        return Err(ServerError::InvalidDigest);
    };
    validate_content_hash(hash_hex).map_err(|_error| ServerError::InvalidDigest)?;
    Ok(hash_hex.to_owned())
}

pub(crate) fn scope_namespace(repository_scope: Option<&RepositoryScope>) -> String {
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

pub(crate) fn stable_hex_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub(crate) fn object_key(value: &str) -> Result<ObjectKey, ServerError> {
    ObjectKey::parse(value).map_err(|_error| ServerError::InvalidContentHash)
}

pub(crate) fn shared_sha256_object_key(digest_hex: &str) -> Result<ObjectKey, ServerError> {
    validate_content_hash(digest_hex)?;
    object_key(&format!("protocols/shared/sha256/{digest_hex}"))
}

pub(crate) fn validate_oci_repository_name(value: &str) -> Result<(), ServerError> {
    if value.is_empty() || value.starts_with('/') || value.ends_with('/') || value.contains('\\') {
        return Err(ServerError::InvalidRepositoryName);
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
            return Err(ServerError::InvalidRepositoryName);
        }
    }

    Ok(())
}

pub(crate) fn validate_oci_repository_scope(
    value: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<(), ServerError> {
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

    Err(ServerError::NotFound)
}

pub(crate) fn validate_oci_tag(value: &str) -> Result<(), ServerError> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(ServerError::InvalidManifestReference);
    };
    if !(first.is_ascii_alphanumeric() || first == b'_') {
        return Err(ServerError::InvalidManifestReference);
    }
    if value.len() > 128
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ServerError::InvalidManifestReference);
    }

    Ok(())
}

pub(crate) fn validate_upload_session_id(value: &str) -> Result<(), ServerError> {
    if value.is_empty()
        || value.len() > MAX_UPLOAD_SESSION_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return Err(ServerError::InvalidUploadSession);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_UPLOAD_SESSION_ID_BYTES, parse_sha256_digest, shared_sha256_object_key,
        validate_oci_repository_name, validate_oci_repository_scope, validate_oci_tag,
        validate_upload_session_id,
    };
    use crate::ServerError;
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    #[test]
    fn sha256_digest_parser_requires_prefixed_lowercase_hex() {
        let digest = parse_sha256_digest(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert!(digest.is_ok());
        assert_eq!(
            digest.unwrap_or_default(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert!(matches!(
            parse_sha256_digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            Err(ServerError::InvalidDigest)
        ));
        assert!(matches!(
            parse_sha256_digest(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"
            ),
            Err(ServerError::InvalidDigest)
        ));
    }

    #[test]
    fn oci_repository_validator_rejects_traversal_and_uppercase() {
        assert!(validate_oci_repository_name("team/assets").is_ok());
        assert!(matches!(
            validate_oci_repository_name("../assets"),
            Err(ServerError::InvalidRepositoryName)
        ));
        assert!(matches!(
            validate_oci_repository_name("Team/assets"),
            Err(ServerError::InvalidRepositoryName)
        ));
        assert!(matches!(
            validate_oci_repository_name("team//assets"),
            Err(ServerError::InvalidRepositoryName)
        ));
    }

    #[test]
    fn oci_tag_validator_enforces_allowed_characters() {
        assert!(validate_oci_tag("v1").is_ok());
        assert!(validate_oci_tag("_debug.2026-04-23").is_ok());
        assert!(matches!(
            validate_oci_tag("bad/tag"),
            Err(ServerError::InvalidManifestReference)
        ));
        assert!(matches!(
            validate_oci_tag("-bad"),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn upload_session_validator_accepts_hex_and_hyphen_only() {
        assert!(validate_upload_session_id("0000000000000001").is_ok());
        assert!(validate_upload_session_id("dead-beef").is_ok());
        assert!(matches!(
            validate_upload_session_id("session_1"),
            Err(ServerError::InvalidUploadSession)
        ));
        assert!(matches!(
            validate_upload_session_id(&"a".repeat(MAX_UPLOAD_SESSION_ID_BYTES + 1)),
            Err(ServerError::InvalidUploadSession)
        ));
    }

    #[test]
    fn oci_repository_scope_validator_accepts_bound_roots_and_nested_namespaces() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        assert!(validate_oci_repository_scope("team/assets", Some(&scope)).is_ok());
        assert!(validate_oci_repository_scope("team/assets/cache", Some(&scope)).is_ok());
        assert!(matches!(
            validate_oci_repository_scope("team/other", Some(&scope)),
            Err(ServerError::NotFound)
        ));
        assert!(matches!(
            validate_oci_repository_scope("other/assets", Some(&scope)),
            Err(ServerError::NotFound)
        ));
    }

    #[test]
    fn shared_sha256_key_uses_stable_shared_namespace() {
        let key = shared_sha256_object_key(
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
