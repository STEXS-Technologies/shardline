use shardline_server_core::protocol_support as core_ps;

use crate::ServerError;

impl core_ps::ProtocolValidation for ServerError {
    fn invalid_digest() -> Self {
        Self::InvalidDigest
    }
    fn invalid_content_hash() -> Self {
        Self::InvalidContentHash
    }
    fn invalid_repository_name() -> Self {
        Self::InvalidRepositoryName
    }
    fn not_found() -> Self {
        Self::NotFound
    }
    fn invalid_manifest_reference() -> Self {
        Self::InvalidManifestReference
    }
    fn invalid_upload_session() -> Self {
        Self::InvalidUploadSession
    }
}

pub(crate) fn parse_sha256_digest(value: &str) -> Result<String, ServerError> {
    core_ps::parse_sha256_digest(value)
}

pub(crate) fn scope_namespace(
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> String {
    core_ps::scope_namespace(repository_scope)
}

/// Builds a shared-namespace object key for a SHA-256 digest.
///
/// # Errors
///
/// Returns [`ServerError::InvalidContentHash`] when the digest hex is malformed.
pub fn shared_sha256_object_key(
    digest_hex: &str,
) -> Result<shardline_storage::ObjectKey, ServerError> {
    core_ps::shared_sha256_object_key(digest_hex)
}

pub(crate) fn validate_oci_repository_name(value: &str) -> Result<(), ServerError> {
    core_ps::validate_oci_repository_name(value)
}

pub(crate) fn validate_oci_repository_scope(
    value: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<(), ServerError> {
    core_ps::validate_oci_repository_scope(value, repository_scope)
}

pub(crate) fn validate_oci_tag(value: &str) -> Result<(), ServerError> {
    core_ps::validate_oci_tag(value)
}

pub(crate) fn validate_upload_session_id(value: &str) -> Result<(), ServerError> {
    core_ps::validate_upload_session_id(value)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_sha256_digest, shared_sha256_object_key, validate_oci_repository_name,
        validate_oci_repository_scope, validate_oci_tag, validate_upload_session_id,
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
            validate_upload_session_id(&"a".repeat(65)),
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

    #[test]
    fn shared_sha256_key_rejects_invalid_digest() {
        assert!(matches!(
            shared_sha256_object_key("not-a-valid-hex-digest"),
            Err(ServerError::InvalidContentHash)
        ));
    }

    #[test]
    fn parse_sha256_digest_rejects_empty() {
        assert!(matches!(
            parse_sha256_digest(""),
            Err(ServerError::InvalidDigest)
        ));
    }

    #[test]
    fn parse_sha256_digest_rejects_non_sha256_prefix() {
        assert!(matches!(
            parse_sha256_digest("md5:abc123"),
            Err(ServerError::InvalidDigest)
        ));
    }

    #[test]
    fn validate_oci_repository_scope_without_scope_accepts_any() {
        assert!(validate_oci_repository_scope("team/assets", None).is_ok());
    }

    #[test]
    fn validate_oci_repository_scope_rejects_out_of_bounds() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        // Create a scope with no revision
        let Ok(scope) = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None)
        else {
            return;
        };
        // "other/assets" does not start with "team/assets" → NotFound
        assert!(matches!(
            validate_oci_repository_scope("other/assets", Some(&scope)),
            Err(ServerError::NotFound)
        ));
    }

    #[test]
    fn scope_namespace_returns_global_for_none() {
        let result = super::scope_namespace(None);
        assert_eq!(result, "global");
    }

    #[test]
    fn scope_namespace_returns_hex_hash_for_some() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let Ok(scope) = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None)
        else {
            return;
        };
        let result = super::scope_namespace(Some(&scope));
        // Returns a SHA-256 hex string of the scoped provider+owner+name
        assert_eq!(
            result.len(),
            64,
            "scope namespace should be a 64-char hex hash"
        );
        assert!(
            result.chars().all(|c| c.is_ascii_hexdigit()),
            "scope namespace should be hex: {result}"
        );
    }
}
