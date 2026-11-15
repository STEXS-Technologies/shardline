use sha2::{Digest, Sha256};
use shardline_server_core::protocol_support as core_ps;
use shardline_storage::ObjectKey;

use crate::OciAdapterError;

impl core_ps::ProtocolValidation for OciAdapterError {
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

pub(crate) fn parse_sha256_digest(value: &str) -> Result<String, OciAdapterError> {
    core_ps::parse_sha256_digest(value)
}

pub(crate) use core_ps::scope_namespace;

pub(crate) fn stable_hex_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub(crate) fn object_key(value: &str) -> Result<ObjectKey, OciAdapterError> {
    core_ps::object_key(value)
}

pub(crate) fn shared_sha256_object_key(digest_hex: &str) -> Result<ObjectKey, OciAdapterError> {
    core_ps::shared_sha256_object_key(digest_hex)
}

pub(crate) fn validate_oci_repository_name(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_repository_name(value)
}

pub(crate) fn validate_oci_repository_scope(
    value: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_repository_scope(value, repository_scope)
}

pub(crate) fn validate_oci_tag(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_oci_tag(value)
}

pub(crate) fn validate_upload_session_id(value: &str) -> Result<(), OciAdapterError> {
    core_ps::validate_upload_session_id(value)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_sha256_digest, shared_sha256_object_key, validate_oci_repository_name,
        validate_oci_repository_scope, validate_oci_tag, validate_upload_session_id,
    };
    use crate::OciAdapterError;
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_server_core::protocol_support::ProtocolValidation;

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
            Err(OciAdapterError::InvalidDigest)
        ));
        assert!(matches!(
            parse_sha256_digest(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"
            ),
            Err(OciAdapterError::InvalidDigest)
        ));
    }

    #[test]
    fn oci_repository_validator_rejects_unsafe_names() {
        assert!(validate_oci_repository_name("team/assets").is_ok());
        for repository in [
            "../assets",
            "Team/assets",
            "team//assets",
            "team/asset\0s",
            "\0team/assets",
        ] {
            assert!(
                matches!(
                    validate_oci_repository_name(repository),
                    Err(OciAdapterError::InvalidRepositoryName)
                ),
                "{repository:?}"
            );
        }
    }

    #[test]
    fn oci_tag_validator_enforces_allowed_characters() {
        for tag in [
            "v1",
            "_debug.2026-04-23",
            "v1.0.0",
            "latest",
            "build-2026-04-23",
        ] {
            assert!(validate_oci_tag(tag).is_ok(), "{tag:?}");
        }
        for tag in [
            "bad/tag",
            "-bad",
            "contains spaces",
            "tag/with/slashes",
            "-starts-with-hyphen",
            ".starts-with-dot",
            "tag-émoji",
            "❤️",
            "café",
            "cafe\u{0301}",
        ] {
            assert!(
                matches!(
                    validate_oci_tag(tag),
                    Err(OciAdapterError::InvalidManifestReference)
                ),
                "{tag:?}"
            );
        }
    }

    #[test]
    fn upload_session_validator_accepts_hex_and_hyphen_only() {
        assert!(validate_upload_session_id("0000000000000001").is_ok());
        assert!(validate_upload_session_id("dead-beef").is_ok());
        assert!(matches!(
            validate_upload_session_id("session_1"),
            Err(OciAdapterError::InvalidUploadSession)
        ));
        assert!(matches!(
            validate_upload_session_id(&"a".repeat(65)),
            Err(OciAdapterError::InvalidUploadSession)
        ));
    }

    #[allow(clippy::expect_used)]
    #[test]
    fn oci_repository_scope_validator_accepts_bound_roots_and_nested_namespaces() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None)
            .expect("valid scope params should succeed");

        assert!(validate_oci_repository_scope("team/assets", Some(&scope)).is_ok());
        assert!(validate_oci_repository_scope("team/assets/cache", Some(&scope)).is_ok());
        assert!(matches!(
            validate_oci_repository_scope("team/other", Some(&scope)),
            Err(OciAdapterError::NotFound)
        ));
        assert!(matches!(
            validate_oci_repository_scope("other/assets", Some(&scope)),
            Err(OciAdapterError::NotFound)
        ));
    }

    #[allow(clippy::expect_used)]
    #[test]
    fn shared_sha256_key_uses_stable_shared_namespace() {
        let key = shared_sha256_object_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("valid hex digest should produce a key");
        assert_eq!(
            key.as_str(),
            "protocols/shared/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn stable_hex_id_is_deterministic_lowercase_sha256() {
        assert_eq!(
            super::stable_hex_id("hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        for input in ["hello", "test-value", "input-a", "input-b"] {
            let id = super::stable_hex_id(input);
            assert_eq!(id.len(), 64);
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            assert_eq!(id, super::stable_hex_id(input));
        }
        assert_ne!(
            super::stable_hex_id("input-a"),
            super::stable_hex_id("input-b")
        );
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn object_key_preserves_valid_paths_and_rejects_unsafe_paths() {
        assert_eq!(
            super::object_key("valid/path").unwrap().as_str(),
            "valid/path"
        );
        for path in ["", "../unsafe"] {
            assert!(
                matches!(
                    super::object_key(path),
                    Err(OciAdapterError::InvalidContentHash)
                ),
                "{path:?}"
            );
        }
    }

    #[test]
    fn protocol_validation_maps_each_error_variant() {
        assert!(matches!(
            OciAdapterError::invalid_digest(),
            OciAdapterError::InvalidDigest
        ));
        assert!(matches!(
            OciAdapterError::invalid_content_hash(),
            OciAdapterError::InvalidContentHash
        ));
        assert!(matches!(
            OciAdapterError::invalid_repository_name(),
            OciAdapterError::InvalidRepositoryName
        ));
        assert!(matches!(
            OciAdapterError::not_found(),
            OciAdapterError::NotFound
        ));
        assert!(matches!(
            OciAdapterError::invalid_manifest_reference(),
            OciAdapterError::InvalidManifestReference
        ));
        assert!(matches!(
            OciAdapterError::invalid_upload_session(),
            OciAdapterError::InvalidUploadSession
        ));
    }

    #[test]
    fn shared_sha256_object_key_invalid_digest_errors() {
        assert!(shared_sha256_object_key("not-64-chars").is_err());
    }

    #[test]
    fn validate_oci_repository_scope_none_scope_ok() {
        assert!(validate_oci_repository_scope("any/repo", None).is_ok());
    }
}
