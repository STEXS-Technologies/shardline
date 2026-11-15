use crate::{ServerError, oci_adapter::validate_repository, protocol_support::parse_sha256_digest};

#[cfg_attr(test, derive(Debug))]
pub(crate) enum OciPath {
    Blob {
        repository: String,
        digest_hex: String,
    },
    BlobUploads {
        repository: String,
    },
    BlobUploadSession {
        repository: String,
        session_id: String,
    },
    Manifest {
        repository: String,
        reference: String,
    },
    TagsList {
        repository: String,
    },
    Referrers {
        repository: String,
        digest_hex: String,
    },
}

impl OciPath {
    /// Returns the requested repository name carried by this parsed OCI path.
    #[must_use]
    pub(crate) fn repository(&self) -> &str {
        match self {
            OciPath::Blob { repository, .. }
            | OciPath::BlobUploads { repository }
            | OciPath::BlobUploadSession { repository, .. }
            | OciPath::Manifest { repository, .. }
            | OciPath::TagsList { repository }
            | OciPath::Referrers { repository, .. } => repository,
        }
    }
}

pub(crate) fn parse_oci_path(path: &str) -> Result<OciPath, ServerError> {
    let path = path.trim_end_matches('/');
    if let Some(repository) = path.strip_suffix("/blobs/uploads") {
        validate_repository(repository)?;
        return Ok(OciPath::BlobUploads {
            repository: repository.to_owned(),
        });
    }
    if let Some(repository) = path.strip_suffix("/tags/list") {
        validate_repository(repository)?;
        return Ok(OciPath::TagsList {
            repository: repository.to_owned(),
        });
    }
    // Operation names are also legal repository components. Resolve the
    // operation from the final path segments rather than its first occurrence.
    let Some((prefix, value)) = path.rsplit_once('/') else {
        return Err(ServerError::NotFound);
    };
    let Some((repository, operation)) = prefix.rsplit_once('/') else {
        return Err(ServerError::NotFound);
    };
    match operation {
        "referrers" => {
            validate_repository(repository)?;
            Ok(OciPath::Referrers {
                repository: repository.to_owned(),
                digest_hex: parse_sha256_digest(value)?,
            })
        }
        "blobs" => {
            validate_repository(repository)?;
            Ok(OciPath::Blob {
                repository: repository.to_owned(),
                digest_hex: parse_sha256_digest(value)?,
            })
        }
        "manifests" => {
            validate_repository(repository)?;
            Ok(OciPath::Manifest {
                repository: repository.to_owned(),
                reference: value.to_owned(),
            })
        }
        "uploads" => {
            let Some(repository) = repository.strip_suffix("/blobs") else {
                return Err(ServerError::NotFound);
            };
            validate_repository(repository)?;
            Ok(OciPath::BlobUploadSession {
                repository: repository.to_owned(),
                session_id: value.to_owned(),
            })
        }
        _ => Err(ServerError::NotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_DIGEST: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // ── Blob ────────────────────────────────────────────────────────────

    #[test]
    fn blob_path_parses_valid_sha256_digest() {
        let path = format!("team/assets/blobs/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Blob {
                repository,
                digest_hex
            }) if repository == "team/assets"
                && digest_hex == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    // ── BlobUploads ─────────────────────────────────────────────────────

    #[test]
    fn blob_uploads_path_parses() {
        let result = parse_oci_path("team/assets/blobs/uploads");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team/assets"
        ));
    }

    #[test]
    fn blob_uploads_path_strips_trailing_slash() {
        let result = parse_oci_path("team/assets/blobs/uploads/");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team/assets"
        ));
    }

    // ── BlobUploadSession ───────────────────────────────────────────────

    #[test]
    fn blob_upload_session_path_parses() {
        let result = parse_oci_path("team/assets/blobs/uploads/0000000000000001");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploadSession {
                repository,
                session_id
            }) if repository == "team/assets" && session_id == "0000000000000001"
        ));
    }

    // ── Manifest ────────────────────────────────────────────────────────

    #[test]
    fn manifest_tag_reference_parses() {
        let result = parse_oci_path("team/assets/manifests/v1");
        assert!(matches!(
            result,
            Ok(OciPath::Manifest {
                repository,
                reference
            }) if repository == "team/assets" && reference == "v1"
        ));
    }

    #[test]
    fn manifest_digest_reference_parses() {
        let path = format!("team/assets/manifests/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Manifest {
                repository,
                reference
            }) if repository == "team/assets" && reference == VALID_DIGEST
        ));
    }

    // ── TagsList ────────────────────────────────────────────────────────

    #[test]
    fn tags_list_path_parses() {
        let result = parse_oci_path("team/assets/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/assets"
        ));
    }

    #[test]
    fn tags_list_path_strips_trailing_slash() {
        let result = parse_oci_path("team/assets/tags/list/");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/assets"
        ));
    }

    // ── Deeply nested repositories ──────────────────────────────────────

    #[test]
    fn deeply_nested_repository_parses() {
        let result = parse_oci_path("a/b/c/d/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "a/b/c/d"
        ));
    }

    // ── Path ordering (blobs/uploads must be checked before blobs) ──────

    #[test]
    fn blob_uploads_session_not_confused_with_blob() {
        let result = parse_oci_path("team/assets/blobs/uploads/abc123");
        assert!(matches!(result, Ok(OciPath::BlobUploadSession { .. })));
    }

    // ── Additional edge cases ────────────────────────────────────────────

    #[test]
    fn path_with_too_many_segments_still_parses_if_matches_pattern() {
        // Deep nesting in the repository portion is fine (matches blob pattern)
        let path = format!("a/b/c/d/e/f/blobs/{VALID_DIGEST}");
        let result = parse_oci_path(&path);
        assert!(matches!(
            result,
            Ok(OciPath::Blob { repository, .. }) if repository == "a/b/c/d/e/f"
        ));
    }

    #[test]
    fn operation_names_inside_repository_preserve_the_complete_repository() {
        for repository in [
            "team/blobs/assets",
            "team/blobs/uploads/assets",
            "team/manifests/assets",
            "team/tags/list/assets",
            "team/blobs/uploads/manifests/tags/list",
        ] {
            for suffix in [
                format!("blobs/{VALID_DIGEST}"),
                "blobs/uploads".to_owned(),
                "blobs/uploads/0000000000000001".to_owned(),
                "manifests/latest".to_owned(),
                format!("manifests/{VALID_DIGEST}"),
                "tags/list".to_owned(),
            ] {
                let path = format!("{repository}/{suffix}");
                let parsed = parse_oci_path(&path).expect("valid operation and repository");
                assert_eq!(parsed.repository(), repository, "{path}");
            }
        }
    }

    #[test]
    fn blob_session_with_extra_trailing_slash() {
        // Trailing slash after session ID
        let result = parse_oci_path("team/assets/blobs/uploads/abc123/");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploadSession { session_id, .. }) if session_id == "abc123"
        ));
    }

    #[test]
    fn tags_list_with_repository_having_hyphens_and_dots() {
        // Repository names commonly contain hyphens and dots.
        let result = parse_oci_path("my-team/my-repo.v2/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "my-team/my-repo.v2"
        ));
    }

    #[test]
    fn repository_with_underscores_accepted() {
        let result = parse_oci_path("team/my_repo/tags/list");
        assert!(matches!(
            result,
            Ok(OciPath::TagsList { repository }) if repository == "team/my_repo"
        ));
    }

    #[test]
    fn repository_with_numbers_accepted() {
        let result = parse_oci_path("team2/project3/blobs/uploads");
        assert!(matches!(
            result,
            Ok(OciPath::BlobUploads { repository }) if repository == "team2/project3"
        ));
    }

    // ── Null byte rejection ──────────────────────────────────────────────

    #[test]
    fn oci_manifest_path_with_null_byte_in_reference() {
        let path = "team/assets/manifests/v1\0tag";
        let result = parse_oci_path(path);
        // parse_oci_path validates the repository portion but passes the
        // reference through without validation.  The null byte is part of
        // the reference string.  Downstream manifest handlers should reject
        // the null-containing reference.
        assert!(matches!(
            result,
            Ok(OciPath::Manifest { reference, .. }) if reference == "v1\0tag"
        ));
    }

    #[test]
    fn paths_return_not_found() {
        for path in [
            "team/assets/unknown",
            "",
            "team/assets/blobs/",
            "team/assets/manifests/",
            "team/assets/manifests/v1/something",
        ] {
            assert!(
                matches!(parse_oci_path(path), Err(ServerError::NotFound)),
                "{path:?}"
            );
        }
    }

    #[test]
    fn paths_return_invalid_repository_name() {
        for path in [
            "Team/assets/tags/list",
            "team/my assets/tags/list",
            "team/./assets/tags/list",
            "team/asset\0s/tags/list",
        ] {
            assert!(
                matches!(
                    parse_oci_path(path),
                    Err(ServerError::InvalidRepositoryName)
                ),
                "{path:?}"
            );
        }
    }

    #[test]
    fn paths_return_invalid_digest() {
        for path in [
            "team/assets/blobs/sha256:not-a-hex",
            "team/assets/blobs/not-sha256-prefix",
            "team/assets/blobs/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc\0ef",
        ] {
            assert!(
                matches!(parse_oci_path(path), Err(ServerError::InvalidDigest)),
                "{path:?}"
            );
        }
    }
}
