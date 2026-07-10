use crate::error::HubApiError;
use crate::routes::HubState;

/// Maximum inline file size (1 MiB).
const MAX_INLINE_SIZE: u64 = 1_048_576;

/// Resolves a file download request.
///
/// Returns `DownloadResult::Inline` for files ≤1 MiB, or `DownloadResult::Redirect`
/// for larger files that should be fetched from the LFS endpoint.
///
/// # Errors
///
/// Returns [`HubApiError`] if the revision or file is not found.
pub fn resolve_file_from_store(
    state: &HubState,
    commit_sha: &str,
    file_path: &str,
) -> Result<DownloadResult, HubApiError> {
    let files = state
        .store
        .get_files(commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let file = files
        .iter()
        .find(|f| f.path == file_path)
        .ok_or(HubApiError::NotFound)?;

    if file.size <= MAX_INLINE_SIZE {
        Ok(DownloadResult::Inline {
            size: file.size,
            sha: file.sha.clone(),
            content: file.inline_content.clone(),
        })
    } else if file.is_lfs {
        Ok(DownloadResult::LfsRedirect {
            oid: file.sha.clone(),
            size: file.size,
        })
    } else {
        Ok(DownloadResult::Inline {
            size: file.size,
            sha: file.sha.clone(),
            content: file.inline_content.clone(),
        })
    }
}

/// Result of resolving a file download.
#[derive(Debug)]
pub enum DownloadResult {
    /// File should be returned inline.
    Inline {
        /// File size.
        size: u64,
        /// File SHA.
        sha: String,
        /// File content (if stored).
        content: Option<Vec<u8>>,
    },
    /// File should be redirected to the LFS endpoint.
    LfsRedirect {
        /// LFS OID.
        oid: String,
        /// File size.
        size: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- DownloadResult enum ---

    #[test]
    fn download_result_inline_has_correct_fields() {
        let content = vec![0u8; 100];
        let result = DownloadResult::Inline {
            size: 100,
            sha: "abc123".to_owned(),
            content: Some(content.clone()),
        };

        match result {
            DownloadResult::Inline { size, sha, content: c } => {
                assert_eq!(size, 100);
                assert_eq!(sha, "abc123");
                assert_eq!(c.as_deref(), Some(content.as_slice()));
            }
            _ => panic!("expected Inline variant"),
        }
    }

    #[test]
    fn download_result_inline_without_content() {
        let result = DownloadResult::Inline {
            size: 500,
            sha: "def456".to_owned(),
            content: None,
        };

        match result {
            DownloadResult::Inline { size, sha, content } => {
                assert_eq!(size, 500);
                assert_eq!(sha, "def456");
                assert!(content.is_none());
            }
            _ => panic!("expected Inline variant"),
        }
    }

    #[test]
    fn download_result_lfs_redirect_has_correct_fields() {
        let result = DownloadResult::LfsRedirect {
            oid: "deadbeef".to_owned(),
            size: 2_097_152,
        };

        match result {
            DownloadResult::LfsRedirect { oid, size } => {
                assert_eq!(oid, "deadbeef");
                assert_eq!(size, 2_097_152);
            }
            _ => panic!("expected LfsRedirect variant"),
        }
    }

    #[test]
    fn inline_variant_is_distinct_from_lfs_redirect() {
        let inline = DownloadResult::Inline {
            size: 1024,
            sha: "sha".to_owned(),
            content: None,
        };
        let redirect = DownloadResult::LfsRedirect {
            oid: "oid".to_owned(),
            size: 1024,
        };

        // They are different variants, so Debug output differs.
        assert_ne!(format!("{inline:?}"), format!("{redirect:?}"));
    }

    // --- Inline vs redirect logic tests ---
    //
    // These tests document the expected decision boundary from `resolve_file_from_store`
    // without needing a full `HubState`. The logic is:
    //   file.size <= MAX_INLINE_SIZE         → Inline
    //   file.size > MAX_INLINE_SIZE && is_lfs → LfsRedirect
    //   file.size > MAX_INLINE_SIZE && !is_lfs → Inline (content returned even if large)

    #[test]
    fn small_file_should_be_inline() {
        // A file ≤ 1 MiB should produce Inline.
        let size = 1_048_576; // exactly 1 MiB
        let is_lfs = false;
        let result = resolve_download_variant(size, is_lfs);
        assert!(matches!(result, DownloadResult::Inline { .. }));
    }

    #[test]
    fn large_lfs_file_should_redirect() {
        let size = 1_048_577; // just above 1 MiB
        let is_lfs = true;
        let result = resolve_download_variant(size, is_lfs);
        assert!(matches!(result, DownloadResult::LfsRedirect { .. }));
    }

    #[test]
    fn large_non_lfs_file_should_be_inline() {
        let size = 5_000_000;
        let is_lfs = false;
        let result = resolve_download_variant(size, is_lfs);
        assert!(matches!(result, DownloadResult::Inline { .. }));
    }

    #[test]
    fn zero_size_file_should_be_inline() {
        let result = resolve_download_variant(0, false);
        assert!(matches!(result, DownloadResult::Inline { .. }));
    }

    /// Mirrors the decision logic in `resolve_file_from_store` for unit-test purposes.
    fn resolve_download_variant(size: u64, is_lfs: bool) -> DownloadResult {
        const MAX_INLINE: u64 = 1_048_576;

        if size <= MAX_INLINE {
            DownloadResult::Inline {
                size,
                sha: String::new(),
                content: None,
            }
        } else if is_lfs {
            DownloadResult::LfsRedirect {
                oid: String::new(),
                size,
            }
        } else {
            DownloadResult::Inline {
                size,
                sha: String::new(),
                content: None,
            }
        }
    }
}
