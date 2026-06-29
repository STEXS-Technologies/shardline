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
    },
    /// File should be redirected to the LFS endpoint.
    LfsRedirect {
        /// LFS OID.
        oid: String,
        /// File size.
        size: u64,
    },
}
