use shardline_protocol::ByteRange;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectStore;

use crate::{error::HubApiError, routes::HubState};

/// Resolves a file download request using the global LFS namespace.
///
/// Backward-compatible wrapper that delegates to
/// [`resolve_file_from_store_scoped`] with no repository scope.
///
/// # Errors
///
/// Returns [`HubApiError`] if the revision or file is not found.
pub fn resolve_file_from_store(
    state: &HubState,
    commit_sha: &str,
    file_path: &str,
) -> Result<DownloadResult, HubApiError> {
    resolve_file_from_store_scoped(
        state,
        commit_sha,
        file_path,
        // Anonymous capability resolves to the global (unscoped) namespace,
        // exactly matching the historical `None` scope argument.
        &AuthorizedRepository::anonymous_full_access(),
    )
}

/// Resolves a file download request, namespacing LFS object reads by the
/// provided repository scope.
///
/// Returns `DownloadResult::Inline` for files ≤1 MiB, or `DownloadResult::Redirect`
/// for larger files that should be fetched from the LFS endpoint.
///
/// # Errors
///
/// Returns [`HubApiError`] if the revision or file is not found.
pub fn resolve_file_from_store_scoped(
    state: &HubState,
    commit_sha: &str,
    file_path: &str,
    auth: &AuthorizedRepository,
) -> Result<DownloadResult, HubApiError> {
    let files = state
        .store
        .get_files(commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let file = files
        .iter()
        .find(|f| f.path == file_path)
        .ok_or(HubApiError::NotFound)?;

    let content = get_object_store_content(state, &file.sha, auth);

    if file.size <= MAX_INLINE_SIZE {
        Ok(DownloadResult::Inline {
            size: file.size,
            sha: file.sha.clone(),
            content,
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
            content,
        })
    }
}

/// Maximum inline file size (1 MiB).
const MAX_INLINE_SIZE: u64 = 1_048_576;

/// Reads file content from ObjectStore using its SHA, namespaced by the given
/// repository scope so reads find the repository-scoped writes.
fn get_object_store_content(
    state: &HubState,
    sha: &str,
    auth: &AuthorizedRepository,
) -> Option<Vec<u8>> {
    let key = crate::routes::lfs_object_key(sha, auth).ok()?;
    let size = state.object_store.metadata(&key).ok()??.length();
    let range_end = size.checked_sub(1)?;
    let range = ByteRange::new(0, range_end).ok()?;
    state.object_store.read_range(&key, range).ok()
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
    #[allow(clippy::panic)]
    fn download_result_inline_has_correct_fields() {
        let content = vec![0u8; 100];
        let result = DownloadResult::Inline {
            size: 100,
            sha: "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6".to_owned(),
            content: Some(content.clone()),
        };

        match result {
            DownloadResult::Inline {
                size,
                sha,
                content: c,
            } => {
                assert_eq!(size, 100);
                assert_eq!(
                    sha,
                    "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6"
                );
                assert_eq!(c.as_deref(), Some(content.as_slice()));
            }
            _ => panic!("expected Inline variant"),
        }
    }

    #[test]
    #[allow(clippy::panic)]
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
    #[allow(clippy::panic)]
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

    // --- Additional edge cases ---

    #[test]
    fn exactly_at_inline_boundary_should_be_inline() {
        // size == MAX_INLINE_SIZE (1 MiB) → Inline
        let result = resolve_download_variant(1_048_576, true);
        assert!(matches!(result, DownloadResult::Inline { .. }));
    }

    #[test]
    fn one_byte_over_inline_boundary_lfs_should_redirect() {
        // size == MAX_INLINE_SIZE + 1 and is_lfs → LfsRedirect
        let result = resolve_download_variant(1_048_577, true);
        assert!(matches!(result, DownloadResult::LfsRedirect { .. }));
    }

    #[test]
    fn lfs_redirect_contains_oid_and_size() {
        let result = DownloadResult::LfsRedirect {
            oid: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            size: 2_000_000,
        };
        match &result {
            DownloadResult::LfsRedirect { oid, size } => {
                assert_eq!(oid.len(), 64);
                assert_eq!(*size, 2_000_000);
            }
            #[allow(clippy::panic)]
            _ => panic!("expected LfsRedirect"),
        }
    }

    #[test]
    fn download_result_debug_roundtrip() {
        let inline = DownloadResult::Inline {
            size: 10,
            sha: "sha".into(),
            content: Some(vec![1, 2, 3]),
        };
        let debug_str = format!("{inline:?}");
        assert!(debug_str.contains("Inline"));
        assert!(debug_str.contains("sha"));

        let redirect = DownloadResult::LfsRedirect {
            oid: "oid".into(),
            size: 20,
        };
        let debug_str = format!("{redirect:?}");
        assert!(debug_str.contains("LfsRedirect"));
        assert!(debug_str.contains("oid"));
    }

    // --- resolve_file_from_store integration tests (requires store) ---

    /// Helper: creates a minimal HubState with a LocalIndexStore, creates a
    /// repo + revision with the given file entries.
    /// Optionally pre-loads content into ObjectStore for the given `(sha, data)` pairs.
    fn setup_resolve_state(
        files: &[shardline_index::hub::HubFileEntry],
        content: &[(&str, &[u8])],
    ) -> (tempfile::TempDir, HubState) {
        use shardline_index::hub::{BoxedHubStore, HubRepoType, ensure_hub_tables};
        use shardline_storage::{ObjectBody, ObjectIntegrity};
        let ts = tempfile::tempdir().expect("tempdir");
        let root = ts.path();
        ensure_hub_tables(root).expect("ensure hub tables");
        let store = shardline_index::LocalIndexStore::open(root.to_path_buf());
        let store = BoxedHubStore::from_store(store);

        store
            .create_repo(HubRepoType::Model, "org/repo", false)
            .unwrap();
        let parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        store
            .create_revision("org/repo", Some(parent), "sha_resolve", "main", "test")
            .unwrap();
        if !files.is_empty() {
            store.store_files("sha_resolve", files).unwrap();
        }
        let object_store = shardline_server_core::ServerObjectStore::local(ts.path().join("lfs"))
            .expect("local object store");

        // Pre-load content into ObjectStore using the repository-namespaced key
        // (global namespace, matching the scope used by these tests).
        for (sha, data) in content {
            let key =
                crate::routes::lfs_object_key(sha, &AuthorizedRepository::anonymous_full_access())
                    .expect("valid key");
            let body = ObjectBody::from_slice(data);
            let integrity = ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
                data.len() as u64,
            );
            object_store
                .put_if_absent(&key, body, &integrity)
                .expect("put content");
        }

        let state = HubState {
            store,
            object_store,
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
        };
        (ts, state)
    }

    #[test]
    fn resolve_file_from_store_small_inline_file() {
        let content = b"small file content";
        let files = vec![shardline_index::hub::HubFileEntry {
            path: "readme.md".into(),
            size: content.len() as u64,
            sha: "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6".into(),
            is_lfs: false,
        }];
        let content_data = content.to_vec();
        let (_ts, state) = setup_resolve_state(
            &files,
            &[(
                "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6",
                content,
            )],
        );
        let result = resolve_file_from_store(&state, "sha_resolve", "readme.md").unwrap();
        match &result {
            DownloadResult::Inline {
                size,
                sha,
                content: c,
            } => {
                assert_eq!(*size, content_data.len() as u64);
                assert_eq!(
                    sha.as_str(),
                    "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6"
                );
                assert_eq!(c, &Some(content_data));
            }
            _ => assert!(
                matches!(result, DownloadResult::Inline { .. }),
                "expected Inline, got {result:?}"
            ),
        }
    }

    #[test]
    fn resolve_file_from_store_large_lfs_redirect() {
        let size = 2_000_000u64;
        let files = vec![shardline_index::hub::HubFileEntry {
            path: "model.bin".into(),
            size,
            sha: "d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7".into(),
            is_lfs: true,
        }];
        let (_ts, state) = setup_resolve_state(&files, &[]);
        let result = resolve_file_from_store(&state, "sha_resolve", "model.bin").unwrap();
        match &result {
            DownloadResult::LfsRedirect { oid, size: s } => {
                assert_eq!(
                    oid.as_str(),
                    "d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7"
                );
                assert_eq!(*s, size);
            }
            _ => assert!(
                matches!(result, DownloadResult::LfsRedirect { .. }),
                "expected LfsRedirect, got {result:?}"
            ),
        }
    }

    #[test]
    fn resolve_file_from_store_file_not_found() {
        let (_ts, state) = setup_resolve_state(&[], &[]);
        let result = resolve_file_from_store(&state, "sha_resolve", "nonexistent.txt");
        assert!(matches!(result, Err(HubApiError::NotFound)));
    }

    #[test]
    fn resolve_file_from_store_large_non_lfs_inline() {
        let content = vec![0u8; 100_000];
        let files = vec![shardline_index::hub::HubFileEntry {
            path: "big.txt".into(),
            size: content.len() as u64,
            sha: "e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8".into(),
            is_lfs: false,
        }];
        let (_ts, state) = setup_resolve_state(
            &files,
            &[(
                "e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8e8",
                &content,
            )],
        );
        let result = resolve_file_from_store(&state, "sha_resolve", "big.txt").unwrap();
        match &result {
            DownloadResult::Inline { size, .. } => {
                // size <= MAX_INLINE_SIZE → Inline
                assert!(*size <= 1_048_576);
            }
            _ => assert!(
                matches!(result, DownloadResult::Inline { .. }),
                "expected Inline, got {result:?}"
            ),
        }
    }

    #[test]
    fn resolve_file_from_store_large_non_lfs_file_above_max_inline() {
        // size > MAX_INLINE_SIZE (1 MiB) and !is_lfs → Inline (else branch)
        let size = super::MAX_INLINE_SIZE + 1;
        let files = vec![shardline_index::hub::HubFileEntry {
            path: "huge.txt".into(),
            size,
            sha: "f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9".into(),
            is_lfs: false,
        }];
        let (_ts, state) = setup_resolve_state(&files, &[]);
        let result = resolve_file_from_store(&state, "sha_resolve", "huge.txt").unwrap();
        match &result {
            DownloadResult::Inline {
                size: s, content, ..
            } => {
                assert_eq!(*s, size);
                assert!(content.is_none());
            }
            _ => assert!(
                matches!(result, DownloadResult::Inline { .. }),
                "expected Inline, got {result:?}"
            ),
        }
    }
}
