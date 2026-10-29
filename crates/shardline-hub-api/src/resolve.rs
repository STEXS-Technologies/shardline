use shardline_server_core::AuthorizedRepository;

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
/// Returns `DownloadResult::LfsRedirect` for LFS-backed files of any size.
/// Other files stream directly from the resolve route.
///
/// # Errors
///
/// Returns [`HubApiError`] if the revision or file is not found.
pub fn resolve_file_from_store_scoped(
    state: &HubState,
    commit_sha: &str,
    file_path: &str,
    _auth: &AuthorizedRepository,
) -> Result<DownloadResult, HubApiError> {
    let files = state
        .store
        .get_files(commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let file = files
        .iter()
        .find(|f| f.path == file_path)
        .ok_or(HubApiError::NotFound)?;

    if file.is_lfs {
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

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_storage::ObjectStore;

    // --- DownloadResult enum ---

    #[test]
    #[allow(clippy::panic)]
    fn download_result_inline_has_correct_fields() {
        let result = DownloadResult::Inline {
            size: 100,
            sha: "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6".to_owned(),
        };

        match result {
            DownloadResult::Inline { size, sha } => {
                assert_eq!(size, 100);
                assert_eq!(
                    sha,
                    "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6"
                );
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
        };

        match result {
            DownloadResult::Inline { size, sha } => {
                assert_eq!(size, 500);
                assert_eq!(sha, "def456");
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
    #[test]
    fn persisted_storage_kind_controls_resolution_on_both_sides_of_old_size_boundary() {
        for size in [0, 1, 32 * 1024, 1_048_576, 1_048_577, 5_000_000] {
            for is_lfs in [false, true] {
                let sha = "a".repeat(64);
                let files = [shardline_index::hub::HubFileEntry {
                    path: "file.bin".to_owned(),
                    size,
                    sha: sha.clone(),
                    is_lfs,
                }];
                let (_root, state) = setup_resolve_state(&files, &[]);
                let result = resolve_file_from_store(&state, "sha_resolve", "file.bin").unwrap();
                match result {
                    DownloadResult::LfsRedirect { oid, size: actual } => {
                        assert!(is_lfs);
                        assert_eq!((oid, actual), (sha, size));
                    }
                    DownloadResult::Inline {
                        sha: actual,
                        size: length,
                    } => {
                        assert!(!is_lfs);
                        assert_eq!((actual, length), (sha, size));
                    }
                }
            }
        }
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
            public_base_url: "http://127.0.0.1:8080".to_owned(),
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
            DownloadResult::Inline { size, sha } => {
                assert_eq!(*size, content_data.len() as u64);
                assert_eq!(
                    sha.as_str(),
                    "c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6c6"
                );
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
                // Non-LFS content streams directly regardless of size.
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
        // Non-LFS content above the old 1 MiB boundary still streams directly.
        let size = 1_048_577;
        let files = vec![shardline_index::hub::HubFileEntry {
            path: "huge.txt".into(),
            size,
            sha: "f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9".into(),
            is_lfs: false,
        }];
        let (_ts, state) = setup_resolve_state(&files, &[]);
        let result = resolve_file_from_store(&state, "sha_resolve", "huge.txt").unwrap();
        match &result {
            DownloadResult::Inline { size: s, .. } => {
                assert_eq!(*s, size);
            }
            _ => assert!(
                matches!(result, DownloadResult::Inline { .. }),
                "expected Inline, got {result:?}"
            ),
        }
    }
}
