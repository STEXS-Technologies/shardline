use std::collections::HashMap;

use crate::error::HubApiError;
use crate::models::TreeEntry;

/// In-memory file tree store.
#[derive(Debug, Clone, Default)]
pub struct TreeStore {
    inner: std::sync::Arc<tokio::sync::RwLock<HashMap<String, Vec<FileEntry>>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct FileEntry {
    pub path: String,
    pub size: u64,
    pub sha: String,
    pub is_lfs: bool,
}

impl TreeStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores files for a given commit SHA.
    pub(crate) async fn store_files(
        &self,
        commit_sha: &str,
        files: Vec<FileEntry>,
    ) {
        let mut inner = self.inner.write().await;
        inner.insert(commit_sha.to_owned(), files);
    }

    /// Returns the file tree at a given commit SHA and path.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::RevisionNotFound`] if the commit does not exist.
    pub async fn tree_at(
        &self,
        commit_sha: &str,
        path: &str,
    ) -> Result<Vec<TreeEntry>, HubApiError> {
        let inner = self.inner.read().await;
        let files = inner
            .get(commit_sha)
            .ok_or(HubApiError::RevisionNotFound)?;

        let mut entries = Vec::new();
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };

        let mut seen_dirs = std::collections::HashSet::new();

        for file in files {
            if !prefix.is_empty() && !file.path.starts_with(&prefix) {
                continue;
            }
            let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);

            if let Some((dir, _rest)) = relative.split_once('/') {
                if seen_dirs.insert(dir.to_owned()) {
                    entries.push(TreeEntry {
                        entry_type: "directory".to_owned(),
                        path: dir.to_owned(),
                        size: None,
                        lfs: None,
                    });
                }
            } else {
                let lfs = if file.is_lfs {
                    Some(crate::models::TreeEntryLfs {
                        oid: file.sha.clone(),
                        size: file.size,
                    })
                } else {
                    None
                };
                entries.push(TreeEntry {
                    entry_type: "file".to_owned(),
                    path: relative.to_owned(),
                    size: Some(file.size),
                    lfs,
                });
            }
        }

        entries.sort_by(|a, b| {
            a.entry_type
                .cmp(&b.entry_type)
                .then_with(|| a.path.cmp(&b.path))
        });

        Ok(entries)
    }

    /// Returns all files at a commit SHA.
    pub(crate) async fn all_files(&self, commit_sha: &str) -> Vec<FileEntry> {
        let inner = self.inner.read().await;
        inner.get(commit_sha).cloned().unwrap_or_default()
    }
}
