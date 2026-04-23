#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
use shardline_index::RepositoryRecordScope;
use shardline_protocol::RepositoryProvider;
#[cfg(test)]
use shardline_protocol::RepositoryScope;

#[cfg(test)]
pub(crate) fn scoped_root(base: &Path, repository_scope: &RepositoryScope) -> PathBuf {
    let mut path = base
        .to_path_buf()
        .join(provider_directory(repository_scope.provider()))
        .join(path_component(repository_scope.owner()))
        .join(path_component(repository_scope.name()));
    if let Some(revision) = repository_scope.revision() {
        path = path.join(path_component(revision));
    }

    path
}

#[cfg(test)]
pub(crate) fn repository_scoped_root(
    base: &Path,
    repository_scope: &RepositoryRecordScope,
) -> PathBuf {
    base.to_path_buf()
        .join(provider_directory(repository_scope.provider()))
        .join(path_component(repository_scope.owner()))
        .join(path_component(repository_scope.name()))
}

pub(crate) const fn provider_directory(provider: RepositoryProvider) -> &'static str {
    provider.as_str()
}

#[cfg(test)]
fn path_component(value: &str) -> String {
    hex::encode(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use shardline_index::RepositoryRecordScope;
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{repository_scoped_root, scoped_root};

    #[test]
    fn scoped_root_encodes_repository_components_for_filesystem_safety() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "../team",
            "assets/..",
            Some("refs/heads/../main"),
        );
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        let path = scoped_root(Path::new("/var/lib/shardline/files"), &scope);

        assert_eq!(
            path,
            Path::new("/var/lib/shardline/files")
                .join("github")
                .join(hex::encode("../team"))
                .join(hex::encode("assets/.."))
                .join(hex::encode("refs/heads/../main"))
        );
    }

    #[test]
    fn repository_scoped_root_encodes_repository_record_scope_components() {
        let scope = RepositoryRecordScope::new(RepositoryProvider::GitLab, "../group", "assets/..");
        let path = repository_scoped_root(Path::new("/var/lib/shardline/file_versions"), &scope);

        assert_eq!(
            path,
            Path::new("/var/lib/shardline/file_versions")
                .join("gitlab")
                .join(hex::encode("../group"))
                .join(hex::encode("assets/.."))
        );
    }
}
