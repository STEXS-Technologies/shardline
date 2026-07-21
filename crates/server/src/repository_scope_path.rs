#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
use shardline_index::RepositoryRecordScope;
#[cfg(test)]
use shardline_protocol::RepositoryScope;
#[cfg(test)]
use shardline_server_core::provider_directory;

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

    #[test]
    fn scoped_root_without_revision_omits_revision_component() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };

        let path = scoped_root(Path::new("/base"), &scope);

        assert_eq!(
            path,
            Path::new("/base")
                .join("github")
                .join(hex::encode("org"))
                .join(hex::encode("repo"))
        );
    }

    #[test]
    fn repository_scoped_root_encodes_various_providers() {
        for provider in [
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
        ] {
            let scope = RepositoryRecordScope::new(provider, "owner", "name");
            let path = repository_scoped_root(Path::new("/data"), &scope);
            assert!(path.starts_with("/data"));
            // Components are hex-encoded, so check for hex of "owner" and "name"
            assert!(
                path.to_string_lossy().contains(&hex::encode("owner")),
                "path {:?} should contain hex-encoded 'owner'",
                path
            );
            assert!(
                path.to_string_lossy().contains(&hex::encode("name")),
                "path {:?} should contain hex-encoded 'name'",
                path
            );
        }
    }

    #[test]
    fn path_component_hex_encodes_value() {
        let result = super::path_component("hello");
        assert_eq!(result, hex::encode(b"hello"));
    }

    #[test]
    fn path_component_encodes_dangerous_characters() {
        let result = super::path_component("../etc");
        assert_eq!(result, hex::encode(b"../etc"));
        // Ensure the hex encoding does not contain path separators
        assert!(!result.contains('/'));
        assert!(!result.contains('.'));
    }
}
