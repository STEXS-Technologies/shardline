use std::{
    fs,
    io::{self, ErrorKind},
    path::{Component, Path, PathBuf},
};

/// Directory path validation failure.
#[derive(Debug)]
pub enum DirectoryPathError {
    /// The path contained a platform-specific prefix unsupported by Shardline.
    UnsupportedPrefix,
    /// An existing path component was a symlink.
    SymlinkedComponent(PathBuf),
    /// An existing path component was not a directory.
    NonDirectoryComponent(PathBuf),
    /// The filesystem could not inspect a path component.
    Io(io::Error),
}

/// Verifies that each existing directory component in `path` is a real directory, not a symlink.
///
/// # Errors
///
/// Returns [`DirectoryPathError`] when a component uses an unsupported prefix, resolves to a
/// symlink, resolves to a non-directory, or cannot be inspected.
pub fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), DirectoryPathError> {
    let mut current = if path.is_absolute() {
        PathBuf::from("/")
    } else {
        PathBuf::from(".")
    };

    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                current.push(component.as_os_str());
                validate_existing_directory_component(&current)?;
            }
            Component::Normal(segment) => {
                current.push(segment);
                validate_existing_directory_component(&current)?;
            }
            Component::Prefix(_prefix) => return Err(DirectoryPathError::UnsupportedPrefix),
        }
    }

    Ok(())
}

fn validate_existing_directory_component(path: &Path) -> Result<(), DirectoryPathError> {
    let check_path = resolve_platform_symlinks(path);
    match fs::symlink_metadata(&check_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(DirectoryPathError::SymlinkedComponent(path.to_path_buf()))
        }
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_metadata) => Err(DirectoryPathError::NonDirectoryComponent(
            path.to_path_buf(),
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(DirectoryPathError::Io(error)),
    }
}

/// Resolves well-known platform symlinks in the path without resolving user-created symlinks.
///
/// On macOS, `/var`, `/etc`, and `/tmp` are symlinks to `/private/var`, `/private/etc`, and
/// `/private/tmp`. Opening files under these paths with `O_NOFOLLOW` or SQLite's `NOFOLLOW`
/// fails because the symlink component is not followed. This resolves the known platform
/// symlinks so the flag applies only to the final path component.
#[cfg(target_os = "macos")]
#[must_use]
pub fn resolve_platform_symlinks(path: &Path) -> PathBuf {
    let components: Vec<_> = path.components().collect();
    if let [Component::RootDir, Component::Normal(first), rest @ ..] = components.as_slice() {
        let resolved_prefix = match first.as_encoded_bytes() {
            b"var" | b"etc" | b"tmp" => Some("/private"),
            _ => None,
        };
        if let Some(prefix) = resolved_prefix {
            let mut resolved = PathBuf::from(prefix);
            resolved.push(first);
            for component in rest {
                resolved.push(component.as_os_str());
            }
            return resolved;
        }
    }
    path.to_path_buf()
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn resolve_platform_symlinks(path: &Path) -> PathBuf {
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    #[cfg(unix)]
    use std::fs::create_dir_all;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use super::{DirectoryPathError, ensure_directory_path_components_are_not_symlinked};

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_ancestor_component() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let target = sandbox.path().join("target");
        let create = create_dir_all(&target);
        assert!(create.is_ok());
        let link = sandbox.path().join("link");
        let linked = symlink(&target, &link);
        assert!(linked.is_ok());

        let result = ensure_directory_path_components_are_not_symlinked(&link.join("child"));

        assert!(matches!(
            result,
            Err(DirectoryPathError::SymlinkedComponent(path)) if path == link
        ));
    }

    #[test]
    fn rejects_non_directory_ancestor_component() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let file_path = sandbox.path().join("file");
        let file = File::create(&file_path);
        assert!(file.is_ok());

        let result = ensure_directory_path_components_are_not_symlinked(&file_path.join("child"));

        assert!(matches!(
            result,
            Err(DirectoryPathError::NonDirectoryComponent(path)) if path == file_path
        ));
    }

    #[test]
    fn allows_missing_components() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };

        let result =
            ensure_directory_path_components_are_not_symlinked(&sandbox.path().join("missing"));

        assert!(result.is_ok());
    }
}
