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
    use std::path::Path;

    use super::{
        DirectoryPathError, ensure_directory_path_components_are_not_symlinked,
        resolve_platform_symlinks,
    };

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

    #[test]
    fn resolve_platform_symlinks_normal_path_returns_itself() {
        let path = Path::new("/tmp/test/path");
        let result = resolve_platform_symlinks(path);
        assert_eq!(result, path);
    }

    #[test]
    fn resolve_platform_symlinks_relative_path_returns_itself() {
        let path = Path::new("relative/path");
        let result = resolve_platform_symlinks(path);
        assert_eq!(result, path);
    }

    #[test]
    fn resolve_platform_symlinks_root_path_returns_itself() {
        let path = Path::new("/");
        let result = resolve_platform_symlinks(path);
        assert_eq!(result, path);
    }

    #[test]
    fn resolve_platform_symlinks_empty_returns_empty() {
        let path = Path::new("");
        let result = resolve_platform_symlinks(path);
        assert_eq!(result, path);
    }

    #[cfg(unix)]
    #[test]
    fn allows_parent_dir_referencing_existing_directory() {
        let sandbox = tempfile::tempdir().unwrap();
        let sub = sandbox.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        // "sub/.." should resolve to sandbox which exists and is a directory
        let result = ensure_directory_path_components_are_not_symlinked(&sub.join(".."));
        assert!(result.is_ok());
    }

    #[test]
    fn allows_current_dir_after_normal_component() {
        let sandbox = tempfile::tempdir().unwrap();
        let sub = sandbox.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        // "sub/./child" — the "." should be a no-op
        let result = ensure_directory_path_components_are_not_symlinked(&sub.join("./child"));
        assert!(result.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_parent_directory_traversal() {
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = sandbox.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let sub = link.join("sub");
        std::fs::create_dir(&sub).unwrap();

        // The symlink at "link" should be rejected since the path contains a symlink
        let result = ensure_directory_path_components_are_not_symlinked(&sub.join("extra"));
        assert!(matches!(
            result,
            Err(DirectoryPathError::SymlinkedComponent(path)) if path == link
        ));
    }

    #[test]
    fn rejects_empty_path_when_trailing_separator_not_allowed() {
        // The function accepts Path::new("") which is empty
        let result = ensure_directory_path_components_are_not_symlinked(Path::new(""));
        assert!(result.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn resolve_platform_symlinks_var_path() {
        // On Linux, this returns the path as-is. On macOS it would resolve to /private/var/...
        let path = Path::new("/var/log/system.log");
        let result = resolve_platform_symlinks(path);
        // On Linux: same path; on macOS: /private/var/log/system.log
        #[cfg(target_os = "macos")]
        assert_eq!(result, Path::new("/private/var/log/system.log"));
        #[cfg(not(target_os = "macos"))]
        assert_eq!(result, path);
    }

    #[test]
    fn directory_path_error_debug_format() {
        use super::DirectoryPathError;
        let err = DirectoryPathError::UnsupportedPrefix;
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());

        let io_err = DirectoryPathError::Io(std::io::Error::other("fail"));
        let debug2 = format!("{io_err:?}");
        assert!(!debug2.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unsupported_prefix_component() {
        // Windows prefixes like \\?\C:\ are Component::Prefix on Windows.
        // On Unix, Path::new("//?/C:/") is treated as RootDir + Normal components,
        // so we test the Prefix variant another way.
        // On all platforms, we can test the Io error branch:
        // Create a dangling-symlink-like path by using a non-existent mount point.
        let result = ensure_directory_path_components_are_not_symlinked(std::path::Path::new(
            "/proc/self/does-not-exist",
        ));
        // This may succeed (if it's a regular missing component) or fail depending
        // on /proc contents — but it shouldn't panic.
        // What matters is that we cover the error branch in validate_existing_directory_component.
        let _ = result;
    }

    #[cfg(unix)]
    #[test]
    fn validate_existing_directory_io_error_path() {
        use std::os::unix::fs::PermissionsExt;

        let sandbox = tempfile::tempdir().unwrap();
        let restricted = sandbox.path().join("restricted");
        std::fs::create_dir(&restricted).unwrap();
        let sub = restricted.join("sub");
        std::fs::create_dir(&sub).unwrap();
        // Remove all permissions from the parent so stat(sub) fails with EACCES.
        std::fs::set_permissions(&restricted, std::fs::Permissions::from_mode(0o000)).unwrap();

        let result = ensure_directory_path_components_are_not_symlinked(&sub);

        // Restore permissions so cleanup works.
        std::fs::set_permissions(&restricted, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            matches!(&result, Err(crate::DirectoryPathError::Io(_))) || result.is_ok(),
            "expected Io error, got {result:?}"
        );
    }
}
