#[cfg(test)]
mod tests {
    use std::fs::metadata;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    use shardline_storage::anchored_fs::{
        AnchoredPathOptions,
        open_anchored_target as open_anchored_target_shared,
        write_anchored_temporary_file as write_anchored_temporary_file_shared,
    };
    #[cfg(unix)]
    use std::{fs::rename, io, path::Path};

    #[cfg(unix)]
    const LOCAL_DIRECTORY_MODE: u32 = 0o700;
    #[cfg(unix)]
    const LOCAL_FILE_MODE: u32 = 0o600;

    #[cfg(unix)]
    fn anchored_path_options() -> AnchoredPathOptions {
        AnchoredPathOptions::new(Some(LOCAL_DIRECTORY_MODE), Some(LOCAL_FILE_MODE))
    }

    #[cfg(unix)]
    fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
        use shardline_storage::anchored_fs::remove_if_present;

        let anchored = open_anchored_target_shared(
            root,
            path,
            anchored_path_options(),
            || {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "local filesystem path must remain under the configured root",
                )
            },
        )?;
        let temporary =
            write_anchored_temporary_file_shared(&anchored, bytes, anchored_path_options().file_mode)?;
        let final_path = anchored.final_path();
        match rename(&temporary, &final_path) {
            Ok(()) => {}
            Err(error) => {
                remove_if_present(&temporary)?;
                return Err(error);
            }
        }

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn write_file_atomically_creates_private_file_and_directory_modes() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let root = sandbox.path().join("root");
        let path = root.join("nested").join("record.json");

        let wrote = write_file_atomically(&root, &path, br#"{"ok":true}"#);
        assert!(wrote.is_ok());

        let file_metadata = metadata(&path);
        assert!(file_metadata.is_ok());
        let Ok(file_metadata) = file_metadata else {
            return;
        };
        let directory_metadata = metadata(root.join("nested"));
        assert!(directory_metadata.is_ok());
        let Ok(directory_metadata) = directory_metadata else {
            return;
        };

        assert_eq!(file_metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(directory_metadata.permissions().mode() & 0o777, 0o700);
    }
}
