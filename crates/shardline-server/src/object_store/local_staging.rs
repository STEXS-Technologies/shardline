//! Keep durable multipart working files beside their final object-store keys.
use std::{ffi::OsStr, io};

use shardline_storage::{AnchoredPathOptions, AnchoredTarget, LocalObjectStore, ObjectKey};

/// The caller retains the anchor through publication and uses the ordinary
/// maintenance barrier while receiving. Unpublished files remain in the
/// existing resumable staging namespace for crash reclamation.
pub(crate) fn local_resumable_staging_file(
    store: &LocalObjectStore,
    prefix: &str,
) -> io::Result<(tempfile::NamedTempFile, AnchoredTarget)> {
    let target = ObjectKey::parse(&format!("{prefix}/receiving"))
        .map_err(|_error| invalid_staging_path())?;
    if !target.as_str().starts_with("staging/resumable/") {
        return Err(invalid_staging_path());
    }
    let anchor = shardline_storage::open_anchored_target(
        store.root(),
        &store.path_for_key(&target),
        AnchoredPathOptions::new(None, None),
        invalid_staging_path,
    )?;
    let directory = shardline_storage::fd_child_path(anchor.parent_dir(), OsStr::new("."));
    let temporary = tempfile::Builder::new()
        .prefix("receiving-")
        .tempfile_in(directory)?;
    shardline_storage::ensure_parent_path_matches_anchor(&anchor, "staging parent changed")?;
    Ok((temporary, anchor))
}

fn invalid_staging_path() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid resumable staging path",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::{MetadataExt as _, symlink};

    #[test]
    fn working_file_is_on_store_filesystem_and_dropped_without_publication() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(root.path().join("objects")).unwrap();
        let prefix = "staging/resumable/s3/session/1";
        let (temporary, anchor) = local_resumable_staging_file(&store, prefix).unwrap();
        assert_eq!(
            temporary.as_file().metadata().unwrap().dev(),
            std::fs::metadata(store.root()).unwrap().dev()
        );
        let logical = store.root().join(prefix);
        assert_eq!(std::fs::read_dir(&logical).unwrap().count(), 1);
        drop(temporary);
        drop(anchor);
        assert_eq!(std::fs::read_dir(logical).unwrap().count(), 0);
    }

    #[test]
    fn verified_publication_retains_the_receiving_inode() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(root.path().join("objects")).unwrap();
        let prefix = "staging/resumable/s3/session/1";
        let (mut temporary, anchor) = local_resumable_staging_file(&store, prefix).unwrap();
        let bytes = b"durable multipart payload";
        temporary.write_all(bytes).unwrap();
        let inode = temporary.as_file().metadata().unwrap().ino();
        let digest = blake3::hash(bytes);
        let key = ObjectKey::parse(&format!("{prefix}/{}", digest.to_hex())).unwrap();
        let integrity = shardline_storage::ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes()),
            bytes.len() as u64,
        );
        shardline_storage::ensure_parent_path_matches_anchor(&anchor, "changed").unwrap();
        assert_eq!(
            store
                .put_temporary_file_if_absent(&key, temporary.path(), &integrity)
                .unwrap(),
            shardline_storage::PutOutcome::Inserted
        );
        let published = store.path_for_key(&key);
        assert_eq!(std::fs::metadata(&published).unwrap().ino(), inode);
        assert_eq!(std::fs::read(published).unwrap(), bytes);
        assert!(!temporary.path().exists());
    }

    #[test]
    fn staging_rejects_other_namespaces_and_symlinked_parents() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(root.path().join("objects")).unwrap();
        assert!(local_resumable_staging_file(&store, "protocols/s3/user").is_err());
        symlink(outside.path(), store.root().join("staging")).unwrap();
        assert!(local_resumable_staging_file(&store, "staging/resumable/s3/session/1").is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }
}
