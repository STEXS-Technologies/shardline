use std::{io, path::Path};

/// Typed durability boundary for local atomic object publication.
///
/// These variants are also the stable vocabulary used by deterministic chaos
/// tests. Adding a persistence transition requires adding an explicit boundary
/// rather than selecting a failpoint with a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocalPublishBoundary {
    /// Before a temporary file is created or written.
    BeforeTemporaryWrite,
    /// After all temporary-file bytes have been synchronized to durable storage.
    AfterTemporaryDurable,
    /// After the final name has been linked or renamed into place.
    AfterInstall,
    /// After the containing directory has been synchronized.
    AfterParentDurable,
}

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) const fn local_publish_failpoint(
    _path: &Path,
    _boundary: LocalPublishBoundary,
) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
pub(crate) fn local_publish_failpoint(
    path: &Path,
    boundary: LocalPublishBoundary,
) -> io::Result<()> {
    enabled::hit(path, boundary)
}

#[cfg(test)]
pub(crate) use enabled::{LocalPublishFailpointGuard, arm};

#[cfg(test)]
mod enabled {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::{LazyLock, Mutex},
    };

    use super::{LocalPublishBoundary, io};

    static NEXT_REGISTRATION_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(1);
    static FAILPOINTS: LazyLock<Mutex<HashMap<PathBuf, (LocalPublishBoundary, u64)>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    pub(crate) struct LocalPublishFailpointGuard {
        path: PathBuf,
        registration_id: u64,
    }

    impl Drop for LocalPublishFailpointGuard {
        fn drop(&mut self) {
            let mut failpoints = FAILPOINTS.lock().unwrap_or_else(|error| error.into_inner());
            if failpoints
                .get(&self.path)
                .is_some_and(|(_boundary, id)| *id == self.registration_id)
            {
                failpoints.remove(&self.path);
            }
        }
    }

    pub(crate) fn arm(path: PathBuf, boundary: LocalPublishBoundary) -> LocalPublishFailpointGuard {
        let registration_id =
            NEXT_REGISTRATION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(path.clone(), (boundary, registration_id));
        LocalPublishFailpointGuard {
            path,
            registration_id,
        }
    }

    pub(super) fn hit(path: &Path, boundary: LocalPublishBoundary) -> io::Result<()> {
        let armed = FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path)
            .is_some_and(|(expected, _id)| *expected == boundary);
        if armed {
            return Err(io::Error::other(format!(
                "injected local publication failure at {boundary:?}"
            )));
        }
        Ok(())
    }
}
