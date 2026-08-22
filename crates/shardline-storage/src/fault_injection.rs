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
    /// While temporary-file bytes are being written.
    DuringTemporaryWrite,
    /// Immediately before synchronizing the temporary file.
    BeforeTemporarySync,
    /// After all temporary-file bytes have been synchronized to durable storage.
    AfterTemporaryDurable,
    /// After the final name has been linked or renamed into place.
    AfterInstall,
    /// Immediately before synchronizing the containing directory.
    BeforeParentSync,
    /// After the containing directory has been synchronized.
    AfterParentDurable,
}

/// Typed local-I/O failure injected at a [`LocalPublishBoundary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocalPublishFault {
    /// Simulates a process interruption at a persistence boundary.
    Interrupted,
    /// Simulates a filesystem reporting `ENOSPC`.
    OutOfSpace,
    /// Simulates a filesystem reporting `EIO`.
    InputOutput,
    /// Writes only a prefix before returning [`io::ErrorKind::WriteZero`].
    PartialWrite,
    /// Simulates `fsync` reporting `EIO`.
    SyncFailure,
}

#[cfg(not(any(test, feature = "test-fault-injection")))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) const fn local_publish_failpoint(
    _path: &Path,
    _boundary: LocalPublishBoundary,
) -> io::Result<()> {
    Ok(())
}

#[cfg(not(any(test, feature = "test-fault-injection")))]
pub(crate) const fn local_publish_partial_write_len(
    _path: &Path,
    _requested: usize,
) -> Option<usize> {
    None
}

#[cfg(any(test, feature = "test-fault-injection"))]
pub(crate) fn local_publish_failpoint(
    path: &Path,
    boundary: LocalPublishBoundary,
) -> io::Result<()> {
    enabled::hit(path, boundary)
}

#[cfg(any(test, feature = "test-fault-injection"))]
pub(crate) fn local_publish_partial_write_len(path: &Path, requested: usize) -> Option<usize> {
    enabled::partial_write_len(path, requested)
}

#[cfg(any(test, feature = "test-fault-injection"))]
pub use enabled::{LocalPublishFailpointGuard, arm, arm_fault};

#[cfg(any(test, feature = "test-fault-injection"))]
mod enabled {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::{LazyLock, Mutex},
    };

    use super::{LocalPublishBoundary, LocalPublishFault, io};

    #[derive(Debug, Clone, Copy)]
    struct ArmedFault {
        boundary: LocalPublishBoundary,
        fault: LocalPublishFault,
        registration_id: u64,
    }

    static NEXT_REGISTRATION_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(1);
    static FAILPOINTS: LazyLock<Mutex<HashMap<PathBuf, ArmedFault>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Removes one armed local publication fault when dropped.
    pub struct LocalPublishFailpointGuard {
        path: PathBuf,
        registration_id: u64,
    }

    impl Drop for LocalPublishFailpointGuard {
        fn drop(&mut self) {
            let mut failpoints = FAILPOINTS.lock().unwrap_or_else(|error| error.into_inner());
            if failpoints
                .get(&self.path)
                .is_some_and(|armed| armed.registration_id == self.registration_id)
            {
                failpoints.remove(&self.path);
            }
        }
    }

    /// Arms a generic interruption at one typed local publication boundary.
    #[must_use]
    pub fn arm(path: PathBuf, boundary: LocalPublishBoundary) -> LocalPublishFailpointGuard {
        arm_fault(path, boundary, LocalPublishFault::Interrupted)
    }

    /// Arms a typed local-I/O fault at one publication boundary.
    #[must_use]
    pub fn arm_fault(
        path: PathBuf,
        boundary: LocalPublishBoundary,
        fault: LocalPublishFault,
    ) -> LocalPublishFailpointGuard {
        let registration_id =
            NEXT_REGISTRATION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                path.clone(),
                ArmedFault {
                    boundary,
                    fault,
                    registration_id,
                },
            );
        LocalPublishFailpointGuard {
            path,
            registration_id,
        }
    }

    pub(super) fn hit(path: &Path, boundary: LocalPublishBoundary) -> io::Result<()> {
        let fault = FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path)
            .filter(|armed| armed.boundary == boundary)
            .map(|armed| armed.fault);
        match fault {
            None => Ok(()),
            Some(LocalPublishFault::Interrupted) => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("injected local publication interruption at {boundary:?}"),
            )),
            Some(LocalPublishFault::OutOfSpace) => Err(io::Error::from_raw_os_error(libc::ENOSPC)),
            Some(LocalPublishFault::InputOutput | LocalPublishFault::SyncFailure) => {
                Err(io::Error::from_raw_os_error(libc::EIO))
            }
            Some(LocalPublishFault::PartialWrite) => Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "injected partial local publication write",
            )),
        }
    }

    pub(super) fn partial_write_len(path: &Path, requested: usize) -> Option<usize> {
        FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path)
            .filter(|armed| {
                armed.boundary == LocalPublishBoundary::DuringTemporaryWrite
                    && armed.fault == LocalPublishFault::PartialWrite
            })
            .map(|_armed| requested / 2)
    }
}
