/// Typed durability boundary in the coordinator-owned upload lifecycle.
///
/// These variants are shared by the state machine and deterministic tests so
/// persistence transitions cannot be selected through string matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UploadLifecycleBoundary {
    /// The durable intent exists in `Created` state.
    AfterIntentCreated,
    /// The durable intent advanced to `Storing`.
    AfterStoring,
    /// Object-store work returned success, before the `Stored` state commit.
    AfterObjectWork,
    /// The durable intent advanced to `Stored`.
    AfterStored,
    /// The durable intent advanced to `MetadataCommitted`.
    AfterMetadataCommitted,
    /// The durable intent advanced to `Visible`.
    AfterVisible,
}

#[cfg(not(any(test, feature = "test-fault-injection")))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) const fn upload_lifecycle_failpoint(
    _intent_id: &str,
    _boundary: UploadLifecycleBoundary,
) -> Result<(), crate::CasError> {
    Ok(())
}

#[cfg(any(test, feature = "test-fault-injection"))]
pub(crate) fn upload_lifecycle_failpoint(
    intent_id: &str,
    boundary: UploadLifecycleBoundary,
) -> Result<(), crate::CasError> {
    enabled::hit(intent_id, boundary)
}

#[cfg(any(test, feature = "test-fault-injection"))]
pub use enabled::{
    UploadLifecycleFailpointGuard, arm_upload_interruption, arm_upload_process_pause,
    arm_upload_process_pause_for_prefix,
};

#[cfg(any(test, feature = "test-fault-injection"))]
mod enabled {
    use std::{
        collections::HashMap,
        sync::{LazyLock, Mutex},
    };

    use super::UploadLifecycleBoundary;

    #[derive(Debug, Clone)]
    struct ArmedFault {
        boundary: UploadLifecycleBoundary,
        registration_id: u64,
        pause_marker: Option<std::path::PathBuf>,
        match_prefix: bool,
    }

    static NEXT_REGISTRATION_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(1);
    static FAILPOINTS: LazyLock<Mutex<HashMap<String, ArmedFault>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Removes one armed upload-lifecycle fault when dropped.
    pub struct UploadLifecycleFailpointGuard {
        intent_id: String,
        registration_id: u64,
    }

    impl Drop for UploadLifecycleFailpointGuard {
        fn drop(&mut self) {
            let mut failpoints = FAILPOINTS.lock().unwrap_or_else(|error| error.into_inner());
            if failpoints
                .get(&self.intent_id)
                .is_some_and(|armed| armed.registration_id == self.registration_id)
            {
                failpoints.remove(&self.intent_id);
            }
        }
    }

    /// Arms an interruption at one typed upload-lifecycle boundary.
    #[must_use]
    pub fn arm_upload_interruption(
        intent_id: String,
        boundary: UploadLifecycleBoundary,
    ) -> UploadLifecycleFailpointGuard {
        let registration_id =
            NEXT_REGISTRATION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                intent_id.clone(),
                ArmedFault {
                    boundary,
                    registration_id,
                    pause_marker: None,
                    match_prefix: false,
                },
            );
        UploadLifecycleFailpointGuard {
            intent_id,
            registration_id,
        }
    }

    /// Pauses a test process at a durable boundary until its parent kills it.
    /// The marker is written only after reaching that boundary. This facility
    /// is unavailable in builds without the explicit fault-injection feature.
    #[must_use]
    pub fn arm_upload_process_pause(
        intent_id: String,
        boundary: UploadLifecycleBoundary,
        marker: std::path::PathBuf,
    ) -> UploadLifecycleFailpointGuard {
        let guard = arm_upload_interruption(intent_id, boundary);
        if let Some(fault) = FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(&guard.intent_id)
        {
            fault.pause_marker = Some(marker);
        }
        guard
    }

    /// Pauses a matching upload whose exact identity is chosen by a stock client.
    /// Only explicit test-fault-injection builds expose this process-kill hook.
    /// The marker records the actual matched intent, never a client credential.
    ///
    /// # Panics
    /// Panics if `prefix` is empty, preventing an accidental match of all uploads.
    #[must_use]
    pub fn arm_upload_process_pause_for_prefix(
        prefix: String,
        boundary: UploadLifecycleBoundary,
        marker: std::path::PathBuf,
    ) -> UploadLifecycleFailpointGuard {
        assert!(!prefix.is_empty(), "upload fault prefix must be nonempty");
        let guard = arm_upload_process_pause(prefix, boundary, marker);
        if let Some(fault) = FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(&guard.intent_id)
        {
            fault.match_prefix = true;
        }
        guard
    }

    pub(super) fn hit(
        intent_id: &str,
        boundary: UploadLifecycleBoundary,
    ) -> Result<(), crate::CasError> {
        let fault = {
            let failpoints = FAILPOINTS.lock().unwrap_or_else(|error| error.into_inner());
            failpoints
                .get(intent_id)
                .filter(|armed| armed.boundary == boundary)
                .or_else(|| {
                    failpoints.iter().find_map(|(prefix, armed)| {
                        (armed.match_prefix
                            && armed.boundary == boundary
                            && intent_id.starts_with(prefix))
                        .then_some(armed)
                    })
                })
                .cloned()
        };
        if let Some(fault) = fault {
            if let Some(marker) = fault.pause_marker {
                std::fs::write(marker, intent_id.as_bytes())
                    .map_err(|error| crate::CasError::ObjectStore(error.to_string()))?;
                loop {
                    std::thread::park();
                }
            }
            Err(crate::CasError::InjectedUploadInterruption { boundary })
        } else {
            Ok(())
        }
    }
}
