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
pub use enabled::{UploadLifecycleFailpointGuard, arm_upload_interruption};

#[cfg(any(test, feature = "test-fault-injection"))]
mod enabled {
    use std::{
        collections::HashMap,
        sync::{LazyLock, Mutex},
    };

    use super::UploadLifecycleBoundary;

    #[derive(Debug, Clone, Copy)]
    struct ArmedFault {
        boundary: UploadLifecycleBoundary,
        registration_id: u64,
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
                },
            );
        UploadLifecycleFailpointGuard {
            intent_id,
            registration_id,
        }
    }

    pub(super) fn hit(
        intent_id: &str,
        boundary: UploadLifecycleBoundary,
    ) -> Result<(), crate::CasError> {
        let interrupted = FAILPOINTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(intent_id)
            .is_some_and(|armed| armed.boundary == boundary);
        if interrupted {
            Err(crate::CasError::InjectedUploadInterruption { boundary })
        } else {
            Ok(())
        }
    }
}
