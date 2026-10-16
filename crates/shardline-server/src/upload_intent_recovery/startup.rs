//! File verification belongs to the router lifetime, outside readiness startup.
use crate::{ServerBackend, ServerError};
use shardline_index::UploadIntent;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) enum RecoveryPass {
    Complete,
    Startup,
    Files(Arc<AtomicBool>),
}

impl RecoveryPass {
    pub(crate) fn stopped(&self) -> bool {
        matches!(self, Self::Files(stopped) if stopped.load(Ordering::Relaxed))
    }

    pub(crate) fn visits(&self, intent: &UploadIntent) -> bool {
        !matches!(self, Self::Files(_))
            || (intent.object_key().starts_with("record/")
                && super::file_publication_may_have_completed(intent))
    }

    pub(crate) const fn defers_file_bytes(&self) -> bool {
        matches!(self, Self::Startup)
    }
}

#[derive(Debug)]
pub(crate) struct RecoveryGuard {
    stopped: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RecoveryGuard {
    pub(crate) fn start(backend: ServerBackend, stopped: Arc<AtomicBool>) -> Self {
        let pass = RecoveryPass::Files(stopped.clone());
        let task = tokio::spawn(async move {
            let result: Result<bool, ServerError> = match backend {
                ServerBackend::Local(backend) => {
                    backend
                        .reconcile_upload_intents(std::time::Duration::from_secs(30), &pass)
                        .await
                }
                ServerBackend::Postgres(backend) => {
                    backend
                        .reconcile_upload_intents(std::time::Duration::from_secs(30), &pass)
                        .await
                }
            };
            if let Err(error) = result
                && !pass.stopped()
            {
                tracing::warn!(%error, "embedded file upload recovery unavailable; unfinished evidence preserved");
            }
        });
        Self { stopped, task }
    }
}

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

#[cfg(test)]
mod tests;
