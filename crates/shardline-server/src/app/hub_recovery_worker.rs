//! Bounded background drain for the Hub ref recovery outbox.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use shardline_index::{
    HubRecoveryOutboxStore, MAX_PENDING_REPOSITORY_PAGE, archive_next_hub_ref_event,
};
use shardline_server_core::ServerObjectStore;
use tokio::task::JoinHandle;

use crate::admission::timeouts;

const PAGE_SIZE: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(size) => size,
    None => NonZeroUsize::MIN,
};
const IDLE_POLL: Duration = Duration::from_secs(1);
const INITIAL_RETRY: Duration = Duration::from_millis(200);
const MAX_RETRY: Duration = Duration::from_secs(10);
const STOP_POLL: Duration = Duration::from_millis(100);

pub(super) fn spawn(
    outbox: Arc<dyn HubRecoveryOutboxStore>,
    archive: ServerObjectStore,
    signing_key: SigningKey,
    key_id: String,
    stopped: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(run(outbox, archive, signing_key, key_id, stopped))
}

async fn run(
    outbox: Arc<dyn HubRecoveryOutboxStore>,
    archive: ServerObjectStore,
    signing_key: SigningKey,
    key_id: String,
    stopped: Arc<AtomicBool>,
) {
    let maximum_page_size =
        NonZeroUsize::new(MAX_PENDING_REPOSITORY_PAGE).unwrap_or(NonZeroUsize::MIN);
    let page_size = PAGE_SIZE.min(maximum_page_size);
    let mut cursor: Option<String> = None;
    let mut retry = INITIAL_RETRY;
    let mut pass_had_failure = false;

    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }

        let result = drain_page(
            outbox.as_ref(),
            &archive,
            &signing_key,
            &key_id,
            &stopped,
            &mut cursor,
            page_size,
        )
        .await;

        let delay = match result {
            Ok(PageOutcome::Stopped) => return,
            Ok(PageOutcome::Processed) => Duration::ZERO,
            Ok(PageOutcome::PassComplete) => {
                if pass_had_failure {
                    pass_had_failure = false;
                    let current = retry;
                    retry = retry.saturating_mul(2).min(MAX_RETRY);
                    current
                } else {
                    retry = INITIAL_RETRY;
                    Duration::ZERO
                }
            }
            Ok(PageOutcome::Empty) => {
                retry = INITIAL_RETRY;
                IDLE_POLL
            }
            Ok(PageOutcome::PartialFailure) => {
                pass_had_failure = true;
                Duration::ZERO
            }
            Err(error) => {
                tracing::warn!(%error, "Hub recovery archive drain failed; pending rows will retry");
                pass_had_failure = true;
                let current = retry;
                retry = retry.saturating_mul(2).min(MAX_RETRY);
                current
            }
        };
        wait_or_stopped(delay, &stopped).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageOutcome {
    Stopped,
    Empty,
    PassComplete,
    Processed,
    PartialFailure,
}

async fn drain_page(
    outbox: &dyn HubRecoveryOutboxStore,
    archive: &ServerObjectStore,
    signing_key: &SigningKey,
    key_id: &str,
    stopped: &AtomicBool,
    cursor: &mut Option<String>,
    page_size: NonZeroUsize,
) -> Result<PageOutcome, shardline_index::HubRecoveryError> {
    let pass_active = cursor.is_some();
    let repositories = tokio::select! {
        result = tokio::time::timeout(
            timeouts::REQUEST_TOTAL,
            outbox.pending_hub_ref_repositories_after(cursor.as_deref(), page_size),
        ) => result.map_err(|_elapsed| {
            shardline_index::HubRecoveryError::Outbox("pending repository page timed out".to_owned())
        })??,
        () = wait_until_stopped(stopped) => return Ok(PageOutcome::Stopped),
    };
    if repositories.is_empty() {
        *cursor = None;
        return Ok(if pass_active {
            PageOutcome::PassComplete
        } else {
            PageOutcome::Empty
        });
    }

    let mut failed = false;
    for repository_id in repositories {
        if stopped.load(Ordering::Relaxed) {
            return Ok(PageOutcome::Processed);
        }
        let result = tokio::select! {
            result = tokio::time::timeout(
                timeouts::REQUEST_TOTAL,
                archive_next_hub_ref_event(outbox, archive, &repository_id, signing_key, key_id),
            ) => result,
            () = wait_until_stopped(stopped) => return Ok(PageOutcome::Stopped),
        };
        match result {
            Ok(Ok(_archived)) => {}
            Ok(Err(error)) => {
                tracing::warn!(%repository_id, %error, "Hub recovery repository failed; continuing bounded page");
                failed = true;
            }
            Err(_elapsed) => {
                tracing::warn!(%repository_id, "Hub recovery repository exceeded its operation deadline; continuing bounded page");
                failed = true;
            }
        }
        // Advance through this bounded page even on a failed repository so it
        // cannot block later repositories. The keyset wraps after an empty page
        // and retries this row; the SQL archived watermark advances only on
        // the archive helper's verified success path.
        *cursor = Some(repository_id);
    }
    Ok(if failed {
        PageOutcome::PartialFailure
    } else {
        PageOutcome::Processed
    })
}

async fn wait_or_stopped(delay: Duration, stopped: &AtomicBool) {
    if delay.is_zero() {
        tokio::task::yield_now().await;
        return;
    }
    let started = tokio::time::Instant::now();
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        let remaining = delay.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return;
        }
        tokio::time::sleep(remaining.min(STOP_POLL)).await;
    }
}

async fn wait_until_stopped(stopped: &AtomicBool) {
    while !stopped.load(Ordering::Relaxed) {
        tokio::time::sleep(STOP_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use shardline_index::{
        HubRecoveryArchiveObject, HubRecoveryError, HubRecoveryMode, HubRecoveryOutboxItem,
    };

    use super::*;

    struct StopOnPage {
        stopped: Arc<AtomicBool>,
        page_calls: Arc<AtomicUsize>,
        stop_on_page: bool,
        repositories: Vec<String>,
        fail_repository: Option<String>,
        next_calls: Arc<AtomicUsize>,
        mark_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HubRecoveryOutboxStore for StopOnPage {
        async fn hub_recovery_mode(
            &self,
        ) -> Result<(HubRecoveryMode, Option<String>), HubRecoveryError> {
            Ok((HubRecoveryMode::Off, None))
        }

        async fn pending_hub_ref_repositories_after(
            &self,
            _cursor: Option<&str>,
            limit: NonZeroUsize,
        ) -> Result<Vec<String>, HubRecoveryError> {
            assert_eq!(limit.get(), 16);
            self.page_calls.fetch_add(1, Ordering::Relaxed);
            if self.stop_on_page {
                self.stopped.store(true, Ordering::Relaxed);
            }
            Ok(self.repositories.clone())
        }

        async fn set_hub_recovery_mode(
            &self,
            _mode: HubRecoveryMode,
            _deployment_id: Option<&str>,
        ) -> Result<(), HubRecoveryError> {
            Ok(())
        }

        async fn next_pending_hub_ref_event(
            &self,
            repository_id: &str,
        ) -> Result<Option<HubRecoveryOutboxItem>, HubRecoveryError> {
            self.next_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_repository.as_deref() == Some(repository_id) {
                Err(HubRecoveryError::Outbox(
                    "injected outbox failure".to_owned(),
                ))
            } else {
                Ok(None)
            }
        }

        async fn mark_hub_ref_event_archived(
            &self,
            _item: &HubRecoveryOutboxItem,
            _archive: &HubRecoveryArchiveObject,
        ) -> Result<(), HubRecoveryError> {
            self.mark_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn drain_stops_promptly_after_the_shutdown_flag_is_set() {
        let stopped = Arc::new(AtomicBool::new(false));
        let page_calls = Arc::new(AtomicUsize::new(0));
        let next_calls = Arc::new(AtomicUsize::new(0));
        let mark_calls = Arc::new(AtomicUsize::new(0));
        let outbox: Arc<dyn HubRecoveryOutboxStore> = Arc::new(StopOnPage {
            stopped: stopped.clone(),
            page_calls: page_calls.clone(),
            stop_on_page: true,
            repositories: Vec::new(),
            fail_repository: None,
            next_calls,
            mark_calls,
        });

        run(
            outbox,
            ServerObjectStore::blackhole(),
            SigningKey::from_bytes(&[1; 32]),
            "key-1".to_owned(),
            stopped,
        )
        .await;

        assert_eq!(page_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn failed_repository_does_not_block_later_repositories_or_advance_archive_watermark() {
        let stopped = AtomicBool::new(false);
        let page_calls = Arc::new(AtomicUsize::new(0));
        let next_calls = Arc::new(AtomicUsize::new(0));
        let mark_calls = Arc::new(AtomicUsize::new(0));
        let outbox = StopOnPage {
            stopped: Arc::new(AtomicBool::new(false)),
            page_calls: page_calls.clone(),
            stop_on_page: false,
            repositories: vec!["repo-a".to_owned(), "repo-b".to_owned()],
            fail_repository: Some("repo-a".to_owned()),
            next_calls: next_calls.clone(),
            mark_calls: mark_calls.clone(),
        };
        let mut cursor = Some("prior".to_owned());

        let result = drain_page(
            &outbox,
            &ServerObjectStore::blackhole(),
            &SigningKey::from_bytes(&[1; 32]),
            "key-1",
            &stopped,
            &mut cursor,
            PAGE_SIZE,
        )
        .await;

        assert!(matches!(result, Ok(PageOutcome::PartialFailure)));
        assert_eq!(next_calls.load(Ordering::Relaxed), 2);
        assert_eq!(mark_calls.load(Ordering::Relaxed), 0);
        assert_eq!(cursor.as_deref(), Some("repo-b"));
    }

    #[tokio::test]
    async fn end_of_pass_wraps_without_being_classified_as_idle() {
        let stopped = AtomicBool::new(false);
        let outbox = StopOnPage {
            stopped: Arc::new(AtomicBool::new(false)),
            page_calls: Arc::new(AtomicUsize::new(0)),
            stop_on_page: false,
            repositories: Vec::new(),
            fail_repository: None,
            next_calls: Arc::new(AtomicUsize::new(0)),
            mark_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut cursor = Some("last-repository".to_owned());

        let result = drain_page(
            &outbox,
            &ServerObjectStore::blackhole(),
            &SigningKey::from_bytes(&[1; 32]),
            "key-1",
            &stopped,
            &mut cursor,
            PAGE_SIZE,
        )
        .await;

        assert!(matches!(result, Ok(PageOutcome::PassComplete)));
        assert!(cursor.is_none());
    }
}
