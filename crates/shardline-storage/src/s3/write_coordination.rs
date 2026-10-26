//! Coordinate overlapping creates without caching an object's integrity.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::ObjectKey;

type Entries = Mutex<HashMap<ObjectKey, Weak<WriteEntry>>>;

#[derive(Default)]
pub(super) struct WriteCoordination {
    entries: Arc<Entries>,
}

#[derive(Debug)]
struct WriteEntry {
    key: ObjectKey,
    entries: Weak<Entries>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for WriteEntry {
    fn drop(&mut self) {
        let Some(entries) = self.entries.upgrade() else {
            return;
        };
        let mut entries = entries.lock().unwrap_or_else(|error| error.into_inner());
        // A new caller can replace this expired weak entry before cleanup gets
        // the registry mutex. Never remove that caller's new coordination lock.
        if entries
            .get(&self.key)
            .is_some_and(|candidate| std::ptr::eq(candidate.as_ptr(), self))
        {
            entries.remove(&self.key);
        }
    }
}

#[derive(Debug)]
pub(super) struct WriteGuard {
    // Fields drop in declaration order: unlock before releasing registration.
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _entry: Arc<WriteEntry>,
}

impl WriteCoordination {
    pub(super) async fn acquire(&self, key: &ObjectKey) -> WriteGuard {
        let entry = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            entries.get(key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let entry = Arc::new(WriteEntry {
                    key: key.clone(),
                    entries: Arc::downgrade(&self.entries),
                    lock: Arc::new(tokio::sync::Mutex::new(())),
                });
                entries.insert(key.clone(), Arc::downgrade(&entry));
                entry
            })
        };
        // Registration spans the wait as well as the write. Cancellation drops
        // it too, so inactive keys need neither a full-map scan nor a reaper.
        let guard = Arc::clone(&entry.lock).lock_owned().await;
        WriteGuard {
            _guard: guard,
            _entry: entry,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_a_woken_waiter_releases_the_last_registration() {
        for cancel_before_unlock in [false, true] {
            let coordination = WriteCoordination::default();
            let key = ObjectKey::parse("chunks/cancelled").unwrap();
            let owner = coordination.acquire(&key).await;
            let mut waiter = Box::pin(coordination.acquire(&key));
            assert!(futures_util::poll!(waiter.as_mut()).is_pending());
            if cancel_before_unlock {
                drop(waiter);
                assert_eq!(coordination.entries.lock().unwrap().len(), 1);
                drop(owner);
            } else {
                drop(owner);
                drop(waiter);
            }
            assert!(coordination.entries.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_churn_preserves_same_key_exclusion() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let coordination = Arc::new(WriteCoordination::default());
        let active = Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let mut tasks = tokio::task::JoinSet::new();
        for task in 0..128 {
            let coordination = Arc::clone(&coordination);
            let active = Arc::clone(&active);
            tasks.spawn(async move {
                for iteration in 0..16 {
                    let index = (task + iteration) % active.len();
                    let key = ObjectKey::parse(&format!("chunks/churn-{index}")).unwrap();
                    let _owner = coordination.acquire(&key).await;
                    assert_eq!(active[index].fetch_add(1, Ordering::SeqCst), 0);
                    tokio::task::yield_now().await;
                    assert_eq!(active[index].fetch_sub(1, Ordering::SeqCst), 1);
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert!(coordination.entries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "explicit CPU diagnostic; run alone with --ignored --exact --nocapture"]
    async fn many_live_keys_coordination_cpu_diagnostic() {
        let coordination = WriteCoordination::default();
        let mut live = Vec::new();
        for index in 0..4096 {
            let key = ObjectKey::parse(&format!("chunks/live-{index}")).unwrap();
            live.push(coordination.acquire(&key).await);
        }
        let started = std::time::Instant::now();
        for index in 0..32768 {
            let key = ObjectKey::parse(&format!("chunks/transient-{index}")).unwrap();
            drop(coordination.acquire(&key).await);
        }
        eprintln!(
            "4096 live keys, 32768 transient acquisitions: {:?}",
            started.elapsed()
        );
        drop(live);
    }

    #[tokio::test]
    async fn unrelated_keys_progress_and_cancelled_waiters_release_ownership() {
        let coordination = Arc::new(WriteCoordination::default());
        let key = ObjectKey::parse("chunks/busy").unwrap();
        let owner = coordination.acquire(&key).await;
        let waiting = {
            let coordination = Arc::clone(&coordination);
            let key = key.clone();
            tokio::spawn(async move { coordination.acquire(&key).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let other = ObjectKey::parse("chunks/independent").unwrap();
        let independent = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coordination.acquire(&other),
        )
        .await
        .unwrap();
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        drop(owner);
        drop(independent);
        let replacement = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coordination.acquire(&key),
        )
        .await
        .unwrap();
        // Completed operations are gone; only the replacement remains live.
        assert_eq!(coordination.entries.lock().unwrap().len(), 1);
        drop(replacement);
    }
}
