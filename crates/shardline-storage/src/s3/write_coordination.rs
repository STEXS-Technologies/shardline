//! Coordinate overlapping creates without caching an object's integrity.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::ObjectKey;

#[derive(Default)]
pub(super) struct WriteCoordination {
    entries: Mutex<HashMap<ObjectKey, Weak<tokio::sync::Mutex<()>>>>,
}

impl WriteCoordination {
    pub(super) async fn acquire(&self, key: &ObjectKey) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // Keep only live operations. Neither historical keys nor object
            // bytes accumulate as the service ingests new content.
            entries.retain(|_, lock| lock.strong_count() > 0);
            entries.get(key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entries.insert(key.clone(), Arc::downgrade(&lock));
                lock
            })
        };
        lock.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Historical keys are pruned on the next acquisition.
        assert_eq!(coordination.entries.lock().unwrap().len(), 1);
        drop(replacement);
    }
}
