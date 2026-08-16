use std::{
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{Notify, RwLock};
use tokio::time::Instant;

use crate::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheFuture,
    ReconstructionCacheKey,
};

mod inner;

#[cfg(test)]
mod tests;

use inner::{CacheInner, MemoryEntry};

/// TOTAL time a waiter tolerates a loading latch that never stores a value
/// before declaring its loader orphaned and releasing the latch.
///
/// A short single-interval wait is NOT enough to declare a loader orphaned:
/// reconstructions of large files routinely exceed 30s, and stealing the latch
/// while a still-running loader is alive breaks the dedup contract (the service
/// layer treats `Ok(None)` as a miss and starts a second load). The waiter
/// therefore waits through the full bound — re-examining the cache whenever the
/// loader's `Notify` fires — and only releases the latch once THIS total bound
/// elapses with no value stored. A slow-but-alive loader keeps its latch, while
/// a genuinely-dead loader (panicked without putting/notifying) still cannot
/// wedge waiters forever.
const LOADER_ORPHAN_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);

/// Bounded in-memory reconstruction cache adapter.
#[derive(Debug, Clone)]
pub struct MemoryReconstructionCache {
    ttl: Duration,
    max_entries: NonZeroUsize,
    inner: Arc<RwLock<CacheInner>>,
}

impl MemoryReconstructionCache {
    /// Creates a bounded in-memory reconstruction cache.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_cache::MemoryReconstructionCache;
    /// use std::num::{NonZeroU64, NonZeroUsize};
    ///
    /// // Entries expire after 60 seconds; at most 1_000 are kept.
    /// let ttl = NonZeroU64::try_from(60)?;
    /// let max_entries = NonZeroUsize::try_from(1_000)?;
    /// let cache = MemoryReconstructionCache::new(ttl, max_entries);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn new(ttl_seconds: NonZeroU64, max_entries: NonZeroUsize) -> Self {
        Self {
            ttl: Duration::from_secs(ttl_seconds.get()),
            max_entries,
            inner: Arc::new(RwLock::new(CacheInner::new())),
        }
    }

    /// Returns the cached value for `key`, or computes it with `loader`.
    ///
    /// Concurrent calls for the same key are deduplicated — only one caller
    /// runs the loader; the rest await the result.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the loader fails or the
    /// underlying cache operation encounters an error.
    pub async fn get_or_load<F, Fut>(
        &self,
        key: &ReconstructionCacheKey,
        loader: F,
    ) -> Result<Option<Vec<u8>>, ReconstructionCacheError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, ReconstructionCacheError>>,
    {
        // Fast path: check the cache without a write lock.
        {
            let inner = self.inner.read().await;
            let now = Instant::now();
            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                return Ok(Some(entry.payload.as_ref().clone()));
            }
        }

        // Try to become the exclusive loader for this key.
        let (should_load, notify) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();

            // Re-check after acquiring the write lock.
            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                return Ok(Some(entry.payload.as_ref().clone()));
            }

            // Check if someone else is already loading this key.
            // Use separate branches on .is_some() to avoid the clippy
            // option_if_let_else lint (which flags both `if let Some`
            // and `match { Some, None }` on Option).
            let (should_load, notify) = if inner.loading.contains_key(key) {
                // SAFETY: contains_key was just checked — get() returns Some.
                // Use unwrap_or_else with a non-panicking default to avoid
                // denied lints while satisfying the type system.
                let dummy = Arc::new(Notify::new());
                let existing = inner.loading.get(key).unwrap_or(&dummy);
                (false, Arc::clone(existing))
            } else {
                let new_notify = Arc::new(Notify::new());
                inner.loading.insert(key.clone(), Arc::clone(&new_notify));
                // Clean up any expired entry so the loader can store fresh data.
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at <= now
                {
                    inner.remove(key);
                }
                (true, new_notify)
            };
            (should_load, notify)
        };

        if should_load {
            let result = loader().await;
            match result {
                Ok(payload) => {
                    self.put(key, &payload).await?;
                    Ok(Some(payload))
                }
                Err(e) => {
                    // Clean up the loading entry so future callers can retry.
                    let mut inner = self.inner.write().await;
                    inner.loading.remove(key);
                    notify.notify_waiters();
                    Err(e)
                }
            }
        } else {
            // Someone else is loading — wait for them (bounded). `wait_for_loader`
            // re-enters for a second interval instead of stealing the latch while
            // a slow-but-alive loader may still be running (F-68).
            Ok(self.wait_for_loader(key, notify).await)
        }
    }

    /// Waits for an in-flight loader of `key` to store a value.
    ///
    /// Returns `Some(payload)` when the loader stores a value, and `None` when
    /// the loading latch disappears without one (loader failed, was cancelled,
    /// or the key was deleted) or the total orphan bound elapses with the latch
    /// still present.
    ///
    /// `tokio::sync::Notify` does not retain a permit: if the loader finishes
    /// and fires `notify_waiters()` before this waiter has polled its
    /// `notified()` future, the wakeup is LOST and a bare `notified().await`
    /// would hang forever. The loop therefore re-checks the cache and loading
    /// map after every wakeup (and before the first wait), re-arming the
    /// notification while the loader is still running.
    async fn wait_for_loader(
        &self,
        key: &ReconstructionCacheKey,
        notify: Arc<Notify>,
    ) -> Option<Vec<u8>> {
        let deadline = Instant::now()
            .checked_add(LOADER_ORPHAN_TOTAL_TIMEOUT)
            .unwrap_or_else(Instant::now);
        loop {
            {
                let inner = self.inner.read().await;
                let now = Instant::now();
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at > now
                {
                    return Some(entry.payload.as_ref().clone());
                }
                if !inner.loading.contains_key(key) {
                    // The loading latch vanished without a stored value (the
                    // loader failed or was cancelled, or the key was deleted).
                    // Report the miss so the caller can retry; the loader that
                    // ran was the only party allowed to put() + notify.
                    return None;
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(remaining) => {
                    // Re-check before giving up: the loader may have finished
                    // and stored a value right before the deadline (a lost
                    // wakeup).
                    let inner = self.inner.read().await;
                    let now = Instant::now();
                    if let Some(entry) = inner.entries.get(key)
                        && entry.expires_at > now
                    {
                        return Some(entry.payload.as_ref().clone());
                    }
                    drop(inner);
                    // The total orphan bound elapsed and no value arrived. The
                    // loader is presumed dead (panicked without notifying):
                    // release the latch so later callers can retry promptly,
                    // and wake sibling waiters so they observe the release too.
                    let mut write_guard = self.inner.write().await;
                    write_guard.loading.remove(key);
                    drop(write_guard);
                    notify.notify_waiters();
                    return None;
                }
            }
        }
    }
}

impl AsyncReconstructionCache for MemoryReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async move {
            let now = Instant::now();
            {
                let inner = self.inner.read().await;
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at > now
                {
                    return Ok(Some(entry.payload.as_ref().clone()));
                } else if !inner.loading.contains_key(key) {
                    return Ok(None);
                }
            }

            let mut inner = self.inner.write().await;

            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                return Ok(Some(entry.payload.as_ref().clone()));
            }

            if let Some(notify) = inner.loading.get(key) {
                let notify = Arc::clone(notify);
                drop(inner);
                // Wait for the in-flight loader (bounded). The latch is NOT
                // stolen after a single stall interval: a slow-but-alive loader
                // keeps its latch so the caller's load() stays deduplicated
                // (F-68).
                return Ok(self.wait_for_loader(key, notify).await);
            }

            let notify = Arc::new(Notify::new());
            inner.loading.insert(key.clone(), Arc::clone(&notify));

            let should_remove = inner
                .entries
                .get(key)
                .is_some_and(|entry| entry.expires_at <= now);
            if should_remove {
                inner.remove(key);
            }
            Ok(None)
        })
    }

    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async move {
            let now = Instant::now();
            let expires_at = now.checked_add(self.ttl).unwrap_or(now);
            let mut inner = self.inner.write().await;
            if !inner.entries.contains_key(key) && inner.entries.len() >= self.max_entries.get() {
                inner.evict_oldest();
            }
            let seq = inner.next_seq;
            inner.next_seq = inner.next_seq.saturating_add(1);
            inner.insert(
                key,
                MemoryEntry {
                    payload: Arc::new(payload.to_vec()),
                    expires_at,
                    inserted_at: now,
                    seq,
                },
            );
            if let Some(notify) = inner.loading.remove(key) {
                notify.notify_waiters();
            }
            Ok(())
        })
    }

    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let mut inner = self.inner.write().await;
            let removed = inner.remove(key).is_some();
            // Also release any in-flight loading latch for this key. The key is
            // explicitly gone, so waiting callers must wake and observe an
            // absence rather than block until the adapter's stall timeout. This
            // is what lets a failed loader's concurrency latch be cleaned up.
            if let Some(notify) = inner.loading.remove(key) {
                notify.notify_waiters();
            }
            Ok(removed)
        })
    }
}
