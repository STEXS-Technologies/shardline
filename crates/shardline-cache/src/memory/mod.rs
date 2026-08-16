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

/// How long a `get`/`get_or_load` caller tolerates an in-flight loader before
/// declaring it orphaned and cleaning up its loading latch. Bounds the wait for
/// the same orphaned-loader condition in both `get` and `get_or_load`.
const LOADER_STALL_TIMEOUT: Duration = Duration::from_secs(30);

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
            // Someone else is loading — wait for them.
            //
            // `tokio::sync::Notify` does not retain a permit: if the loader
            // finishes and fires `notify_waiters()` before this waiter has
            // polled its `notified()` future, the wakeup is LOST and a bare
            // `notify.notified().await` would hang forever. Re-check the cache
            // and loading map after every wakeup (and before the first wait),
            // looping to re-arm the notification while the loader is still
            // running. Wakes are bounded by LOADER_STALL_TIMEOUT so an
            // orphaned loader (one that never notifies) cannot hang the caller.
            loop {
                {
                    let inner = self.inner.read().await;
                    let now = Instant::now();
                    if let Some(entry) = inner.entries.get(key)
                        && entry.expires_at > now
                    {
                        return Ok(Some(entry.payload.as_ref().clone()));
                    }
                    if !inner.loading.contains_key(key) {
                        // The loading latch vanished without a stored value
                        // (the loader failed or was cancelled). Report the miss
                        // so the caller can retry; the loader that did run was
                        // the only party allowed to put() + notify.
                        break;
                    }
                }

                tokio::select! {
                    () = notify.notified() => {}
                    () = tokio::time::sleep(LOADER_STALL_TIMEOUT) => {
                        // The loader may have finished and stored a value right
                        // before this timeout (a lost wakeup). Re-check before
                        // declaring it orphaned.
                        let inner = self.inner.read().await;
                        let now = Instant::now();
                        if let Some(entry) = inner.entries.get(key)
                            && entry.expires_at > now
                        {
                            return Ok(Some(entry.payload.as_ref().clone()));
                        }
                        drop(inner);
                        // The loader is orphaned: release its latch so later
                        // callers can retry immediately, then report the miss.
                        let mut write_guard = self.inner.write().await;
                        write_guard.loading.remove(key);
                        break;
                    }
                }
            }
            Ok(None)
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
                // If the loader fails, the Notify is never fired and
                // subsequent get() calls for this key would hang forever.
                // Use a timeout so we can clean up the orphaned loading
                // entry and let the next caller retry.
                tokio::select! {
                    () = notify.notified() => {}
                    () = tokio::time::sleep(LOADER_STALL_TIMEOUT) => {
                        let mut write_guard = self.inner.write().await;
                        write_guard.loading.remove(key);
                        return Ok(None);
                    }
                }

                let read_inner = self.inner.read().await;
                if let Some(entry) = read_inner.entries.get(key)
                    && entry.expires_at > Instant::now()
                {
                    return Ok(Some(entry.payload.as_ref().clone()));
                }
                return Ok(None);
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
