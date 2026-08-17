use std::{
    collections::HashMap,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
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

use inner::{CacheInner, LoadingEntry, MemoryEntry};

/// TOTAL time a waiter tolerates a loading latch that never stores a value
/// before declaring its loader orphaned and releasing the latch.
///
/// A short single-interval wait is NOT enough to declare a loader orphaned:
/// reconstructions of large files routinely exceed 30s, and stealing the latch
/// while a still-running loader is alive breaks the dedup contract (the service
/// layer treats `Ok(None)` as a miss and starts a second load). The waiter
/// therefore waits through the full bound — re-examining the cache whenever the
/// loader's `Notify` fires — and only releases the latch once THIS total bound
/// elapses with no value stored. Before releasing, the waiter re-checks the
/// loader's aliveness stamp ([`LoadingEntry::last_seen_alive`]): a loader that
/// refreshes its stamp while running is slow but alive, so the waiter extends
/// its bound instead of stealing the latch (F-78).
///
/// The extension is capped at [`LOADER_ORPHAN_EXTENSION_CAP`] consecutive
/// intervals (F-100): a slow-but-progressing loader keeps its latch, but a
/// loader that never completes — while still yielding so its heartbeat keeps
/// its stamp fresh — cannot pin waiters beyond `(1 + cap) × 60s`. This
/// preserves the "cannot wedge waiters forever" guarantee even when the
/// aliveness stamp never goes stale.
const LOADER_ORPHAN_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum number of consecutive alive-extension intervals a waiter grants to
/// a loader that keeps refreshing its aliveness stamp at the orphan bound.
///
/// Combined with [`LOADER_ORPHAN_TOTAL_TIMEOUT`] this caps the total time a
/// waiter tolerates an in-flight latch at `(1 + 4) × 60s = 300s`, matching the
/// server's `admission::timeouts::REQUEST_TOTAL` request budget: a request that
/// waited that long for a loader is about to be cut off by the server anyway,
/// and a wedged-but-pollable loader must not pin its waiters past that point
/// (F-100).
const LOADER_ORPHAN_EXTENSION_CAP: u32 = 4;

/// Interval at which an in-flight loader refreshes its aliveness stamp while
/// still running. Smaller than [`LOADER_ALIVE_GRACE`] so a waiter that wakes
/// at the orphan bound always observes a fresh stamp for a live loader.
const LOADER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// How stale a loading latch's aliveness stamp may be before a waiter at the
/// orphan bound declares the loader dead and releases the latch. Larger than
/// the heartbeat interval so a loader that just missed a tick is not stolen
/// from, while a genuinely-dead latch (never heartbeated) is still cleared at
/// the first orphan bound.
const LOADER_ALIVE_GRACE: Duration = Duration::from_secs(10);

/// Bounded in-memory reconstruction cache adapter.
#[derive(Debug, Clone)]
pub struct MemoryReconstructionCache {
    ttl: Duration,
    max_entries: NonZeroUsize,
    inner: Arc<RwLock<CacheInner>>,
    /// In-flight loading latches, keyed by reconstruction key.
    ///
    /// Kept behind a *synchronous* mutex (rather than inside [`CacheInner`]'s
    /// async `RwLock`) so a `Drop` guard can release a latch synchronously
    /// when a caller's future is dropped mid-load — an async lock cannot be
    /// acquired from `Drop` (F-112).
    loading: Arc<Mutex<HashMap<ReconstructionCacheKey, LoadingEntry>>>,
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
            loading: Arc::new(Mutex::new(HashMap::new())),
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
        let (should_load, notify, last_seen_alive) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();

            // Re-check after acquiring the write lock.
            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                return Ok(Some(entry.payload.as_ref().clone()));
            }

            // Check if someone else is already loading this key. The loading
            // map lives behind a synchronous mutex so the loader's Drop guard
            // can release a latch synchronously on caller cancellation
            // (F-112). Use separate branches on contains_key() to avoid the
            // clippy option_if_let_else lint.
            let mut loading = self
                .loading
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if loading.contains_key(key) {
                // SAFETY: contains_key was just checked — get() returns Some.
                // Use unwrap_or_else with a non-panicking default to avoid
                // denied lints while satisfying the type system.
                let dummy = LoadingEntry::new(Arc::new(Notify::new()));
                let existing = loading.get(key).unwrap_or(&dummy);
                (
                    false,
                    Arc::clone(&existing.notify),
                    Arc::clone(&existing.last_seen_alive),
                )
            } else {
                let new = LoadingEntry::new(Arc::new(Notify::new()));
                let notify = Arc::clone(&new.notify);
                let last_seen_alive = Arc::clone(&new.last_seen_alive);
                loading.insert(key.clone(), new);
                // Clean up any expired entry so the loader can store fresh data.
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at <= now
                {
                    inner.remove(key);
                }
                (true, notify, last_seen_alive)
            }
        };

        if should_load {
            // F-112: if the caller's future is dropped mid-load (client
            // disconnect, request timeout, shutdown), the loading latch must
            // be released. The Drop guard removes it synchronously on
            // cancellation, so a zombie latch never stalls the next caller for
            // the full orphan bound and the loading map never grows without
            // bound. Normal completion is unaffected — put() (success) or the
            // guard (error) releases the latch, and while the caller is alive
            // the latch persists so concurrent callers still coalesce on this
            // one load (F-90).
            let _latch_guard = LoadingLatchGuard {
                loading: Arc::clone(&self.loading),
                key: key.clone(),
                notify: Arc::clone(&notify),
            };
            let result = run_loader_with_heartbeat(last_seen_alive, loader).await;
            match result {
                Ok(payload) => {
                    self.put(key, &payload).await?;
                    Ok(Some(payload))
                }
                Err(e) => {
                    // The guard above releases the loading latch and wakes
                    // waiters on the way out.
                    Err(e)
                }
            }
        } else {
            // Someone else is loading — wait for them (bounded). `wait_for_loader`
            // re-enters for a second interval instead of stealing the latch while
            // a slow-but-alive loader may still be running (F-68), and only
            // releases the latch once the loader's aliveness stamp goes stale
            // (F-78).
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
    ///
    /// At the deadline the waiter re-checks the loader's aliveness stamp before
    /// releasing the latch: a loader that refreshes its stamp while running is
    /// slow but alive, so the waiter extends its bound and keeps waiting; only
    /// a stale stamp (genuinely dead loader) — or a saturated
    /// [`LOADER_ORPHAN_EXTENSION_CAP`] (a wedged-but-pollable loader, F-100) —
    /// causes the latch to be stolen (F-78).
    async fn wait_for_loader(
        &self,
        key: &ReconstructionCacheKey,
        notify: Arc<Notify>,
    ) -> Option<Vec<u8>> {
        let mut deadline = Instant::now()
            .checked_add(LOADER_ORPHAN_TOTAL_TIMEOUT)
            .unwrap_or_else(Instant::now);
        // Consecutive alive-extensions granted so far. Capped by
        // LOADER_ORPHAN_EXTENSION_CAP so a never-completing loader whose stamp
        // stays fresh cannot pin waiters past the bounded total (F-100).
        let mut alive_extensions: u32 = 0;
        loop {
            {
                let inner = self.inner.read().await;
                let now = Instant::now();
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at > now
                {
                    return Some(entry.payload.as_ref().clone());
                }
                if !self
                    .loading
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains_key(key)
                {
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
                    // Aliveness re-check: a running loader refreshes its stamp
                    // at intervals, so a fresh stamp means it is slow but alive.
                    // Extend the wait instead of stealing its latch (F-78); the
                    // latch is only released when the stamp has gone stale
                    // (the loader genuinely died without putting or notifying)
                    // or the extension cap is saturated (the loader is alive
                    // but wedged — F-100).
                    let loader_alive = {
                        let loading = self
                            .loading
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        loading.get(key).is_some_and(|entry| {
                            now.saturating_duration_since(
                                *entry
                                    .last_seen_alive
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                            ) <= LOADER_ALIVE_GRACE
                        })
                    };
                    drop(inner);
                    if loader_alive && alive_extensions < LOADER_ORPHAN_EXTENSION_CAP {
                        alive_extensions = alive_extensions.saturating_add(1);
                        deadline = now
                            .checked_add(LOADER_ORPHAN_TOTAL_TIMEOUT)
                            .unwrap_or(now);
                        continue;
                    }
                    // The total orphan bound elapsed with no stored value and
                    // either no recent aliveness (the loader is presumed dead —
                    // panicked without putting or notifying) or the extension
                    // cap was reached (a wedged-but-pollable loader that keeps
                    // its stamp fresh but never completes, F-100). Release the
                    // latch so later callers can retry promptly, and wake
                    // sibling waiters so they observe the release too.
                    let released = {
                        let mut loading = self
                            .loading
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        loading.remove(key).map(|entry| entry.notify)
                    };
                    if let Some(released) = released {
                        released.notify_waiters();
                    }
                    return None;
                }
            }
        }
    }
}

/// Runs a loader future while refreshing the loading latch's aliveness stamp
/// at intervals. The waiter at the orphan bound re-checks the stamp: a fresh
/// stamp proves the loader is slow but alive and the wait must be extended; a
/// stale stamp means the loader died without putting or notifying (F-78).
async fn run_loader_with_heartbeat<F, Fut>(
    last_seen_alive: Arc<Mutex<Instant>>,
    loader: F,
) -> Result<Vec<u8>, ReconstructionCacheError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Vec<u8>, ReconstructionCacheError>>,
{
    let mut loader_future = std::pin::pin!(loader());
    let mut heartbeat = tokio::time::interval(LOADER_HEARTBEAT_INTERVAL);
    loop {
        tokio::select! {
            result = &mut loader_future => return result,
            _ = heartbeat.tick() => {
                *last_seen_alive
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
            }
        }
    }
}

/// Drop guard that releases a loading latch when the loader future is dropped.
///
/// A caller that is cancelled mid-load (client disconnect, request timeout,
/// shutdown) drops its future without ever reaching `put()`/`delete()`. The
/// latch registered by [`MemoryReconstructionCache::get_or_load`] would then
/// linger until a waiter steals it at the orphan bound — stalling the next
/// caller for the full bound and growing the loading map without bound under
/// caller churn. The guard removes the latch synchronously from `Drop`, so
/// cancellation releases it immediately (F-112).
///
/// Normal completion is unaffected: `put()` removes the latch on success, and
/// the guard's drop is a no-op once the latch is gone. The guard only ever
/// removes the latch it registered — identified by its [`Notify`] — so a newer
/// generation of loading for the same key is never torn down.
struct LoadingLatchGuard {
    loading: Arc<Mutex<HashMap<ReconstructionCacheKey, LoadingEntry>>>,
    key: ReconstructionCacheKey,
    notify: Arc<Notify>,
}

impl Drop for LoadingLatchGuard {
    fn drop(&mut self) {
        let mut loading = self
            .loading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = loading.get(&self.key) else {
            // The latch was already released by put()/delete()/a waiter steal.
            return;
        };
        if !Arc::ptr_eq(&entry.notify, &self.notify) {
            // A newer loading generation holds the latch; leave it intact so
            // its callers keep coalescing on it.
            return;
        }
        loading.remove(&self.key);
        drop(loading);
        self.notify.notify_waiters();
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
            // Fast path: serve a live cached entry without a write lock.
            {
                let inner = self.inner.read().await;
                if let Some(entry) = inner.entries.get(key)
                    && entry.expires_at > now
                {
                    return Ok(Some(entry.payload.as_ref().clone()));
                }
                // No early return on a miss: a cold miss (or an expired entry)
                // falls through to the write-lock path, which registers a
                // loading latch so concurrent callers deduplicate on the load
                // the caller is about to run (F-91).
            }

            let mut inner = self.inner.write().await;

            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                return Ok(Some(entry.payload.as_ref().clone()));
            }

            let existing_notify = {
                let loading = self
                    .loading
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loading.get(key).map(|entry| Arc::clone(&entry.notify))
            };
            if let Some(notify) = existing_notify {
                drop(inner);
                // Wait for the in-flight loader (bounded). The latch is NOT
                // stolen after a single stall interval: a slow-but-alive loader
                // keeps its latch so the caller's load() stays deduplicated
                // (F-68), and the waiter extends past the total bound while the
                // loader's aliveness stamp stays fresh (F-78), up to the
                // extension cap (F-100).
                return Ok(self.wait_for_loader(key, notify).await);
            }

            // No cached entry and no in-flight loader: register a loading latch
            // for the caller's upcoming load. The caller is expected to put()
            // (which stores the value and releases the latch) or delete()
            // (which releases the latch on failure) — otherwise the latch
            // lingers until a waiter steals it at the orphan bound or a Drop
            // guard in the caller's future releases it on cancellation
            // (F-112). Without this latch, N concurrent callers would each run
            // their own load with no deduplication (F-91).
            let loading = LoadingEntry::new(Arc::new(Notify::new()));
            self.loading
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(key.clone(), loading);

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
            let released = {
                let mut loading = self
                    .loading
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loading.remove(key)
            };
            if let Some(released) = released {
                released.notify.notify_waiters();
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
            let released = {
                let mut loading = self
                    .loading
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loading.remove(key)
            };
            if let Some(released) = released {
                released.notify.notify_waiters();
            }
            Ok(removed)
        })
    }

    fn touch_loading<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let loading = self
                .loading
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(entry) = loading.get(key) else {
                return Ok(false);
            };
            *entry
                .last_seen_alive
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
            Ok(true)
        })
    }

    fn release_loading(&self, key: &ReconstructionCacheKey) {
        let released = {
            let mut loading = self
                .loading
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loading.remove(key)
        };
        if let Some(released) = released {
            released.notify.notify_waiters();
        }
    }
}
