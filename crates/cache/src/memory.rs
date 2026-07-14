use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use tokio::sync::{Notify, RwLock};

use crate::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheFuture,
    ReconstructionCacheKey,
};

#[derive(Debug, Clone)]
struct MemoryEntry {
    payload: Arc<Vec<u8>>,
    expires_at: Instant,
    inserted_at: Instant,
    seq: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct EvictionKey(Instant, u64);

impl Ord for EvictionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for EvictionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct CacheInner {
    entries: HashMap<ReconstructionCacheKey, MemoryEntry>,
    eviction_order: BTreeMap<EvictionKey, ReconstructionCacheKey>,
    next_seq: u64,
    loading: HashMap<ReconstructionCacheKey, Arc<Notify>>,
}

impl CacheInner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            eviction_order: BTreeMap::new(),
            next_seq: 0,
            loading: HashMap::new(),
        }
    }

    fn insert(&mut self, key: &ReconstructionCacheKey, entry: MemoryEntry) {
        let inserted_at = entry.inserted_at;
        if let Some(old) = self.entries.insert(key.clone(), entry) {
            self.eviction_order
                .remove(&EvictionKey(old.inserted_at, old.seq));
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.eviction_order
            .insert(EvictionKey(inserted_at, seq), key.clone());
        // Keep the entry's seq in sync with the eviction_order key so that
        // remove() can find and delete the correct eviction entry.
        if let Some(cached) = self.entries.get_mut(key) {
            cached.seq = seq;
        }
    }

    fn remove(&mut self, key: &ReconstructionCacheKey) -> Option<MemoryEntry> {
        if let Some(entry) = self.entries.remove(key) {
            self.eviction_order
                .remove(&EvictionKey(entry.inserted_at, entry.seq));
            Some(entry)
        } else {
            None
        }
    }

    fn evict_oldest(&mut self) {
        while let Some((_eviction_key, key)) = self.eviction_order.pop_first() {
            if self.entries.contains_key(&key) {
                self.entries.remove(&key);
                return;
            }
        }
    }
}

/// Bounded in-memory reconstruction cache adapter.
#[derive(Debug, Clone)]
pub struct MemoryReconstructionCache {
    ttl: Duration,
    max_entries: NonZeroUsize,
    inner: Arc<RwLock<CacheInner>>,
}

impl MemoryReconstructionCache {
    /// Creates a bounded in-memory reconstruction cache.
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
            #[allow(clippy::option_if_let_else)]
            let (should_load, notify) = if let Some(existing) = inner.loading.get(key) {
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
            notify.notified().await;
            let inner = self.inner.read().await;
            let now = Instant::now();
            if let Some(entry) = inner.entries.get(key)
                && entry.expires_at > now
            {
                Ok(Some(entry.payload.as_ref().clone()))
            } else {
                Ok(None)
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
                // If the loader fails, the Notify is never fired and
                // subsequent get() calls for this key would hang forever.
                // Use a timeout so we can clean up the orphaned loading
                // entry and let the next caller retry.
                tokio::select! {
                    () = notify.notified() => {}
                    () = tokio::time::sleep(Duration::from_secs(30)) => {
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
            inner.insert(
                key,
                MemoryEntry {
                    payload: Arc::new(payload.to_vec()),
                    expires_at,
                    inserted_at: now,
                    seq: 0,
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
            Ok(inner.remove(key).is_some())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use super::MemoryReconstructionCache;
    use crate::{AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheKey};

    #[tokio::test]
    async fn memory_cache_roundtrips_one_payload() {
        let cache = MemoryReconstructionCache::new(NonZeroU64::MIN, NonZeroUsize::MIN);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let put = cache.put(&key, b"payload").await;
        assert!(put.is_ok());

        let value = cache.get(&key).await;

        assert!(value.is_ok());
        assert_eq!(value.ok(), Some(Some(b"payload".to_vec())));
    }

    #[tokio::test]
    async fn memory_cache_evicts_oldest_entry_when_capacity_is_full() {
        let max_entries = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let cache = MemoryReconstructionCache::new(ttl_seconds, max_entries);
        let first = ReconstructionCacheKey::latest("asset-1.bin", None);
        let second = ReconstructionCacheKey::latest("asset-2.bin", None);
        let third = ReconstructionCacheKey::latest("asset-3.bin", None);

        assert!(cache.put(&first, b"first").await.is_ok());
        assert!(cache.put(&second, b"second").await.is_ok());
        assert!(cache.put(&third, b"third").await.is_ok());

        let first_value = cache.get(&first).await;
        let second_value = cache.get(&second).await;
        let third_value = cache.get(&third).await;

        assert!(first_value.is_ok());
        assert!(second_value.is_ok());
        assert!(third_value.is_ok());
        assert_eq!(first_value.ok(), Some(None));
        assert_eq!(second_value.ok(), Some(Some(b"second".to_vec())));
        assert_eq!(third_value.ok(), Some(Some(b"third".to_vec())));
    }

    #[tokio::test(start_paused = true)]
    async fn memory_cache_expires_entries_after_ttl() {
        let ttl_seconds = NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN);
        let cache = MemoryReconstructionCache::new(ttl_seconds, NonZeroUsize::MIN);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        assert!(cache.put(&key, b"payload").await.is_ok());

        tokio::time::advance(Duration::from_secs(1)).await;
        let value = cache.get(&key).await;

        assert!(value.is_ok());
        assert_eq!(value.ok(), Some(None));
    }

    // ── TTL expiry (wall-clock) ───────────────────────────────────────────

    #[tokio::test]
    async fn memory_cache_expires_entries_after_ttl_wall_clock() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(1).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );
        let key = ReconstructionCacheKey::latest("test-file", None);
        cache.put(&key, b"hello").await.unwrap();

        // Immediately readable
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));

        // After expiry
        tokio::time::sleep(Duration::from_secs(2)).await;
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None, "expired entry should return None");
    }

    // ── Eviction with max_entries = 1 ─────────────────────────────────────

    #[tokio::test]
    async fn memory_cache_evicts_oldest_when_at_capacity_one() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap(),
            NonZeroUsize::new(1).unwrap(), // only 1 entry
        );
        let key_a = ReconstructionCacheKey::latest("file-a", None);
        let key_b = ReconstructionCacheKey::latest("file-b", None);

        cache.put(&key_a, b"aaa").await.unwrap();
        cache.put(&key_b, b"bbb").await.unwrap();

        // key_a should be evicted (oldest)
        let result = cache.get(&key_a).await.unwrap();
        assert_eq!(result, None, "oldest entry should be evicted");

        // key_b should still be present
        let result = cache.get(&key_b).await.unwrap();
        assert_eq!(result, Some(b"bbb".to_vec()));
    }

    // ── Concurrent get_or_load deduplication ──────────────────────────────

    #[tokio::test]
    async fn memory_cache_concurrent_get_or_load_deduplicates() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        ));
        let key = ReconstructionCacheKey::latest("dedup-test", None);
        let load_count = std::sync::Arc::new(AtomicU64::new(0));

        // Spawn 10 concurrent get_or_load calls with a slow loader
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = std::sync::Arc::clone(&cache);
            let key = key.clone();
            let load_count = std::sync::Arc::clone(&load_count);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_load(&key, || {
                        load_count.fetch_add(1, Ordering::Relaxed);
                        Box::pin(async { Ok::<_, ReconstructionCacheError>(b"result".to_vec()) })
                    })
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "get_or_load should succeed");
            assert_eq!(result.unwrap(), Some(b"result".to_vec()));
        }

        // Loader should only have been called once
        assert_eq!(load_count.load(Ordering::Relaxed), 1);
    }

    // ── Loader failure cleanup ────────────────────────────────────────────

    #[tokio::test]
    async fn memory_cache_loader_failure_cleans_up_pending() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );
        let key = ReconstructionCacheKey::latest("fail-test", None);

        // First call: loader fails with Operation error
        let result = cache
            .get_or_load(&key, || {
                Box::pin(async { Err::<Vec<u8>, _>(ReconstructionCacheError::Operation) })
            })
            .await;
        assert!(result.is_err());

        // Second call should retry (not cache the error)
        let result = cache
            .get_or_load(&key, || {
                Box::pin(async { Ok::<_, ReconstructionCacheError>(b"success".to_vec()) })
            })
            .await;
        assert_eq!(result.unwrap(), Some(b"success".to_vec()));
    }

    // ── Delete ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn memory_cache_delete_removes_entry() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );
        let key = ReconstructionCacheKey::latest("del-test", None);

        cache.put(&key, b"data").await.unwrap();
        assert!(cache.get(&key).await.unwrap().is_some());

        let deleted = cache.delete(&key).await.unwrap();
        assert!(deleted);
        assert!(cache.get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_cache_delete_missing_returns_false() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );
        let key = ReconstructionCacheKey::latest("missing", None);
        let deleted = cache.delete(&key).await.unwrap();
        assert!(!deleted);
    }

    // ── ready() test ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn memory_cache_ready_returns_ok() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let result = cache.ready().await;
        assert!(result.is_ok());
    }

    // ── get returns None for missing key ──────────────────────────────────

    #[tokio::test]
    async fn memory_cache_get_returns_none_for_missing_key() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let key = ReconstructionCacheKey::latest("nonexistent", None);
        let result = cache.get(&key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ── get returns None for expired entry (not in loading state) ─────────

    #[tokio::test(start_paused = true)]
    async fn memory_cache_get_returns_none_for_expired_not_loading() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let key = ReconstructionCacheKey::latest("expired-test", None);
        cache.put(&key, b"data").await.unwrap();

        // Advance past TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        let result = cache.get(&key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None, "expired entry should return None");
    }

    // ── delete on expired entry ───────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn memory_cache_delete_expired_entry_returns_true() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let key = ReconstructionCacheKey::latest("expired-del", None);
        cache.put(&key, b"data").await.unwrap();

        // Advance past TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        // Entry is expired but still stored — delete should succeed
        let deleted = cache.delete(&key).await.unwrap();
        assert!(deleted, "delete should return true even for expired entry");

        // After delete, get should return None
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None);
    }

    // ── get_or_load with concurrent timeout ──────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_cache_get_times_out_when_loader_hangs() {
        use std::sync::Arc;

        let cache = Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("hung-loader", None);

        // Task1: start get_or_load with a slow loader (60s)
        let cache_1 = Arc::clone(&cache);
        let key_1 = key.clone();
        let task1 = tokio::spawn(async move {
            let _result = cache_1
                .get_or_load(&key_1, || {
                    Box::pin(async {
                        // Slow loader — takes 60 seconds
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        Ok::<_, ReconstructionCacheError>(b"new-data".to_vec())
                    })
                })
                .await;
        });

        // Give task1 time to enter the loading state
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Task2: get() should hit the internal 30-second timeout
        let result = tokio::time::timeout(
            Duration::from_secs(35),
            cache.get(&key),
        )
        .await;

        #[allow(clippy::panic, clippy::match_wild_err_arm)]
        match result {
            Ok(Ok(value)) => {
                // Internal timeout fired — get returned None
                assert_eq!(value, None, "get should return None after internal timeout");
            }
            Ok(Err(e)) => panic!("get returned unexpected error: {e}"),
            Err(_elapsed) => {
                panic!("get did not complete within 35 seconds (internal 30s timeout should have fired)");
            }
        }

        drop(task1);
    }

    // ── get_or_load: cache hit (fast path) ────────────────────────────────

    #[tokio::test]
    async fn get_or_load_cache_hit_fast_path() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let key = ReconstructionCacheKey::latest("hit-test", None);

        // First call: loader runs and stores the value
        let result = cache
            .get_or_load(&key, || {
                Box::pin(async { Ok::<_, ReconstructionCacheError>(b"cached".to_vec()) })
            })
            .await;
        assert_eq!(result.unwrap(), Some(b"cached".to_vec()));

        // Second call: fast path cache hit — loader MUST NOT be called
        let load_count = std::sync::Arc::new(AtomicU64::new(0));
        let load_count_2 = std::sync::Arc::clone(&load_count);
        let result = cache
            .get_or_load(&key, || {
                load_count_2.fetch_add(1, Ordering::Relaxed);
                Box::pin(async { Ok::<_, ReconstructionCacheError>(b"should-not-call".to_vec()) })
            })
            .await;
        assert_eq!(result.unwrap(), Some(b"cached".to_vec()));
        assert_eq!(load_count.load(Ordering::Relaxed), 0);
    }

    // ── get_or_load: concurrent loading dedup hits loading map ────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_load_loading_dedup_hits_loading_map() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("dedup-map", None);
        let load_count = std::sync::Arc::new(AtomicU64::new(0));

        // Use a barrier so all 10 tasks hit get_or_load nearly simultaneously
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(11));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = std::sync::Arc::clone(&cache);
            let key = key.clone();
            let load_count = std::sync::Arc::clone(&load_count);
            let barrier = std::sync::Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(&key, || {
                        load_count.fetch_add(1, Ordering::Relaxed);
                        Box::pin(async { Ok::<_, ReconstructionCacheError>(b"result".to_vec()) })
                    })
                    .await
            }));
        }

        // Release all tasks at once from the main thread
        barrier.wait().await;

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "get_or_load should succeed");
            assert_eq!(result.unwrap(), Some(b"result".to_vec()));
        }

        assert_eq!(load_count.load(Ordering::Relaxed), 1);
    }

    // ── get_or_load: concurrent waiter gets None when loader fails ────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_load_concurrent_loader_failure_waiter_gets_none() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("fail-waiter", None);

        // Task 1: slow loader (300ms) that will fail
        let cache_1 = std::sync::Arc::clone(&cache);
        let key_1 = key.clone();
        let task1 = tokio::spawn(async move {
            cache_1
                .get_or_load(&key_1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Err::<Vec<u8>, ReconstructionCacheError>(
                            ReconstructionCacheError::Operation,
                        )
                    })
                })
                .await
        });

        // Give task1 time to register as the exclusive loader
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Task 2: should find task1 loading and wait on notify
        let cache_2 = std::sync::Arc::clone(&cache);
        let key_2 = key.clone();
        let task2 = tokio::spawn(async move {
            cache_2
                .get_or_load(&key_2, || {
                    Box::pin(async {
                        panic!("waiter should not call loader");
                        #[allow(unreachable_code)]
                        Ok::<_, ReconstructionCacheError>(vec![])
                    })
                })
                .await
        });

        let result1 = task1.await.unwrap();
        assert!(result1.is_err(), "loader should have failed");

        let result2 = task2.await.unwrap();
        // Waiter should get None (not an error), because the cleanup
        // notifies waiters without propagating the error.
        assert_eq!(result2.unwrap(), None);
    }

    // ── get_or_load: expired entry removed before loading ─────────────────

    #[tokio::test(start_paused = true)]
    async fn get_or_load_removes_expired_entry_before_loading() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        );
        let key = ReconstructionCacheKey::latest("expired-before-load", None);

        // Put an entry
        cache.put(&key, b"expired").await.unwrap();

        // Advance past TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        // get_or_load should: fast path miss, then in the loading setup,
        // find the expired entry and remove it (lines 164-167)
        let result = cache
            .get_or_load(&key, || {
                Box::pin(async { Ok::<_, ReconstructionCacheError>(b"fresh".to_vec()) })
            })
            .await;
        assert_eq!(result.unwrap(), Some(b"fresh".to_vec()));

        // Verify the old expired entry was cleaned up
        let get_result = cache.get(&key).await.unwrap();
        assert_eq!(get_result, Some(b"fresh".to_vec()));
    }

    // ── get: loading coalescing — successful notify path ───────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_loading_coalescing_success() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("coalesce-success", None);

        // Spawn a get_or_load with a slow loader so get() can find the loading entry
        let cache_put = std::sync::Arc::clone(&cache);
        let key_put = key.clone();
        let task_loader = tokio::spawn(async move {
            cache_put
                .get_or_load(&key_put, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        Ok::<_, ReconstructionCacheError>(b"loaded-data".to_vec())
                    })
                })
                .await
        });

        // Give get_or_load time to enter loading state
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now call get() — should find the loading entry, wait, and get the result
        let result = tokio::time::timeout(Duration::from_secs(5), cache.get(&key)).await;

        match result {
            Ok(Ok(Some(data))) => {
                assert_eq!(data, b"loaded-data".to_vec());
            }
            Ok(Ok(None)) => {
                panic!("get() should have returned the loaded data");
            }
            Ok(Err(e)) => {
                panic!("get() returned unexpected error: {e}");
            }
            Err(_) => {
                panic!("get() timed out — loading coalescing failed");
            }
        }

        let _ = task_loader.await;
    }

    // ── get: loading coalescing — loader failure returns None ──────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_loading_coalescing_loader_failure_returns_none() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("coalesce-fail", None);

        // Spawn a get_or_load that will fail
        let cache_loader = std::sync::Arc::clone(&cache);
        let key_loader = key.clone();
        let task_loader = tokio::spawn(async move {
            cache_loader
                .get_or_load(&key_loader, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Err::<Vec<u8>, ReconstructionCacheError>(
                            ReconstructionCacheError::Operation,
                        )
                    })
                })
                .await
        });

        // Give get_or_load time to enter loading state
        tokio::time::sleep(Duration::from_millis(50)).await;

        // get() should find the loading entry, wait on notify, then find nothing
        // and return None (not propagate the error)
        let result = tokio::time::timeout(Duration::from_secs(5), cache.get(&key)).await;

        match result {
            Ok(Ok(None)) => {
                // Expected: loader failed, no entry stored
            }
            Ok(Ok(Some(_))) => {
                panic!("get() should return None after loader failure");
            }
            Ok(Err(e)) => {
                panic!("get() returned unexpected error: {e}");
            }
            Err(_) => {
                panic!("get() timed out — loading coalescing failed");
            }
        }

        let _ = task_loader.await;
    }

    // ── put: update existing entry does not evict ──────────────────────────

    #[tokio::test]
    async fn put_update_existing_no_eviction() {
        let cache = MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN), // capacity = 1
        );
        let key = ReconstructionCacheKey::latest("update-key", None);

        // Insert first entry
        cache.put(&key, b"first").await.unwrap();
        // Update same key (should NOT evict since key already exists)
        cache.put(&key, b"second").await.unwrap();

        // Only one entry exists — the updated one
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(b"second".to_vec()));
    }

    // ── PartialOrd implementation for EvictionKey ────────────────────────

    #[test]
    fn eviction_key_partial_cmp() {
        use super::EvictionKey;
        use tokio::time::Instant;

        let a = EvictionKey(Instant::now(), 1);
        let b = EvictionKey(Instant::now(), 2);
        // partial_cmp delegates to cmp
        let ordering = a.partial_cmp(&b);
        assert!(ordering.is_some());
    }

    // ── get: race to hit no-loading / re-check paths ─────────────────────

    // ── Race: get() write-lock contention (triggers re-check + no-loading) ─

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn get_race_with_write_lock_contention() {
        // Use a longer TTL so pre-seeded entries don't expire before we race.
        // The goal is to have get()'s read-lock see loading=true on an expired
        // entry, then lose the write-lock race to a concurrent operation,
        // landing on lines 260-270.
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
        ));

        let mut handles = Vec::new();
        for i in 0..500 {
            let key = ReconstructionCacheKey::latest(&format!("contention-{i}"), None);

            // Write-lock contender (put)
            let c1 = std::sync::Arc::clone(&cache);
            let k1 = key.clone();
            let h1 = tokio::spawn(async move {
                let _ = c1.put(&k1, b"preloaded").await;
            });

            // Write-lock contender (delete)
            let c2 = std::sync::Arc::clone(&cache);
            let k2 = key.clone();
            let h2 = tokio::spawn(async move {
                let _ = c2.delete(&k2).await;
            });

            // Failing loader (get_or_load)
            let c3 = std::sync::Arc::clone(&cache);
            let k3 = key.clone();
            let h3 = tokio::spawn(async move {
                let _ = c3
                    .get_or_load(&k3, || {
                        Box::pin(async {
                            tokio::time::sleep(Duration::from_micros(100)).await;
                            Err::<Vec<u8>, ReconstructionCacheError>(
                                ReconstructionCacheError::Operation,
                            )
                        })
                    })
                    .await;
            });

            // Getter
            let c4 = std::sync::Arc::clone(&cache);
            let k4 = key.clone();
            let h4 = tokio::spawn(async move {
                let _ = c4.get(&k4).await;
            });

            // Extra write-lock contender
            let c5 = std::sync::Arc::clone(&cache);
            let k5 = key.clone();
            let h5 = tokio::spawn(async move {
                let _ = c5.put(&k5, b"extra").await;
            });

            handles.push(h1);
            handles.push(h2);
            handles.push(h3);
            handles.push(h4);
            handles.push(h5);
        }

        for h in handles {
            let _ = h.await;
        }
    }

    // ── Concurrency stress tests ──────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_cache_concurrent_get_put_delete() {
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
        ));
        let keys: std::sync::Arc<[ReconstructionCacheKey; 4]> = std::sync::Arc::new([
            ReconstructionCacheKey::latest("concurrent-1", None),
            ReconstructionCacheKey::latest("concurrent-2", None),
            ReconstructionCacheKey::latest("concurrent-3", None),
            ReconstructionCacheKey::latest("concurrent-4", None),
        ]);

        let mut handles = Vec::new();

        // 10 concurrent tasks: mix of get, put, delete
        for task_id in 0..10 {
            let cache = std::sync::Arc::clone(&cache);
            let keys = std::sync::Arc::clone(&keys);
            handles.push(tokio::spawn(async move {
                for round in 0..50 {
                    let key = &keys[(task_id + round) % 4];
                    match (task_id + round) % 5 {
                        0 | 1 => {
                            // put
                            let payload = format!("payload-{task_id}-{round}");
                            let _ = cache.put(key, payload.as_bytes()).await;
                        }
                        2 | 3 => {
                            // get
                            let _ = cache.get(key).await;
                        }
                        _ => {
                            // delete
                            let _ = cache.delete(key).await;
                        }
                    }
                    // Yield between operations to increase interleaving
                    tokio::task::yield_now().await;
                }
            }));
        }

        for handle in handles {
            let result = handle.await;
            assert!(result.is_ok(), "task panicked: {:?}", result.err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_cache_concurrent_same_key() {
        // Stress the loading/Notify coalescing pattern when multiple tasks
        // race on the same key simultaneously.
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        ));
        let key = ReconstructionCacheKey::latest("hot-key", None);

        let mut handles = Vec::new();
        for _ in 0..20 {
            let cache = std::sync::Arc::clone(&cache);
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                // First put to ensure there's data
                let _ = cache.put(&key, b"shared-data").await;
                // Multiple concurrent gets — some will hit loading coalescing
                for _ in 0..20 {
                    let result = cache.get(&key).await;
                    assert!(result.is_ok(), "concurrent get should not error");
                    tokio::task::yield_now().await;
                }
            }));
        }

        for handle in handles {
            let result = handle.await;
            assert!(result.is_ok(), "task panicked: {:?}", result.err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_cache_concurrent_eviction_stress() {
        // Stress the eviction path by filling the cache beyond capacity
        // with concurrent puts.
        let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
            NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN),
            NonZeroUsize::new(10).unwrap_or(NonZeroUsize::MIN),
        ));

        let mut handles = Vec::new();
        for task_id in 0..20 {
            let cache = std::sync::Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                for i in 0..20 {
                    let key =
                        ReconstructionCacheKey::latest(&format!("evict-key-{task_id}-{i}"), None);
                    let payload = format!("evict-payload-{task_id}-{i}");
                    let _ = cache.put(&key, payload.as_bytes()).await;
                    // Also read back randomly
                    if i % 3 == 0 {
                        let _ = cache.get(&key).await;
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        for handle in handles {
            let result = handle.await;
            assert!(result.is_ok(), "task panicked: {:?}", result.err());
        }
    }
}
