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
