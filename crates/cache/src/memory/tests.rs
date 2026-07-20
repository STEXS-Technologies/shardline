#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::match_wild_err_arm
)]

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

#[allow(clippy::shadow_unrelated)]
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

#[allow(clippy::shadow_unrelated)]
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

#[allow(clippy::shadow_unrelated)]
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
    let result = tokio::time::timeout(Duration::from_secs(35), cache.get(&key)).await;

    #[allow(clippy::panic, clippy::match_wild_err_arm)]
    match result {
        Ok(Ok(value)) => {
            // Internal timeout fired — get returned None
            assert_eq!(value, None, "get should return None after internal timeout");
        }
        Ok(Err(e)) => panic!("get returned unexpected error: {e}"),
        Err(_elapsed) => {
            panic!(
                "get did not complete within 35 seconds (internal 30s timeout should have fired)"
            );
        }
    }

    drop(task1);
}

// ── get_or_load: cache hit (fast path) ────────────────────────────────

#[allow(clippy::shadow_unrelated)]
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
                    Err::<Vec<u8>, ReconstructionCacheError>(ReconstructionCacheError::Operation)
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
                    Err::<Vec<u8>, ReconstructionCacheError>(ReconstructionCacheError::Operation)
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
    use super::inner::EvictionKey;
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
                let key = ReconstructionCacheKey::latest(&format!("evict-key-{task_id}-{i}"), None);
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

// ── get(): write-lock re-check finds valid entry ───────────────────────
//
// This covers the code at lines ~236-239 where get() acquires the write
// lock and finds a valid (non-expired) entry that was stored by a
// concurrent put() between the read-lock and write-lock acquisition.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn get_write_lock_recheck_finds_valid_entry() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    let key = ReconstructionCacheKey::latest("recheck-write-lock", None);

    // Use 500 rounds of concurrent get/get_or_load so the race window
    // where a loader finishes between get()'s read-lock and write-lock
    // is hit at least once.
    let mut handles = Vec::new();
    for _ in 0..500 {
        // Task: get_or_load with a fast loader
        let c1 = std::sync::Arc::clone(&cache);
        let k1 = key.clone();
        handles.push(tokio::spawn(async move {
            let result = c1
                .get_or_load(&k1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_micros(10)).await;
                        Ok::<_, ReconstructionCacheError>(b"value".to_vec())
                    })
                })
                .await;
            let _ = result;
        }));

        // Task: get() — if it lands between the loading entry creation
        // and the put() that stores the loaded value, it will exercise
        // the write-lock re-check path (lines 236-239).
        let c2 = std::sync::Arc::clone(&cache);
        let k2 = key.clone();
        handles.push(tokio::spawn(async move {
            let result = c2.get(&k2).await;
            assert!(result.is_ok(), "get() should not error");
        }));

        // Additional puts to create more write-lock pressure
        let c3 = std::sync::Arc::clone(&cache);
        let k3 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c3.put(&k3, b"extra").await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

// ── get(): creates a new loading entry after a previous one vanished ──
//
// This covers lines ~267-277 where get() finds NO loading entry in the
// write lock (it was removed by a concurrent failing loader between
// the read and write lock), so it creates a fresh loading entry.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn get_creates_loading_entry_when_loader_disappears() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    // Use a unique key per round so stale loading entries from one
    // round never cascade into another round's 30-second timeout.
    let mut handles = Vec::new();
    for i in 0..1000 {
        let key = ReconstructionCacheKey::latest(&format!("disappearing-loader-{i}"), None);

        // Failing get_or_load — fast 10 µs sleep
        let c1 = std::sync::Arc::clone(&cache);
        let k1 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c1
                .get_or_load(&k1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_micros(10)).await;
                        Err::<Vec<u8>, ReconstructionCacheError>(
                            ReconstructionCacheError::Operation,
                        )
                    })
                })
                .await;
        }));

        // get() — may race to see loading entry in read lock but not
        // in write lock, exercising lines 261-271.
        let c2 = std::sync::Arc::clone(&cache);
        let k2 = key.clone();
        handles.push(tokio::spawn(async move {
            let result = c2.get(&k2).await;
            assert!(result.is_ok(), "get() should not error");
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

// ── put() notifies waiters when a loading entry exists ─────────────────
//
// When put() is called externally while a get_or_load is loading the
// same key, put() should find the loading entry and notify waiters.
// This covers put() lines ~302-303.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_notifies_waiting_loading_entry() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    let key = ReconstructionCacheKey::latest("put-notify", None);

    // Start a get_or_load with a slow loader that creates a loading entry
    let c1 = std::sync::Arc::clone(&cache);
    let k1 = key.clone();
    let loader_task = tokio::spawn(async move {
        c1.get_or_load(&k1, || {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<_, ReconstructionCacheError>(b"from-loader".to_vec())
            })
        })
        .await
    });

    // Wait for the loading entry to be created
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Call put() externally — it should find the loading entry and
    // remove + notify.  This does NOT break the in-flight loader;
    // when the loader finishes it calls put() again.
    cache
        .put(&key, b"external-put")
        .await
        .expect("external put should succeed");

    // The get_or_load loader eventually finishes and its put()
    // overwrites with the loader's value.
    let loader_result = loader_task.await.expect("loader task panicked");
    assert!(
        loader_result.is_ok(),
        "loader should still succeed even with external put"
    );

    // Either "external-put" or "from-loader" is the final value —
    // both are valid depending on ordering.
    let final_val = cache.get(&key).await.unwrap();
    assert!(
        final_val.is_some(),
        "cache should have some value after put notify"
    );
}

// ── CacheInner seq ordering ───────────────────────────────────────────
//
// Verifies that entries are evicted in FIFO order (oldest first) by
// using monotonic Insertion-time progression (wall-clock with small
// sleeps between inserts).

#[tokio::test]
async fn cache_inner_fifo_eviction_order() {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN), // capacity = 3
    );
    let keys = [
        ReconstructionCacheKey::latest("first", None),
        ReconstructionCacheKey::latest("second", None),
        ReconstructionCacheKey::latest("third", None),
        ReconstructionCacheKey::latest("fourth", None),
    ];

    // Insert 3 entries with distinct insertion times
    for k in &keys[..3] {
        cache.put(k, b"data").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Fourth insert triggers eviction of the oldest (first)
    cache.put(&keys[3], b"fourth").await.unwrap();

    // First should be evicted (oldest)
    assert_eq!(
        cache.get(&keys[0]).await.unwrap(),
        None,
        "first (oldest) should be evicted"
    );
    // Second and third should still be present
    assert_eq!(
        cache.get(&keys[1]).await.unwrap(),
        Some(b"data".to_vec()),
        "second should survive"
    );
    assert_eq!(
        cache.get(&keys[2]).await.unwrap(),
        Some(b"data".to_vec()),
        "third should survive"
    );
    assert_eq!(
        cache.get(&keys[3]).await.unwrap(),
        Some(b"fourth".to_vec()),
        "fourth should be present"
    );
}

// ── CacheInner evict_oldest skips stale eviction entries ──────────────
//
// If eviction_order has a stale key (entry was already removed from
// `entries` without cleaning up eviction_order), evict_oldest should
// skip it and pop the next one.

#[tokio::test]
async fn cache_inner_evict_oldest_skips_stale_entries() {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN), // capacity = 2
    );
    let keys = [
        ReconstructionCacheKey::latest("stale-a", None),
        ReconstructionCacheKey::latest("stale-b", None),
        ReconstructionCacheKey::latest("stale-c", None),
    ];

    // Fill cache to capacity
    cache.put(&keys[0], b"a").await.unwrap();
    cache.put(&keys[1], b"b").await.unwrap();

    // Delete keys[0] normally (cleans up eviction_order)
    cache.delete(&keys[0]).await.unwrap();

    // Now cache has capacity for 1 more, insert keys[2] (no eviction needed)
    cache.put(&keys[2], b"c").await.unwrap();

    // keys[0] is gone, keys[1] and keys[2] are present
    assert_eq!(cache.get(&keys[0]).await.unwrap(), None);
    assert_eq!(cache.get(&keys[1]).await.unwrap(), Some(b"b".to_vec()));
    assert_eq!(cache.get(&keys[2]).await.unwrap(), Some(b"c".to_vec()));
}

// ── get_or_load write-lock re-check hit ───────────────────────────────
//
// When get_or_load re-checks after acquiring the write lock, it may
// find a valid entry that was stored by a concurrent put() between
// the read-lock and write-lock acquisition (lines ~150-153).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_or_load_recheck_after_write_lock_finds_entry() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    let key = ReconstructionCacheKey::latest("or-write-recheck", None);

    // Race many get_or_load + put operations to trigger the write-lock
    // re-check path where a put stores data between read/write lock.
    let mut handles = Vec::new();
    for i in 0..50 {
        let c1 = std::sync::Arc::clone(&cache);
        let k1 = key.clone();
        handles.push(tokio::spawn(async move {
            let result = c1
                .get_or_load(&k1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_micros(20)).await;
                        Ok::<_, ReconstructionCacheError>(format!("loaded-{i}").into_bytes())
                    })
                })
                .await;
            let _ = result;
        }));

        let c2 = std::sync::Arc::clone(&cache);
        let k2 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c2.put(&k2, format!("put-{i}").as_bytes()).await;
        }));

        let c3 = std::sync::Arc::clone(&cache);
        let k3 = key.clone();
        handles.push(tokio::spawn(async move {
            let result = c3.get(&k3).await;
            assert!(result.is_ok());
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Final state should have some value
    let final_val = cache.get(&key).await.unwrap();
    assert!(final_val.is_some(), "cache should hold a final value");
}

// ── put into full cache triggers eviction (non-existing key) ──────────
//
// The eviction guard in put() is: if the key does NOT already exist AND
// the cache is at capacity, evict the oldest.  This test verifies the
// condition correctly distinguishes existing-key updates (no eviction)
// from new-key inserts (eviction).

#[allow(clippy::shadow_unrelated)]
#[tokio::test]
async fn put_new_key_evicts_when_full_existing_key_update_does_not() {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN), // capacity = 2
    );
    let first = ReconstructionCacheKey::latest("first", None);
    let second = ReconstructionCacheKey::latest("second", None);
    let overflow = ReconstructionCacheKey::latest("overflow", None);

    // Fill cache: first inserted first (oldest), second inserted second
    cache.put(&first, b"first").await.unwrap();
    cache.put(&second, b"second").await.unwrap();

    // Update first key — should NOT trigger eviction because
    // contains_key(first) is true; the guard only evicts for new keys.
    // The update gives first a newer timestamp, making it the newest.
    cache.put(&first, b"first-updated").await.unwrap();
    assert_eq!(
        cache.get(&first).await.unwrap(),
        Some(b"first-updated".to_vec()),
        "existing key update should not evict"
    );
    assert_eq!(
        cache.get(&second).await.unwrap(),
        Some(b"second".to_vec()),
        "second should still be present after first is updated"
    );

    // Insert overflow key — SHOULD evict the oldest entry.
    // Since "first" was just updated (newest), "second" is now oldest.
    cache.put(&overflow, b"third").await.unwrap();
    assert_eq!(
        cache.get(&second).await.unwrap(),
        None,
        "second (now oldest) should be evicted for new key"
    );
    assert_eq!(
        cache.get(&first).await.unwrap(),
        Some(b"first-updated".to_vec()),
        "first (updated, newest) should survive"
    );
    assert_eq!(
        cache.get(&overflow).await.unwrap(),
        Some(b"third".to_vec()),
        "overflow should be present"
    );
}

// ── get_or_load concurrent waiter notified via put ────────────────────
//
// Multiple concurrent get_or_load calls: one becomes the loader
// (which calls put() on success), the others wait on the notify.
// This exercises the "waiter gets notified and returns the stored
// value" path in get_or_load (lines ~198-207).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_or_load_waiter_gets_notified_value() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    let key = ReconstructionCacheKey::latest("waiter-notified", None);
    let load_count = std::sync::Arc::new(AtomicU64::new(0));

    // Use a barrier so all tasks arrive at nearly the same time
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(21));

    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = std::sync::Arc::clone(&cache);
        let k = key.clone();
        let b = std::sync::Arc::clone(&barrier);
        let lc = std::sync::Arc::clone(&load_count);
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let result = c
                .get_or_load(&k, || {
                    lc.fetch_add(1, Ordering::Relaxed);
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok::<_, ReconstructionCacheError>(b"shared-result".to_vec())
                    })
                })
                .await;
            assert!(result.is_ok(), "get_or_load should succeed");
            assert_eq!(
                result.unwrap(),
                Some(b"shared-result".to_vec()),
                "all waiters should get the stored value"
            );
        }));
    }

    // Release all tasks at once
    barrier.wait().await;

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(
        load_count.load(Ordering::Relaxed),
        1,
        "loader should only run once"
    );
}

// ── get_or_load expired entry cleanup in write-lock re-check ──────────
//
// When the re-check after write lock finds an entry that was stored
// between read and write lock but that entry is already expired,
// get_or_load removes it and proceeds to load the new value.
// This covers the expired-entry cleanup path in the re-check block.

#[tokio::test(start_paused = true)]
async fn get_or_load_recheck_finds_expired_entry_after_concurrent_put() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    ));
    let key = ReconstructionCacheKey::latest("recheck-expired", None);

    // Put a short-lived entry
    cache.put(&key, b"short-lived").await.unwrap();

    // Advance time past TTL so the entry is expired
    tokio::time::advance(Duration::from_secs(2)).await;

    // The entry exists but is expired. get_or_load's fast-path read lock
    // will miss (expired), then the write lock re-check also finds it
    // expired, proceeds to the loading setup, finds no loading entry,
    // removes the expired entry, and loads fresh data.
    let result = cache
        .get_or_load(&key, || {
            Box::pin(async { Ok::<_, ReconstructionCacheError>(b"fresh".to_vec()) })
        })
        .await;
    assert_eq!(
        result.unwrap(),
        Some(b"fresh".to_vec()),
        "should load fresh data after finding expired entry"
    );

    // Verify the old expired entry was cleaned up
    let stored = cache.get(&key).await.unwrap();
    assert_eq!(
        stored,
        Some(b"fresh".to_vec()),
        "expired entry should have been replaced"
    );
}

// ── Concurrent put/get loading entry cleanup ─────────────────────────
//
// Stress test: many concurrent puts and gets to exercise the loading
// entry creation and cleanup paths under high contention.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_put_and_get_loading_entry_stress() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(50).unwrap_or(NonZeroUsize::MIN),
    ));

    let mut handles = Vec::new();
    for i in 0..30 {
        let key = ReconstructionCacheKey::latest(&format!("stress-key-{i}"), None);

        // Concurrent put + get on the same key
        for _ in 0..3 {
            let c_put = std::sync::Arc::clone(&cache);
            let k_put = key.clone();
            handles.push(tokio::spawn(async move {
                let _ = c_put.put(&k_put, b"data").await;
            }));

            let c_get = std::sync::Arc::clone(&cache);
            let k_get = key.clone();
            handles.push(tokio::spawn(async move {
                let result = c_get.get(&k_get).await;
                assert!(result.is_ok(), "get should not error under stress");
            }));

            let c_del = std::sync::Arc::clone(&cache);
            let k_del = key.clone();
            handles.push(tokio::spawn(async move {
                let _ = c_del.delete(&k_del).await;
            }));
        }
    }

    for h in handles {
        let _ = h.await;
    }
}

// ── get: phantom loading entry + write-lock re-check with contention ─
//
// Spawns many concurrent operations (get_or_load, get, put, delete)
// on the same cache to create write-lock contention.  This improves
// the chance that get()'s write-lock acquisition yields, allowing:
//
//   a) A loading entry to disappear between read lock and write lock,
//      triggering the phantom loading entry path (lines 261-271).
//
//   b) A put() to store an entry between read lock and write lock,
//      triggering the write-lock re-check path (lines 230-233).

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn get_phantom_loading_recheck_with_contention() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
    ));

    let mut handles = Vec::new();

    for i in 0..3000 {
        let key = ReconstructionCacheKey::latest(&format!("contention-{i}"), None);

        // Fast-failing get_or_load — creates a loading entry that
        // vanishes quickly.
        let c1 = std::sync::Arc::clone(&cache);
        let k1 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c1
                .get_or_load(&k1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_micros(1)).await;
                        Err::<Vec<u8>, ReconstructionCacheError>(
                            ReconstructionCacheError::Operation,
                        )
                    })
                })
                .await;
        }));

        // get() calls that race against the loader.
        for _ in 0..3 {
            let c = std::sync::Arc::clone(&cache);
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                let result = c.get(&k).await;
                assert!(result.is_ok(), "get() should not error");
            }));
        }

        // put() calls to create write-lock contention
        let c_put2 = std::sync::Arc::clone(&cache);
        let k_put2 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c_put2.put(&k_put2, b"data").await;
        }));

        // delete() calls for more write-lock contention
        let c_del2 = std::sync::Arc::clone(&cache);
        let k_del2 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c_del2.delete(&k_del2).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

// ── get: phantom loading with expired entry removal ──────────────────
//
// When the phantom loading path is hit AND an expired entry exists,
// the `should_remove` check (line 267) is true and the expired entry
// is cleaned up (lines 268-269).  Uses high contention to hit the
// race.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn get_phantom_loading_removes_expired_entry() {
    let cache = std::sync::Arc::new(MemoryReconstructionCache::new(
        NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
    ));

    let mut handles = Vec::new();

    for i in 0..2000 {
        // Each key gets a short-TTL entry via the main cache (1s TTL).
        // By the time operations race, the entry may be expired,
        // exercising the should_remove check in the phantom path.
        let key = ReconstructionCacheKey::latest(&format!("phantom-exp-{i}"), None);

        // Seed: put with 1s TTL
        let c_seed = std::sync::Arc::clone(&cache);
        let k_seed = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c_seed.put(&k_seed, b"seed").await;
        }));

        // Fast-failing loader
        let c1 = std::sync::Arc::clone(&cache);
        let k1 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c1
                .get_or_load(&k1, || {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_micros(1)).await;
                        Err::<Vec<u8>, ReconstructionCacheError>(
                            ReconstructionCacheError::Operation,
                        )
                    })
                })
                .await;
        }));

        // get() calls
        for _ in 0..3 {
            let c_get2 = std::sync::Arc::clone(&cache);
            let k_get2 = key.clone();
            handles.push(tokio::spawn(async move {
                let result = c_get2.get(&k_get2).await;
                assert!(result.is_ok(), "get() should not error");
            }));
        }

        // Additional write-lock contention via put/delete
        let c_put3 = std::sync::Arc::clone(&cache);
        let k_put3 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c_put3.put(&k_put3, b"noise").await;
        }));
        let c_del3 = std::sync::Arc::clone(&cache);
        let k_del3 = key.clone();
        handles.push(tokio::spawn(async move {
            let _ = c_del3.delete(&k_del3).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

// ── CacheInner evict_oldest on empty cache ────────────────────────────

#[tokio::test]
async fn cache_inner_evict_oldest_empty_cache_is_noop() {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(5).unwrap_or(NonZeroUsize::MIN),
    );
    let key = ReconstructionCacheKey::latest("new-key", None);

    // Eviction should not prevent insertion
    cache.put(&key, b"data").await.unwrap();
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"data".to_vec()));
}

// ── CacheInner evict_oldest stale eviction entries only ────────────────

#[tokio::test]
async fn cache_inner_evict_oldest_all_stale_clears_eviction_order() {
    // Create a cache with capacity 2 and insert 2 entries, then
    // delete both via remove() which cleans eviction_order.
    // Then insert a third key — evict_oldest must handle finding
    // nothing to evict gracefully.
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN),
    );
    let k1 = ReconstructionCacheKey::latest("k1", None);
    let k2 = ReconstructionCacheKey::latest("k2", None);
    let k3 = ReconstructionCacheKey::latest("k3", None);

    cache.put(&k1, b"one").await.unwrap();
    cache.put(&k2, b"two").await.unwrap();

    // Delete both to clean up entries but keep eviction_order entries
    cache.delete(&k1).await.unwrap();
    cache.delete(&k2).await.unwrap();

    // k3 should be insertable without panic
    cache.put(&k3, b"three").await.unwrap();
    assert_eq!(cache.get(&k3).await.unwrap(), Some(b"three".to_vec()));
}

// ── CacheInner insert overwrites existing key correctly ────────────────

#[tokio::test]
async fn cache_inner_insert_overwrites_existing_key() {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
    );
    let key = ReconstructionCacheKey::latest("overwrite", None);

    // Insert twice — the first entry is replaced in insert()
    cache.put(&key, b"first").await.unwrap();
    cache.put(&key, b"second").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"second".to_vec()));
}
