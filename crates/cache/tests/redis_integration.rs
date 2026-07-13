#![allow(clippy::unwrap_used)]

use std::num::NonZeroU64;

use shardline_cache::{AsyncReconstructionCache, ReconstructionCacheKey, RedisReconstructionCache};
use shardline_test_support::DockerLocalStack;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_put_get_roundtrip() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("test-file", None);
    let payload = b"Hello, Redis!";

    // Initially empty
    assert!(cache.get(&key).await.unwrap().is_none());

    // Put
    cache.put(&key, payload).await.unwrap();

    // Get
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(payload.to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_removes_entry() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("del-test", None);
    cache.put(&key, b"delete me").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    let deleted = cache.delete(&key).await.unwrap();
    assert!(deleted);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_missing_returns_false() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("missing", None);
    assert!(!cache.delete(&key).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_overwrite_updates_value() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("overwrite-test", None);
    cache.put(&key, b"first").await.unwrap();
    cache.put(&key, b"second").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"second".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_works_with_repository_scope() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"));
    let Ok(scope) = scope else {
        return;
    };
    let key = ReconstructionCacheKey::latest("scoped-file", Some(&scope));

    cache.put(&key, b"scoped data").await.unwrap();
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"scoped data".to_vec()));
}
